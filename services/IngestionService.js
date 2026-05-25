import fs from 'fs';
import csv from 'fast-csv';
import { Transaction } from "../models/transaction.model.js";
import { normalizeAsset } from "../utils/normalizers.js";
import crypto from "crypto";

class IngestionService {
    
    /**
     * Ingests, normalizes, validates, and batches a transaction CSV file.
     * @param {string} filePath - Path to user_transactions.csv or exchange_transactions.csv
     * @param {string} source - Either "USER" or "EXCHANGE"
     * @param {string} runId - The current MongoDB Run Object ID
     * @returns {Promise<void>} Resolves when the file is entirely parsed and saved
     */
    ingestData(filePath, source, runId) {
        return new Promise((resolve, reject) => {
            const batchLimit = 500;
            let currentBatch = [];
            
            const readStream = fs.createReadStream(filePath);
            const csvStream = csv.parse({ headers: true, trim: true });
            
            
            readStream.on("error", reject);
            csvStream.on("error", reject);

            readStream.pipe(csvStream)
                .on("data", (row) => { 
                    const processedRow = this.validateAndNormalize(row, source, runId);
                    currentBatch.push(processedRow);

                    if (currentBatch.length >= batchLimit) {
                        csvStream.pause();
                    
                        const batchToSave = [...currentBatch];
                        currentBatch = [];
                        
                        this.processBatch(batchToSave)
                            .then(() => csvStream.resume()) // Resume only after DB confirms
                            .catch(err => {
                                readStream.destroy();
                                reject(err);
                            });
                    }
                })
                .on("end", async () => {
                    try {
                        if (currentBatch.length > 0) {
                            await this.processBatch(currentBatch);
                        }
                        resolve();
                    } catch (err) {
                        reject(err);
                    }
                });
            }
        );
    }

    /**
     * Sanitizes raw CSV rows without dropping bad data, flagging it instead
     */
    validateAndNormalize(row, source, runId) {
        const errors = [];
        
        const extId = row.transaction_id;
        const rawType = row.type;
        const rawAsset = row.asset;
        const rawQty = row.quantity;
        const rawTimestamp = row.timestamp;

        const normalizedType = rawType ? rawType.toString().trim().toUpperCase() : '';
        const validTypes = new Set(["BUY", "SELL", "TRANSFER_IN", "TRANSFER_OUT"]);

        if (!extId) errors.push("Missing transactional reference ID.");
        if (!rawType) {
            errors.push("Missing transactional execution operation type.");
        } else if (!validTypes.has(normalizedType)) {
            errors.push(`Invalid transaction type: '${rawType}'. Expected BUY, SELL, TRANSFER_IN, or TRANSFER_OUT.`);
        }
        if (!rawAsset) errors.push("Missing transactional token asset label.");
        
        const numericQty = parseFloat(rawQty);
        if (isNaN(numericQty)) {
            errors.push(`Invalid quantity syntax: '${rawQty}' cannot be cast into a number.`);
        }else if (numericQty <= 0) {
            errors.push(`Invalid quantity: '${rawQty}' must be greater than 0.`);
        }

        const timestampDate = new Date(rawTimestamp);
        const invalidTimestamp = new Date(0);
        if (isNaN(timestampDate.getTime())) {
            errors.push(`Invalid timestamp format: '${rawTimestamp}' is unparseable.`);
        }

        const isValid = errors.length === 0;

        return {
            runId,
            source,
            externalId: extId || "MALFORMED_ROW",
            timestamp: isValid ? timestampDate : invalidTimestamp,
            asset: isValid ? normalizeAsset(rawAsset) : (rawAsset || "UNKNOWN"),
            quantity: isValid ? numericQty : 0, 
            type: isValid ? normalizedType : (normalizedType || "UNKNOWN"),
            rawRow: row,
            isValid,
            validationErrors: errors,
            matchingStatus: "UNMATCHED"
        };
    }

    /**
     * Writes processed records to the database using an unordered, idempotent batch strategy.
     * Uses a composite deterministic ID containing the runId to isolate multiple config variations.
     */
    async processBatch(batch) {
        if (!batch || batch.length === 0) return;
        
        console.log("Saving batch of size:", batch.length);

        try {
            const operations = batch.map(doc => {
                // Generate a compound deterministic ID specific to this run execution context
                // This combines the unique run instance with the unique remote transaction row reference
                const deterministicString = `${doc.runId}_${doc.source}_${doc.externalId}`;
                const compositeId = crypto.createHash('sha256').update(deterministicString).digest('hex');
                
                const updateDoc = { ...doc };
                delete updateDoc._id; // _id is a reserved key
                const {matchingStatus,...rest} = updateDoc;
                
                return {
                    updateOne: {
                        filter: { _id: compositeId },
                        update: { 
                            $set: rest,
                            $setOnInsert:{
                                matchingStatus,
                            }
                         },
                        upsert: true
                    }
                };
            });

            await Transaction.bulkWrite(operations, { ordered: false });

        } catch (err) {
            // Handle partial bulk write errors safely without killing the active CSV stream
            if (err.name === 'BulkWriteError' || err.code === 11000) {
                this.handleBulkWriteErrors(err, batch);
            } else {
                // Systemic failure (database down, authentication timeout, etc.); throw to halt the stream
                console.error("Fatal network or database connectivity exception:", err);
                throw err;
            }
        }
    }

    /**
     * Isolates failed operations within an unordered batch and offloads them to an audit trail.
     */
    handleBulkWriteErrors(bulkWriteError, originalBatch) {
        const writeErrors = bulkWriteError.writeErrors || [];
        console.warn(`Captured ${writeErrors.length} validation or database errors in current execution batch.`);

        for (const error of writeErrors) {
            const failedIndex = error.index;
            const failedRow = originalBatch[failedIndex];

            // Append the database engine failure reason directly into the document structure
            failedRow.validationErrors.push(`Database Constraint Error: ${error.errmsg}`);
            failedRow.isValid = false;

            // Route out of band to prevent pipeline stalls
            this.routeToDeadLetterQueue(failedRow);
        }
    }

    routeToDeadLetterQueue(failedRecord) {
        // Production: Push to Kafka/RabbitMQ or an isolated 'failed_ingestions' MongoDB collection
        console.error(`[DLQ ROUTE] Isolation triggered for row ID: ${failedRecord.externalId}`);
    }

}

export { IngestionService };