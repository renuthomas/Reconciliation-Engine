import crypto from 'crypto';
import fs from 'fs';
import { MatchingEngine } from "../services/MatchingEngine.js";
import { IngestionService } from "../services/IngestionService.js";
import { Run } from "../models/run.model.js";

const ingestionService = new IngestionService();
const matchingEngine = new MatchingEngine();

// Generate a unique fingerprint for a file for deterministic run signatures
function getFileFingerprint(filePath) {
    return new Promise((resolve, reject) => {
        const hash = crypto.createHash('sha256');
        const stream = fs.createReadStream(filePath);

        stream.on('data', (data) => hash.update(data));

        stream.on('end', () => {
            resolve(hash.digest('hex'));
        });

        stream.on('error', (err) => {
            reject(err);
        });
    });
}

const parseConfigValue = (input, envValue, defaultValue) => {
    const candidate = input !== undefined ? Number(input) : envValue !== undefined ? Number(envValue) : defaultValue;
    return Number.isFinite(candidate) && candidate >= 0 ? candidate : defaultValue;
};

const startReconcile = async (req, res) => {
    let runId = null;

    try {
        const userFile = "user_transactions.csv";
        const exchangeFile = "exchange_transactions.csv";

        const config = {
            timestampToleranceSeconds: parseConfigValue(req.body?.timestampToleranceSeconds, process.env.TIMESTAMP_TOLERANCE_SECONDS, 300),
            quantityTolerancePct: parseConfigValue(req.body?.quantityTolerancePct, process.env.QUANTITY_TOLERANCE_PCT, 0.01)
        };

        // Generate unique fingerprints for the files being processed
        const userFingerprint = await getFileFingerprint(userFile);
        const exchangeFingerprint = await getFileFingerprint(exchangeFile);

       
        // Hash the string to create a completely deterministic run signature
        const runSignature = crypto
            .createHash('sha256')
            .update(userFingerprint)
            .update(exchangeFingerprint)
            .update(String(config.timestampToleranceSeconds))
            .update(String(config.quantityTolerancePct))
            .digest('hex');

        console.log("userFingerprint:", userFingerprint);
        console.log("exchangeFingerprint:", exchangeFingerprint);
        console.log("runSignature:", runSignature);

        // Atomic Upsert: Re-use the existing runId if the exact same files + configs are run again
        const result = await Run.findOneAndUpdate(
            { runSignature: runSignature },
            {
                $setOnInsert: {
                    status: "PROCESSING",
                    config,
                    runSignature
                }
            },
            { upsert: true, returnDocument: "after", setDefaultsOnInsert: true,includeResultMetadata:true }
        );
        
        const runResult = result.value;
        const isNewRun=!result.lastErrorObject.updatedExisting;
        runId = runResult._id;

        // Short-circuit if this exact run combination was already fully processed
        if (!isNewRun && runResult.status === "COMPLETED") {
            return res.status(200).json({
                success: true,
                runId,
                message: "[IDEMPOTENCY] This exact file batch and configuration has already been matched successfully. Skipping execution.",
            });
        }

        if( !isNewRun && runResult.status==="PROCESSING"){
            return res.status(409).json({
                success: false,
                runId,
                message: "This reconciliation batch is currently being processed by another request. Please poll or check back later."
            });
        }

        // Handle resume state changes
        if (runResult.status === "FAILED") {
            await Run.findOneAndUpdate(
                {
                    _id:runId, 
                    status: "FAILED",
                },
                {
                    $set:{
                        status: "PROCESSING", 
                        errorMessage: null 
                    }
                }
            );
            console.log(`[Run ${runId}] Resuming crashed run for identical file signatures.`);
        } else {
            console.log(`[Run ${runId}] Commencing brand new run based on file fingerprints.`);
        }


        // Pipeline Execution
        console.log(`[Run ${runId}] Commencing ingestion of user records...`);
        await ingestionService.ingestData(userFile, "USER", runId);

        console.log(`[Run ${runId}] Commencing ingestion of exchange records...`);
        await ingestionService.ingestData(exchangeFile, "EXCHANGE", runId);

        console.log(`[Run ${runId}] Running matching analysis calculations...`);
        const summaryResults = await matchingEngine.reconcile(runId, config);

        console.log(`[Run ${runId}] Finalizing reconciliation run...`);

        await Run.findByIdAndUpdate(runId, {
            status: "COMPLETED",
            summary: summaryResults
        });

        return res.status(200).json({
            success: true,
            runId,
            message: "Reconciliation analysis finalized successfully.",
        });

    } catch (error) {
        console.error(`[Fatal System Breakdown] Run process failed to complete:`, error);

        if (runId) {
            await Run.findByIdAndUpdate(runId, {
                status: "FAILED",
                errorMessage: error.message || "Unknown transactional calculation exception occurred."
            });
        }

        return res.status(500).json({
            success: false,
            runId,
            message: "Reconciliation run encountered a fatal error.",
            error: error.message
        });
    }
};

export { startReconcile };
