import csv from 'fast-csv';
import { Transaction } from "../models/transaction.model.js";
import { Run } from "../models/run.model.js";

class ReportService {
  /**
   * Generates a side-by-side reconciliation CSV report stream for a specific run
   * @param {string} runId - The MongoDB execution run reference ID
   * @param {Object} writableStream - The Express HTTP response stream (res) to write data directly to the client
   */
  async generateFullCsvReport(runId, writableStream) {
    const run = await Run.findById(runId).lean();
    if (!run) {
      throw new Error(`Reconciliation run context with ID ${runId} could not be located.`);
    }

    const csvStream = csv.format({ headers: true });
    csvStream.pipe(writableStream);

    const transactions = await Transaction.find({ runId }).lean();

    // Fast-lookup map for all transactions using their unique _id string
    const txMap = new Map();
    
    // Arrays to isolate records by source for side-by-side matching
    const userRecords = [];
    const unmatchedExchangeRows = [];

    // First pass: Index everything by its _id and sort by source
    for (const tx of transactions) {
      txMap.set(tx._id.toString(), tx);
      
      if (tx.source === 'USER') {
        userRecords.push(tx);
      } else if (tx.matchingStatus === 'UNMATCHED') {
        // Exchange rows that are natively unmatched can immediately be flagged for late streaming
        unmatchedExchangeRows.push(tx);
      }
    }

    // Track processed exchange transaction IDs to avoid duplicate output rows
    const processedExchangeIds = new Set();

    // Process all USER rows (MATCHED, CONFLICTING, and UNMATCHED)
    for (const userTx of userRecords) {
      if (userTx.matchingStatus === 'UNMATCHED') {
        // Stream unmatched user rows immediately
        //console.log(userTx);
        csvStream.write(this._formatReportRow('UNMATCHED_USER_ONLY', userTx, null, userTx.reconciliationReason || userTx.validationErrors.join(', ')));
      } else if (userTx.pairedTxId) {
        // Use the pairing pointer for an O(1) direct map lookup
        const exchangeTx = txMap.get(userTx.pairedTxId.toString());

        if (exchangeTx) {
          // Pass the precise matching status ('MATCHED' or 'CONFLICTING') instead of a hardcoded fallback
          csvStream.write(this._formatReportRow(userTx.matchingStatus, userTx, exchangeTx, userTx.reconciliationReason));
          processedExchangeIds.add(exchangeTx._id.toString());
        } else {
          // Fallback: If the engine paired it but the exchange record is missing from DB records
          csvStream.write(this._formatReportRow('UNMATCHED_USER_ONLY', userTx, null, userTx.reconciliationReason));
        }
      } else {
        // Fallback safety catch
        csvStream.write(this._formatReportRow('UNMATCHED_USER_ONLY', userTx, null, userTx.reconciliationReason));
      }
    }

    // Stream remaining Exchange rows that were never processed/paired
    for (const tx of transactions) {
      if (tx.source === 'EXCHANGE' && tx.matchingStatus !== 'UNMATCHED') {
        if (!processedExchangeIds.has(tx._id.toString())) {
          unmatchedExchangeRows.push(tx);
        }
      }
    }

    // Stream all unmatched exchange rows to the bottom of the CSV file
    for (const exchangeTx of unmatchedExchangeRows) {
      csvStream.write(this._formatReportRow('UNMATCHED_EXCHANGE_ONLY', null, exchangeTx, exchangeTx.reconciliationReason));
    }

    // End the CSV stream cleanly
    csvStream.end();
  }

  /**
   * Structures a single side-by-side row map layout for the output reconciliation CSV
   */
  _formatReportRow(category, userTx, exchangeTx, reason) {
    // if(category=="UNMATCHED_USER_ONLY"){
    //   console.log(reason);
    // }
    return {
      'Category': category,
      'Reason': reason || 'N/A',
      
      // User Transaction Fields
      'User_Tx_ID': userTx ? userTx.externalId : '',
      'User_Timestamp': userTx && userTx.timestamp ? (userTx.timestamp instanceof Date ? userTx.timestamp.toISOString() : new Date(userTx.timestamp).toISOString()) : '',
      'User_Type': userTx ? userTx.type : '',
      'User_Asset': userTx ? userTx.asset : '',
      // Mongoose Decimal128 values must be safely string-cast using .toString()
      'User_Quantity': userTx && userTx.quantity ? userTx.quantity.toString() : '',
      'User_Price_USD': userTx ? (userTx.rawRow?.price_usd || '') : '',
      'User_Fee': userTx ? (userTx.rawRow?.fee || '') : '',
      
      // Exchange Transaction Fields
      'Exchange_Tx_ID': exchangeTx ? exchangeTx.externalId : '',
      'Exchange_Timestamp': exchangeTx && exchangeTx.timestamp ? (exchangeTx.timestamp instanceof Date ? exchangeTx.timestamp.toISOString() : new Date(exchangeTx.timestamp).toISOString()) : '',
      'Exchange_Type': exchangeTx ? exchangeTx.type : '',
      'Exchange_Asset': exchangeTx ? exchangeTx.asset : '',
      // Mongoose Decimal128 values must be safely string-cast using .toString()
      'Exchange_Quantity': exchangeTx && exchangeTx.quantity ? exchangeTx.quantity.toString() : '',
      'Exchange_Price_USD': exchangeTx ? (exchangeTx.rawRow?.price_usd || '') : '',
      'Exchange_Fee': exchangeTx ? (exchangeTx.rawRow?.fee || '') : '',
    };
  }
}

export { ReportService };