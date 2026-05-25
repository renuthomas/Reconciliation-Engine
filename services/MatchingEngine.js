import mongoose from "mongoose";
import { Transaction } from "../models/transaction.model.js";
import { mapType, normalizeAsset } from "../utils/normalizers.js";

class MatchingEngine {
  /**
   * Run a high-performance linear reconciliation algorithm for a specific runId.
   * Time Complexity: O(N + M) using a temporal sliding pointer window.
   */
  async reconcile(runId, config) {
    const timestampToleranceMs = config.timestampToleranceSeconds * 1000;
    const quantityTolerancePct = config.quantityTolerancePct;
    const EPSILON = 1e-9; // Protects against JS floating point arithmetic anomalies

    const userTransactions = await Transaction.find({ runId, source: 'USER' }).sort({ timestamp: 1, _id: 1 }).lean();
    const exchangeTransactions = await Transaction.find({ runId, source: 'EXCHANGE' }).sort({ timestamp: 1, _id: 1 }).lean();

    const matchedExchangeIds = new Set();
    const bulkDbOps = []; 

    let matchedCount = 0;
    let conflictingCount = 0;

    let exchangeLeftWindowIdx = 0;

    for (const uTx of userTransactions) {
      if (uTx.isValid === false) continue;
      
      const uTime = uTx.timestamp.getTime();
      const uAsset = normalizeAsset(uTx.asset);
      const uExpectedExchangeType = mapType(uTx.type); 
      const uQty = parseFloat(uTx.quantity.toString());

      // Advance window tail safely
      while (exchangeLeftWindowIdx < exchangeTransactions.length && uTime - exchangeTransactions[exchangeLeftWindowIdx].timestamp.getTime() > timestampToleranceMs) {
        exchangeLeftWindowIdx++;
      }

      let bestMatch = null;
      let bestConflict = null;
      let minMatchTimeDiff = Infinity;
      let minMatchQtyVariance = Infinity;
      let minConflictTimeDiff = Infinity;
      let conflictReason = "";

      // Temporal look-ahead window scan
      for (let i = exchangeLeftWindowIdx; i < exchangeTransactions.length; i++) {
        const eTx = exchangeTransactions[i];
        if (eTx.isValid === false) continue;
        
        const eTime = eTx.timestamp.getTime();

        // Array is sorted; break if elements drift beyond the future threshold boundary
        if (eTime - uTime > timestampToleranceMs) {
          break;
        }

        if (matchedExchangeIds.has(eTx._id.toString())) continue;

        const eAsset = normalizeAsset(eTx.asset);
        if (uAsset !== eAsset || uExpectedExchangeType !== eTx.type) continue;

        const timeDiff = Math.abs(uTime - eTime);
        const eQty = parseFloat(eTx.quantity.toString());
        const normalizedUserQty = Math.abs(uQty);
        
        // Calculate variance safely dealing with floating points
        let qtyVariancePct = Number.isFinite(normalizedUserQty) && normalizedUserQty > 0
          ? (Math.abs(uQty - eQty) / normalizedUserQty) * 100
          : (eQty === 0 ? 0 : Infinity);

        if (qtyVariancePct < EPSILON) qtyVariancePct = 0;

        // Selection Resolution Block
        if (qtyVariancePct <= quantityTolerancePct) {
          if (timeDiff < minMatchTimeDiff) {
            minMatchTimeDiff = timeDiff;
            minMatchQtyVariance = qtyVariancePct;
            bestMatch = eTx;
          }
          else if (timeDiff === minMatchTimeDiff && qtyVariancePct < minMatchQtyVariance) {
            minMatchQtyVariance = qtyVariancePct;
            bestMatch = eTx;
          }
        } else {
          // Track candidate conflict only if we haven't locked onto a match yet
          if (!bestMatch && timeDiff < minConflictTimeDiff) {
            minConflictTimeDiff = timeDiff;
            bestConflict = eTx;
            conflictReason = `Quantity variance of ${qtyVariancePct.toFixed(4)}% exceeds tolerance limit of ${quantityTolerancePct}%`;
          }
        }
      }

      // Commit State Assertions
      if (bestMatch) {
        matchedExchangeIds.add(bestMatch._id.toString());
        matchedCount++;
        this._queueStatusUpdate(bulkDbOps, uTx._id, bestMatch._id, 'MATCHED', 'Matched successfully across both sources');
      } else if (bestConflict) {
        matchedExchangeIds.add(bestConflict._id.toString());
        conflictingCount++;
        this._queueStatusUpdate(bulkDbOps, uTx._id, bestConflict._id, 'CONFLICTING', conflictReason);
      } else {
        bulkDbOps.push({
          updateOne: {
            filter: { _id: uTx._id },
            update: { 
              $set:{
                matchingStatus: 'UNMATCHED', 
                reconciliationReason: (uTx.validationErrors && uTx.validationErrors.length > 0) ? uTx.validationErrors.join(', ') : 'No matching transaction found within time boundaries on Exchange records' 
            }
          }
          }
        });
      }
    }

    // Wrap up unmatched exchange records
    const unmatchedExchangeRows = exchangeTransactions.filter(eTx => !matchedExchangeIds.has(eTx._id.toString()));
    for (const eTx of unmatchedExchangeRows) {
      bulkDbOps.push({
        updateOne: {
          filter: { _id: eTx._id },
          update: { 
            $set:{
              matchingStatus: 'UNMATCHED', 
              reconciliationReason: 'No corresponding record logged by User within historical time blocks' 
              
            }
          }
        }
      });
    }

    // Flush updates concurrently to MongoDB cluster
    if (bulkDbOps.length > 0) {
      await Transaction.bulkWrite(bulkDbOps, { ordered: false });
    }

    console.log(`User Rows Processed: ${userTransactions.length} | Matched: ${matchedCount} | Conflicting: ${conflictingCount} | Unmatched: ${userTransactions.length - (matchedCount + conflictingCount)}`);
    console.log(`Exchange Rows Processed: ${exchangeTransactions.length}  | Matched: ${matchedCount} | Conflicting: ${conflictingCount}| Unmatched: ${unmatchedExchangeRows.length}`);
    
    return {
      matchedCount,
      conflictingCount,
      unmatchedUserCount: userTransactions.length - (matchedCount + conflictingCount),
      unmatchedExchangeCount: unmatchedExchangeRows.length
    };
  }

  _queueStatusUpdate(opsArray, userDbId, exchangeDbId, status, reason) {
    opsArray.push(
      {
        updateOne: {
          filter: { _id: userDbId },
          update: { 
            $set:{
              matchingStatus: status, 
              reconciliationReason: reason, 
              pairedTxId: exchangeDbId
            }
          }
        }
      },
      {
        updateOne: {
          filter: { _id: exchangeDbId },
          update: { 
            $set:{
              matchingStatus: status,
              reconciliationReason: reason,
              pairedTxId: userDbId
            }
          }
        }
      }
    );
  }
}

export { MatchingEngine };
