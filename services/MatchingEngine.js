import mongoose from "mongoose";
import { Transaction } from "../models/transaction.model.js";
import { mapType, normalizeAsset } from "../utils/normalizers.js";

class MatchingEngine {
  /**
   * Run a memory-safe linear reconciliation algorithm for a specific runId.
   * Time Complexity: O(N + M)
   * Space Complexity: O(W) where W is the small number of transactions within the sliding time window.
   */
  async reconcile(runId, config) {
    const timestampToleranceMs = config.timestampToleranceSeconds * 1000;
    const quantityTolerancePct = config.quantityTolerancePct;
    const EPSILON = 1e-9; 
    const BULK_FLUSH_SIZE = 1000; 

    // Initialize Cursors sorted by timestamp to allow linear streaming
    const userCursor = Transaction.find({ runId, source: 'USER' }).sort({ timestamp: 1, _id: 1 }).lean().cursor();
    const exchangeCursor = Transaction.find({ runId, source: 'EXCHANGE' }).sort({ timestamp: 1, _id: 1 }).lean().cursor();

    const matchedExchangeIds = new Set();
    let bulkDbOps = []; 

    let totalUserCount = 0;
    let totalExchangeCount = 0;
    let matchedCount = 0;
    let conflictingCount = 0;
    let unmatchedUserCount = 0;
    let unmatchedExchangeCount = 0;

    // Sliding Window Buffer for Exchange records
    const exchangeWindow = [];
    let exchangeCursorExhausted = false;

    // Helper to flush bulk operations safely
    const flushBulkOps = async () => {
      if (bulkDbOps.length > 0) {
        await Transaction.bulkWrite(bulkDbOps, { ordered: false });
        bulkDbOps = []; 
      }
    };

    // Stream USER transactions one by one
    for await (const uTx of userCursor) {
      totalUserCount++; // Count every user record as it streams past
      if (uTx.isValid === false){
        if(uTx.matchingStatus === 'UNMATCHED'){
          unmatchedUserCount++;
        }
        continue;
      }

      const uTime = uTx.timestamp.getTime();
      const uAsset = normalizeAsset(uTx.asset);
      const uExpectedExchangeType = mapType(uTx.type); 
      const uQty = parseFloat(uTx.quantity.toString());

      // A. Evict exchange records from the front of the window that have fallen behind the past threshold boundary
      while (exchangeWindow.length > 0 && uTime - exchangeWindow[0].timestamp.getTime() > timestampToleranceMs) {
        const evictedTx = exchangeWindow.shift();
        
        if (!matchedExchangeIds.has(evictedTx._id.toString())) {
          unmatchedExchangeCount++; // Real-time increment for evicted unmatched records
          bulkDbOps.push({
            updateOne: {
              filter: { _id: evictedTx._id },
              update: { $set: { matchingStatus: 'UNMATCHED', reconciliationReason: 'No corresponding record logged by User within historical time blocks' } }
            }
          });
          if (bulkDbOps.length >= BULK_FLUSH_SIZE) await flushBulkOps();
        } else {
          matchedExchangeIds.delete(evictedTx._id.toString()); 
        }
      }

      // B. Pull exchange records from cursor into window until we reach the future threshold boundary
      while (!exchangeCursorExhausted) {
        const eTx = await exchangeCursor.next();
        if (!eTx) {
          exchangeCursorExhausted = true;
          break;
        }

        totalExchangeCount++; // Inline tracking of total processed Exchange records
        const eTime = eTx.timestamp.getTime();
        exchangeWindow.push(eTx);

        if (eTime - uTime > timestampToleranceMs) {
          break;
        }
      }

      let bestMatch = null;
      let bestConflict = null;
      let minMatchTimeDiff = Infinity;
      let minMatchQtyVariance = Infinity;
      let minConflictTimeDiff = Infinity;
      let conflictReason = "";

      // C. Evaluate candidates inside the sliding window
      for (const eTx of exchangeWindow) {
        if (eTx.isValid === false) continue;
        
        const eTime = eTx.timestamp.getTime();

        if (eTime - uTime > timestampToleranceMs) break;
        if (uTime - eTime > timestampToleranceMs) continue;
        if (matchedExchangeIds.has(eTx._id.toString())) continue;

        const eAsset = normalizeAsset(eTx.asset);
        if (uAsset !== eAsset || uExpectedExchangeType !== eTx.type) continue;

        const timeDiff = Math.abs(uTime - eTime);
        const eQty = parseFloat(eTx.quantity.toString());
        const normalizedUserQty = Math.abs(uQty);
        
        let qtyVariancePct = Number.isFinite(normalizedUserQty) && normalizedUserQty > 0
          ? (Math.abs(uQty - eQty) / normalizedUserQty) * 100
          : (eQty === 0 ? 0 : Infinity);

        if (qtyVariancePct < EPSILON) qtyVariancePct = 0;

        if (qtyVariancePct <= quantityTolerancePct) {
          if (timeDiff < minMatchTimeDiff || (timeDiff === minMatchTimeDiff && qtyVariancePct < minMatchQtyVariance)) {
            minMatchTimeDiff = timeDiff;
            minMatchQtyVariance = qtyVariancePct;
            bestMatch = eTx;
          }
        } else if (!bestMatch && timeDiff < minConflictTimeDiff) {
          minConflictTimeDiff = timeDiff;
          bestConflict = eTx;
          conflictReason = `Quantity variance of ${qtyVariancePct.toFixed(4)}% exceeds tolerance limit of ${quantityTolerancePct}%`;
        }
      }

      // D. Process State Matches
      if (bestMatch) {
        matchedExchangeIds.add(bestMatch._id.toString());
        matchedCount++;
        this._queueStatusUpdate(bulkDbOps, uTx._id, bestMatch._id, 'MATCHED', 'Matched successfully across both sources');
      } else if (bestConflict) {
        matchedExchangeIds.add(bestConflict._id.toString());
        conflictingCount++;
        this._queueStatusUpdate(bulkDbOps, uTx._id, bestConflict._id, 'CONFLICTING', conflictReason);
      } else {
        unmatchedUserCount++; // Direct execution path monitoring
        bulkDbOps.push({
          updateOne: {
            filter: { _id: uTx._id },
            update: { 
              $set: {
                matchingStatus: 'UNMATCHED', 
                reconciliationReason: (uTx.validationErrors && uTx.validationErrors.length > 0) ? uTx.validationErrors.join(', ') : 'No matching transaction found within time boundaries on Exchange records' 
              }
            }
          }
        });
      }

      if (bulkDbOps.length >= BULK_FLUSH_SIZE) await flushBulkOps();
    }

    // 3. Process remaining items in the window after User Cursor finishes
    while (exchangeWindow.length > 0) {
      const evictedTx = exchangeWindow.shift();
      if (!matchedExchangeIds.has(evictedTx._id.toString())) {
        unmatchedExchangeCount++;
        bulkDbOps.push({
          updateOne: {
            filter: { _id: evictedTx._id },
            update: { $set: { matchingStatus: 'UNMATCHED', reconciliationReason: 'No corresponding record logged by User within historical time blocks' } }
          }
        });
      }
    }

    // 4. Handle any remaining items left in the Exchange Cursor
    let remainingETx;
    while ((remainingETx = await exchangeCursor.next())) {
      totalExchangeCount++; // Continue capturing counts for trailing records
      unmatchedExchangeCount++;
      bulkDbOps.push({
        updateOne: {
          filter: { _id: remainingETx._id },
          update: { $set: { matchingStatus: 'UNMATCHED', reconciliationReason: 'No corresponding record logged by User within historical time blocks' } }
        }
      });
      if (bulkDbOps.length >= BULK_FLUSH_SIZE) await flushBulkOps();
    }

    // Final database flush
    await flushBulkOps();

    console.log(`[Reconcile Complete] RunId: ${runId}`);
    console.log(`User Source: Total = ${totalUserCount} | Matched = ${matchedCount} | Conflicting = ${conflictingCount} | Unmatched = ${unmatchedUserCount}`);
    console.log(`Exchange Source: Total = ${totalExchangeCount} | Matched = ${matchedCount} | Conflicting = ${conflictingCount} | Unmatched = ${unmatchedExchangeCount}`);

    return {
      totalUserCount,
      totalExchangeCount,
      matchedCount,
      conflictingCount,
      unmatchedUserCount,
      unmatchedExchangeCount
    };
  }

  _queueStatusUpdate(opsArray, userDbId, exchangeDbId, status, reason) {
    opsArray.push(
      {
        updateOne: {
          filter: { _id: userDbId },
          update: { $set: { matchingStatus: status, reconciliationReason: reason, pairedTxId: exchangeDbId } }
        }
      },
      {
        updateOne: {
          filter: { _id: exchangeDbId },
          update: { $set: { matchingStatus: status, reconciliationReason: reason, pairedTxId: userDbId } }
        }
      }
    );
  }
}

export { MatchingEngine };