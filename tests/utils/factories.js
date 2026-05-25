import mongoose from 'mongoose';

const createRunDoc = (overrides = {}) => ({
  _id: new mongoose.Types.ObjectId(),
  runSignature: overrides.runSignature || 'fixture-signature',
  status: overrides.status || 'PROCESSING',
  config: overrides.config || { timestampToleranceSeconds: 300, quantityTolerancePct: 0.01 },
  summary: overrides.summary || {
    matchedCount: 0,
    conflictingCount: 0,
    unmatchedUserCount: 0,
    unmatchedExchangeCount: 0
  },
  errorMessage: overrides.errorMessage || ''
});

import crypto from 'crypto';

const createTransactionDoc = (overrides = {}) => ({
  _id: overrides._id || cryptoDeterministicId(overrides.runId || new mongoose.Types.ObjectId(), overrides.source || 'USER', overrides.externalId || 'TX-123'),
  runId: overrides.runId || new mongoose.Types.ObjectId(),
  source: overrides.source || 'USER',
  externalId: overrides.externalId || 'TX-123',
  timestamp: overrides.timestamp || new Date('2026-01-01T00:00:00Z'),
  asset: overrides.asset || 'BTC',
  quantity: overrides.quantity !== undefined ? overrides.quantity : 1.0,
  type: overrides.type || 'BUY',
  rawRow: overrides.rawRow || { price_usd: '1000', fee: '1' },
  isValid: overrides.isValid !== undefined ? overrides.isValid : true,
  validationErrors: overrides.validationErrors || [],
  matchingStatus: overrides.matchingStatus || 'UNMATCHED',
  reconciliationReason: overrides.reconciliationReason || ''
});

function cryptoDeterministicId(runId, source, externalId) {
  return crypto.createHash('sha256').update(`${runId}_${source}_${externalId}`).digest('hex');
}

export { createRunDoc, createTransactionDoc, cryptoDeterministicId };
