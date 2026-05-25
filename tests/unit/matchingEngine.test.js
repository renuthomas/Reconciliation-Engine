import { jest } from '@jest/globals';

const TransactionMock = {
  find: jest.fn(),
  bulkWrite: jest.fn()
};

jest.unstable_mockModule('../../models/transaction.model.js', () => ({ Transaction: TransactionMock }));

const { MatchingEngine } = await import('../../services/MatchingEngine.js');

describe('MatchingEngine', () => {
  let engine;

  const createFindResponse = (rows) => ({
    sort: jest.fn(() => ({
      lean: jest.fn().mockResolvedValue(rows)
    }))
  });

  beforeEach(() => {
    engine = new MatchingEngine();
    TransactionMock.find.mockReset();
    TransactionMock.bulkWrite.mockReset().mockResolvedValue({});
  });

  it('matches user and exchange transactions when asset, type, time, and quantity are within tolerance', async () => {
    const userTx = {
      _id: 'u1',
      runId: 'run-x',
      source: 'USER',
      externalId: 'tx1',
      timestamp: new Date('2026-05-01T00:00:00Z'),
      asset: 'btc',
      quantity: 1.0,
      type: 'BUY',
      isValid: true
    };
    const exchangeTx = {
      _id: 'e1',
      runId: 'run-x',
      source: 'EXCHANGE',
      externalId: 'tx1',
      timestamp: new Date('2026-05-01T00:00:01Z'),
      asset: 'BTC',
      quantity: 1.0005,
      type: 'BUY',
      isValid: true
    };

    TransactionMock.find.mockImplementation((filter) => {
      if (filter.source === 'USER') return createFindResponse([userTx]);
      return createFindResponse([exchangeTx]);
    });

    const result = await engine.reconcile('run-x', { timestampToleranceSeconds: 10, quantityTolerancePct: 0.1 });

    expect(result).toEqual({
      matchedCount: 1,
      conflictingCount: 0,
      unmatchedUserCount: 0,
      unmatchedExchangeCount: 0
    });

    expect(TransactionMock.bulkWrite).toHaveBeenCalledWith(expect.any(Array), { ordered: false });
    const ops = TransactionMock.bulkWrite.mock.calls[0][0];
    expect(ops).toEqual(expect.arrayContaining([
      expect.objectContaining({
        updateOne: expect.objectContaining({
          filter: { _id: 'u1' },
          update: expect.objectContaining({ matchingStatus: 'MATCHED' })
        })
      }),
      expect.objectContaining({
        updateOne: expect.objectContaining({
          filter: { _id: 'e1' },
          update: expect.objectContaining({ matchingStatus: 'MATCHED' })
        })
      })
    ]));
  });

  it('classifies a transaction as conflicting when quantity variance exceeds tolerance', async () => {
    const userTx = {
      _id: 'u2',
      runId: 'run-x',
      source: 'USER',
      externalId: 'tx2',
      timestamp: new Date('2026-05-01T00:00:00Z'),
      asset: 'BTC',
      quantity: 1.0,
      type: 'BUY',
      isValid: true
    };
    const exchangeTx = {
      _id: 'e2',
      runId: 'run-x',
      source: 'EXCHANGE',
      externalId: 'tx2',
      timestamp: new Date('2026-05-01T00:00:02Z'),
      asset: 'BTC',
      quantity: 1.25,
      type: 'BUY',
      isValid: true
    };

    TransactionMock.find.mockImplementation((filter) => {
      if (filter.source === 'USER') return createFindResponse([userTx]);
      return createFindResponse([exchangeTx]);
    });

    const result = await engine.reconcile('run-x', { timestampToleranceSeconds: 10, quantityTolerancePct: 10 });

    expect(result).toEqual({
      matchedCount: 0,
      conflictingCount: 1,
      unmatchedUserCount: 0,
      unmatchedExchangeCount: 0
    });
    expect(TransactionMock.bulkWrite).toHaveBeenCalled();
    const ops = TransactionMock.bulkWrite.mock.calls[0][0];
    expect(ops).toEqual(expect.arrayContaining([
      expect.objectContaining({
        updateOne: expect.objectContaining({
          filter: { _id: 'u2' },
          update: expect.objectContaining({ matchingStatus: 'CONFLICTING' })
        })
      }),
      expect.objectContaining({
        updateOne: expect.objectContaining({
          filter: { _id: 'e2' },
          update: expect.objectContaining({ matchingStatus: 'CONFLICTING' })
        })
      })
    ]));
  });

  it('marks records as unmatched when they fall outside the time window', async () => {
    const userTx = {
      _id: 'u3',
      runId: 'run-x',
      source: 'USER',
      externalId: 'tx3',
      timestamp: new Date('2026-05-01T00:00:00Z'),
      asset: 'BTC',
      quantity: 1.0,
      type: 'SELL',
      isValid: true
    };
    const exchangeTx = {
      _id: 'e3',
      runId: 'run-x',
      source: 'EXCHANGE',
      externalId: 'tx3',
      timestamp: new Date('2026-05-01T01:00:00Z'),
      asset: 'BTC',
      quantity: 1.0,
      type: 'SELL',
      isValid: true
    };

    TransactionMock.find.mockImplementation((filter) => {
      if (filter.source === 'USER') return createFindResponse([userTx]);
      return createFindResponse([exchangeTx]);
    });

    const result = await engine.reconcile('run-x', { timestampToleranceSeconds: 30, quantityTolerancePct: 1 });

    expect(result).toEqual({
      matchedCount: 0,
      conflictingCount: 0,
      unmatchedUserCount: 1,
      unmatchedExchangeCount: 1
    });
    expect(TransactionMock.bulkWrite).toHaveBeenCalled();
  });

  it('skips exchange rows already matched by earlier user rows', async () => {
    const userTx1 = {
      _id: 'u4',
      runId: 'run-x',
      source: 'USER',
      externalId: 'tx4',
      timestamp: new Date('2026-05-01T00:00:00Z'),
      asset: 'BTC',
      quantity: 1.0,
      type: 'BUY',
      isValid: true
    };
    const userTx2 = {
      _id: 'u5',
      runId: 'run-x',
      source: 'USER',
      externalId: 'tx5',
      timestamp: new Date('2026-05-01T00:00:01Z'),
      asset: 'BTC',
      quantity: 1.0,
      type: 'BUY',
      isValid: true
    };
    const exchangeTx = {
      _id: 'e4',
      runId: 'run-x',
      source: 'EXCHANGE',
      externalId: 'tx4',
      timestamp: new Date('2026-05-01T00:00:00Z'),
      asset: 'BTC',
      quantity: 1.0,
      type: 'BUY',
      isValid: true
    };

    TransactionMock.find.mockImplementation((filter) => {
      if (filter.source === 'USER') return createFindResponse([userTx1, userTx2]);
      return createFindResponse([exchangeTx]);
    });

    const result = await engine.reconcile('run-x', { timestampToleranceSeconds: 10, quantityTolerancePct: 1 });

    expect(result.matchedCount).toBe(1);
    expect(result.unmatchedUserCount).toBe(1);
    expect(result.unmatchedExchangeCount).toBe(0);
  });

  it('handles zero quantity user transactions safely without division by zero', async () => {
    const userTx = {
      _id: 'u6',
      runId: 'run-x',
      source: 'USER',
      externalId: 'zero-qty',
      timestamp: new Date('2026-05-01T00:00:00Z'),
      asset: 'BTC',
      quantity: 0,
      type: 'BUY',
      isValid: true
    };
    const exchangeTx = {
      _id: 'e6',
      runId: 'run-x',
      source: 'EXCHANGE',
      externalId: 'zero-qty',
      timestamp: new Date('2026-05-01T00:00:00Z'),
      asset: 'BTC',
      quantity: 0,
      type: 'BUY',
      isValid: true
    };

    TransactionMock.find.mockImplementation((filter) => {
      if (filter.source === 'USER') return createFindResponse([userTx]);
      return createFindResponse([exchangeTx]);
    });

    const result = await engine.reconcile('run-x', { timestampToleranceSeconds: 10, quantityTolerancePct: 0 });

    expect(result.matchedCount).toBe(1);
    expect(result.conflictingCount).toBe(0);
  });
});
