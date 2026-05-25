import fs from 'fs';
import os from 'os';
import path from 'path';
import { jest } from '@jest/globals';

const TransactionMock = {
  bulkWrite: jest.fn()
};

jest.unstable_mockModule('../../models/transaction.model.js', () => ({ Transaction: TransactionMock }));

const { IngestionService } = await import('../../services/IngestionService.js');

describe('IngestionService', () => {
  let ingestionService;
  let tempFilePath;

  beforeEach(() => {
    ingestionService = new IngestionService();
    TransactionMock.bulkWrite.mockReset();
  });

  afterEach(async () => {
    if (tempFilePath && fs.existsSync(tempFilePath)) {
      await fs.promises.unlink(tempFilePath);
      tempFilePath = null;
    }
  });

  describe('validateAndNormalize()', () => {
    it('normalizes valid CSV input into a valid transaction document', () => {
      const row = {
        transaction_id: 'abc123',
        type: 'buy',
        asset: ' ethereum ',
        quantity: '2.5',
        timestamp: '2026-04-01T12:00:00Z'
      };

      const normalized = ingestionService.validateAndNormalize(row, 'USER', 'run-1');

      expect(normalized).toMatchObject({
        runId: 'run-1',
        source: 'USER',
        externalId: 'abc123',
        asset: 'ETH',
        quantity: 2.5,
        type: 'BUY',
        isValid: true,
        validationErrors: [],
        matchingStatus: 'UNMATCHED'
      });
      expect(normalized.timestamp.toISOString()).toBe('2026-04-01T12:00:00.000Z');
    });

    it('flags missing fields and malformed values without dropping rows', () => {
      const row = {
        transaction_id: '',
        type: '',
        asset: '',
        quantity: 'notanumber',
        timestamp: 'invalid-date'
      };

      const normalized = ingestionService.validateAndNormalize(row, 'EXCHANGE', 'run-2');

      expect(normalized.isValid).toBe(false);
      expect(normalized.externalId).toBe('MALFORMED_ROW');
      expect(normalized.asset).toBe('UNKNOWN');
      expect(normalized.quantity).toBe(0);
      expect(normalized.validationErrors).toEqual(expect.arrayContaining([
        'Missing transactional reference ID.',
        'Missing transactional execution operation type.',
        'Missing transactional token asset label.',
        "Invalid quantity syntax: 'notanumber' cannot be cast into a number.",
        "Invalid timestamp format: 'invalid-date' is unparseable."
      ]));
    });

    it('rejects invalid transaction type values', () => {
      const row = {
        transaction_id: 'tx-invalid-type',
        type: 'UNKNOWN_TYPE',
        asset: 'BTC',
        quantity: '1',
        timestamp: '2026-05-01T00:00:00Z'
      };

      const normalized = ingestionService.validateAndNormalize(row, 'USER', 'run-3');

      expect(normalized.isValid).toBe(false);
      expect(normalized.validationErrors).toEqual(expect.arrayContaining([
        "Invalid transaction type: 'UNKNOWN_TYPE'. Expected BUY, SELL, TRANSFER_IN, or TRANSFER_OUT."
      ]));
      expect(normalized.type).toBe('UNKNOWN_TYPE');
    });
  });

  describe('processBatch()', () => {
    it('does nothing when the batch is empty', async () => {
      await expect(ingestionService.processBatch([])).resolves.toBeUndefined();
      expect(TransactionMock.bulkWrite).not.toHaveBeenCalled();
    });

    it('writes transactional upserts in unordered mode', async () => {
      const batch = [
        ingestionService.validateAndNormalize({ transaction_id: 'tx1', type: 'BUY', asset: 'BTC', quantity: '3', timestamp: '2026-05-01T00:00:00Z' }, 'USER', 'runId'),
        ingestionService.validateAndNormalize({ transaction_id: 'tx2', type: 'SELL', asset: 'ETH', quantity: '1.5', timestamp: '2026-05-01T00:05:00Z' }, 'EXCHANGE', 'runId')
      ];

      TransactionMock.bulkWrite.mockResolvedValue({});
      await ingestionService.processBatch(batch);

      expect(TransactionMock.bulkWrite).toHaveBeenCalledTimes(1);
      const [argOps, argOptions] = TransactionMock.bulkWrite.mock.calls[0];
      expect(argOptions).toEqual({ ordered: false });
      expect(argOps).toHaveLength(2);
      expect(argOps[0].updateOne.filter._id).toBeDefined();
      expect(argOps[1].updateOne.filter._id).toBeDefined();
    });

    it('routes individual bulk write failures to dead-letter handling', async () => {
      const batch = [
        ingestionService.validateAndNormalize({ transaction_id: 'tx1', type: 'BUY', asset: 'BTC', quantity: '3', timestamp: '2026-05-01T00:00:00Z' }, 'USER', 'runId')
      ];

      const error = new Error('duplicate key');
      error.name = 'BulkWriteError';
      error.writeErrors = [
        { index: 0, errmsg: 'duplicate key error' }
      ];

      const routeSpy = jest.spyOn(ingestionService, 'routeToDeadLetterQueue').mockImplementation(() => {});
      TransactionMock.bulkWrite.mockRejectedValue(error);

      await expect(ingestionService.processBatch(batch)).resolves.toBeUndefined();
      expect(routeSpy).toHaveBeenCalledWith(expect.objectContaining({
        validationErrors: expect.arrayContaining([expect.stringContaining('Database Constraint Error: duplicate key error')])
      }));
      routeSpy.mockRestore();
    });

    it('throws on systemic database failures', async () => {
      const batch = [
        ingestionService.validateAndNormalize({ transaction_id: 'tx1', type: 'BUY', asset: 'BTC', quantity: '3', timestamp: '2026-05-01T00:00:00Z' }, 'USER', 'runId')
      ];

      const error = new Error('connection lost');
      TransactionMock.bulkWrite.mockRejectedValue(error);

      await expect(ingestionService.processBatch(batch)).rejects.toThrow('connection lost');
    });
  });

  describe('ingestData()', () => {
    it('reads CSV rows and persists them in batch operations', async () => {
      const csvText = 'transaction_id,type,asset,quantity,timestamp\nabc,BUY,BTC,1.1,2026-05-01T00:00:00Z\ndef,SELL,ETH,2,2026-05-01T00:01:00Z\n';
      tempFilePath = path.join(os.tmpdir(), `ingest-${Date.now()}.csv`);
      await fs.promises.writeFile(tempFilePath, csvText, 'utf8');

      const spy = jest.spyOn(ingestionService, 'processBatch');
      spy.mockResolvedValue();

      await expect(ingestionService.ingestData(tempFilePath, 'USER', 'runId')).resolves.toBeUndefined();
      expect(spy).toHaveBeenCalled();
      spy.mockRestore();
    });

    it('rejects when the file cannot be read', async () => {
      await expect(ingestionService.ingestData('missing-file.csv', 'USER', 'runId')).rejects.toThrow(/ENOENT|no such file or directory/i);
    });
  });
});
