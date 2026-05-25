import { jest } from '@jest/globals';
import { createWritableCapture } from '../utils/streamMocks.js';

const RunMock = {
  findById: jest.fn()
};
const TransactionMock = {
  find: jest.fn()
};

jest.unstable_mockModule('../../models/run.model.js', () => ({ Run: RunMock }));
jest.unstable_mockModule('../../models/transaction.model.js', () => ({ Transaction: TransactionMock }));

const { ReportService } = await import('../../services/reportService.js');

describe('ReportService', () => {
  let service;

  beforeEach(() => {
    service = new ReportService();
    RunMock.findById.mockReset();
    TransactionMock.find.mockReset();
  });

  it('throws when the run cannot be found', async () => {
    RunMock.findById.mockReturnValue({
      lean: jest.fn().mockResolvedValue(null)
    });
    await expect(service.generateFullCsvReport('bad-id', createWritableCapture())).rejects.toThrow('could not be located');
  });

  it('writes matched and unmatched rows into a CSV stream', async () => {
    const resultStream = createWritableCapture();
    RunMock.findById.mockReturnValue({
      lean: jest.fn().mockResolvedValue({ _id: 'run-id' })
    });
    TransactionMock.find.mockReturnValue({
      lean: jest.fn().mockResolvedValue([
        {
          _id: 'u1',
          runId: 'run-id',
          source: 'USER',
          externalId: 'match-1',
          timestamp: new Date('2026-01-01T00:00:00Z'),
          type: 'BUY',
          asset: 'BTC',
          quantity: 1.0,
          matchingStatus: 'MATCHED',
          reconciliationReason: 'Matched successfully',
          rawRow: { price_usd: '100', fee: '0.1' }
        },
        {
          _id: 'e1',
          runId: 'run-id',
          source: 'EXCHANGE',
          externalId: 'match-1',
          timestamp: new Date('2026-01-01T00:00:01Z'),
          type: 'BUY',
          asset: 'BTC',
          quantity: 1.0,
          matchingStatus: 'MATCHED',
          reconciliationReason: 'Matched successfully',
          rawRow: { price_usd: '100', fee: '0.1' }
        },
        {
          _id: 'u2',
          runId: 'run-id',
          source: 'USER',
          externalId: 'only-user',
          timestamp: new Date('2026-01-02T00:00:00Z'),
          type: 'SELL',
          asset: 'ETH',
          quantity: 2.0,
          matchingStatus: 'UNMATCHED',
          reconciliationReason: 'No corresponding record logged by User within historical time blocks',
          rawRow: { price_usd: '200', fee: '0.2' }
        }
      ])
    });

    await expect(service.generateFullCsvReport('run-id', resultStream)).resolves.toBeUndefined();

    expect(resultStream.data).toContain('Category,Reason,User_Tx_ID');
    expect(resultStream.data).toContain('MATCHED');
    expect(resultStream.data).toContain('UNMATCHED_USER_ONLY');
    expect(resultStream.data).toContain('only-user');
  });

  it('pairs duplicate external IDs consistently in the generated CSV', async () => {
    const resultStream = createWritableCapture();
    RunMock.findById.mockReturnValue({
      lean: jest.fn().mockResolvedValue({ _id: 'run-id' })
    });
    TransactionMock.find.mockReturnValue({
      lean: jest.fn().mockResolvedValue([
        {
          _id: 'u10',
          runId: 'run-id',
          source: 'USER',
          externalId: 'dup',
          timestamp: new Date('2026-01-01T00:00:00Z'),
          type: 'BUY',
          asset: 'BTC',
          quantity: 1.0,
          matchingStatus: 'MATCHED',
          reconciliationReason: 'Matched successfully',
          rawRow: { price_usd: '100', fee: '0.1' }
        },
        {
          _id: 'u11',
          runId: 'run-id',
          source: 'USER',
          externalId: 'dup',
          timestamp: new Date('2026-01-01T00:00:01Z'),
          type: 'BUY',
          asset: 'BTC',
          quantity: 2.0,
          matchingStatus: 'MATCHED',
          reconciliationReason: 'Matched successfully',
          rawRow: { price_usd: '101', fee: '0.2' }
        },
        {
          _id: 'e10',
          runId: 'run-id',
          source: 'EXCHANGE',
          externalId: 'dup',
          timestamp: new Date('2026-01-01T00:00:00Z'),
          type: 'BUY',
          asset: 'BTC',
          quantity: 1.0,
          matchingStatus: 'MATCHED',
          reconciliationReason: 'Matched successfully',
          rawRow: { price_usd: '100', fee: '0.1' }
        },
        {
          _id: 'e11',
          runId: 'run-id',
          source: 'EXCHANGE',
          externalId: 'dup',
          timestamp: new Date('2026-01-01T00:00:01Z'),
          type: 'BUY',
          asset: 'BTC',
          quantity: 2.0,
          matchingStatus: 'MATCHED',
          reconciliationReason: 'Matched successfully',
          rawRow: { price_usd: '101', fee: '0.2' }
        }
      ])
    });

    await expect(service.generateFullCsvReport('run-id', resultStream)).resolves.toBeUndefined();
    expect(resultStream.data.match(/MATCHED/g).length).toBeGreaterThanOrEqual(2);
  });
});
