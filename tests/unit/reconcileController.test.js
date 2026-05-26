import { jest } from '@jest/globals';
import { Readable } from 'stream';

const RunMock = {
  findOneAndUpdate: jest.fn(),
  findByIdAndUpdate: jest.fn()
};
const ingestInstance = { ingestData: jest.fn().mockResolvedValue() };
const matchingInstance = { reconcile: jest.fn().mockResolvedValue({ matchedCount: 0, conflictingCount: 0, unmatchedUserCount: 0, unmatchedExchangeCount: 0 }) };
const IngestionServiceMock = jest.fn(() => ingestInstance);
const MatchingEngineMock = jest.fn(() => matchingInstance);
const createReadStreamMock = jest.fn();
const StatSyncMock = jest.fn();

jest.unstable_mockModule('fs', () => ({ default: { createReadStream: createReadStreamMock, statSync: StatSyncMock }, createReadStream: createReadStreamMock, statSync: StatSyncMock }));
jest.unstable_mockModule('../../models/run.model.js', () => ({ Run: RunMock }));
jest.unstable_mockModule('../../services/IngestionService.js', () => ({
  IngestionService: IngestionServiceMock
}));
jest.unstable_mockModule('../../services/MatchingEngine.js', () => ({
  MatchingEngine: MatchingEngineMock
}));

const { startReconcile } = await import('../../controllers/reconcile.controller.js');

describe('startReconcile controller', () => {
  let req;
  let res;

  beforeEach(() => {
    req = { body: {} };
    res = {
      status: jest.fn(function (code) { this.statusCode = code; return this; }),
      json: jest.fn()
    };
    RunMock.findOneAndUpdate.mockReset();
    RunMock.findByIdAndUpdate.mockReset();
    ingestInstance.ingestData.mockReset().mockResolvedValue();
    matchingInstance.reconcile.mockReset().mockResolvedValue({ matchedCount: 0, conflictingCount: 0, unmatchedUserCount: 0, unmatchedExchangeCount: 0 });
    StatSyncMock.mockReset();
    StatSyncMock.mockReturnValue({ size: 100, mtimeMs: 1000 });
    createReadStreamMock.mockReset().mockImplementation(() => Readable.from(['dummy']));
  });

  it('creates a new run and executes the reconciliation pipeline', async () => {
    RunMock.findOneAndUpdate.mockResolvedValue({ value: { _id: 'run-1', status: 'PROCESSING' }, lastErrorObject: { updatedExisting: false } });
    req.body = { timestampToleranceSeconds: 120, quantityTolerancePct: 0.5 };

    await startReconcile(req, res);

    expect(RunMock.findOneAndUpdate).toHaveBeenCalled();
    expect(ingestInstance.ingestData).toHaveBeenCalledTimes(2);
    expect(matchingInstance.reconcile).toHaveBeenCalledWith('run-1', { timestampToleranceSeconds: 120, quantityTolerancePct: 0.5 });
    expect(RunMock.findByIdAndUpdate).toHaveBeenCalledWith('run-1', expect.objectContaining({ status: 'COMPLETED' }));
    expect(res.status).toHaveBeenCalledWith(200);
    expect(res.json).toHaveBeenCalledWith(expect.objectContaining({ success: true, runId: 'run-1' }));
  });

  it('preserves zero tolerance values instead of falling back to defaults', async () => {
    RunMock.findOneAndUpdate.mockResolvedValue({ value: { _id: 'run-5', status: 'PROCESSING' }, lastErrorObject: { updatedExisting: false } });
    req.body = { timestampToleranceSeconds: 0, quantityTolerancePct: 0 };

    await startReconcile(req, res);

    expect(matchingInstance.reconcile).toHaveBeenCalledWith('run-5', { timestampToleranceSeconds: 0, quantityTolerancePct: 0 });
  });

  it('returns immediately for a completed run without reprocessing', async () => {
    RunMock.findOneAndUpdate.mockResolvedValue({ value: { _id: 'run-2', status: 'COMPLETED' }, lastErrorObject: { updatedExisting: true } });

    await startReconcile(req, res);

    expect(ingestInstance.ingestData).not.toHaveBeenCalled();
    expect(matchingInstance.reconcile).not.toHaveBeenCalled();
    expect(RunMock.findByIdAndUpdate).not.toHaveBeenCalled();
    expect(res.status).toHaveBeenCalledWith(200);
    expect(res.json).toHaveBeenCalledWith(expect.objectContaining({ message: expect.stringContaining('[IDEMPOTENCY]') }));
  });

  it('resumes a failed run by resetting status and continuing processing', async () => {
    RunMock.findOneAndUpdate
      .mockResolvedValueOnce({ value: { _id: 'run-3', status: 'FAILED' }, lastErrorObject: { updatedExisting: true } })
      .mockResolvedValueOnce({ value: { _id: 'run-3', status: 'PROCESSING' }, lastErrorObject: { updatedExisting: true } });

    await startReconcile(req, res);

    expect(RunMock.findOneAndUpdate).toHaveBeenCalledWith(
      { _id: 'run-3', status: 'FAILED' },
      { $set: { status: 'PROCESSING', errorMessage: null } }
    );
    expect(ingestInstance.ingestData).toHaveBeenCalledTimes(2);
    expect(res.status).toHaveBeenCalledWith(200);
  });

  it('marks the run as FAILED and returns 500 when ingestion throws', async () => {
    RunMock.findOneAndUpdate.mockResolvedValue({ value: { _id: 'run-4', status: 'PROCESSING' }, lastErrorObject: { updatedExisting: false } });
    ingestInstance.ingestData.mockRejectedValue(new Error('ingest failure'));

    await startReconcile(req, res);

    expect(RunMock.findByIdAndUpdate).toHaveBeenCalledWith('run-4', expect.objectContaining({ status: 'FAILED', errorMessage: 'ingest failure' }));
    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.json).toHaveBeenCalledWith(expect.objectContaining({ success: false, message: expect.stringContaining('fatal') }));
  });
});
