import { jest } from '@jest/globals';

const RunMock = {
  exists: jest.fn(),
  findById: jest.fn()
};
const TransactionMock = {
  find: jest.fn()
};
const reportServiceMock = {
  generateFullCsvReport: jest.fn()
};

jest.unstable_mockModule('../../models/run.model.js', () => ({ Run: RunMock }));
jest.unstable_mockModule('../../models/transaction.model.js', () => ({ Transaction: TransactionMock }));
jest.unstable_mockModule('../../services/reportService.js', () => ({
  ReportService: jest.fn(() => reportServiceMock)
}));

const { downloadReportController, summaryReport, unmatchedReport } = await import('../../controllers/report.controller.js');

describe('report.controller', () => {
  beforeEach(() => {
    RunMock.exists.mockReset();
    RunMock.findById.mockReset();
    TransactionMock.find.mockReset();
    reportServiceMock.generateFullCsvReport.mockReset();
  });

  it('streams the report when the run exists', async () => {
    RunMock.exists.mockResolvedValue(true);
    reportServiceMock.generateFullCsvReport.mockResolvedValue();
    const res = {
      setHeader: jest.fn(() => { res.headersSent = true; }),
      headersSent: false
    };
    const req = { params: { runId: 'run-id' } };

    await downloadReportController(req, res);

    expect(RunMock.exists).toHaveBeenCalledWith({ _id: 'run-id' });
    expect(res.setHeader).toHaveBeenCalledWith('Content-Type', 'text/csv');
    expect(res.setHeader).toHaveBeenCalledWith('Content-Disposition', 'attachment; filename="reconciliation_report_run-id.csv"');
    expect(reportServiceMock.generateFullCsvReport).toHaveBeenCalledWith('run-id', res);
  });

  it('ends the response when report streaming fails after headers are already sent', async () => {
    RunMock.exists.mockResolvedValue(true);
    reportServiceMock.generateFullCsvReport.mockRejectedValue(new Error('stream failure'));
    const res = {
      setHeader: jest.fn(() => { res.headersSent = true; }),
      headersSent: false,
      end: jest.fn()
    };
    const req = { params: { runId: 'run-id' } };

    await downloadReportController(req, res);

    expect(res.end).toHaveBeenCalled();
  });

  it('returns 404 when the run does not exist for download', async () => {
    RunMock.exists.mockResolvedValue(false);
    const res = {
      status: jest.fn(() => res),
      json: jest.fn()
    };
    const req = { params: { runId: 'bad-id' } };

    await downloadReportController(req, res);

    expect(res.status).toHaveBeenCalledWith(404);
    expect(res.json).toHaveBeenCalledWith({ success: false, message: 'Reconciliation run bad-id does not exist.' });
  });

  it('returns 200 summary when the run exists', async () => {
    const run = { status: 'COMPLETED', summary: { matchedCount: 1, conflictingCount: 0, unmatchedUserCount: 0, unmatchedExchangeCount: 0 } };
    RunMock.findById.mockReturnValue({
      select: jest.fn(() => ({ lean: jest.fn().mockResolvedValue(run) }))
    });
    const res = {
      status: jest.fn(() => res),
      json: jest.fn()
    };
    const req = { params: { runId: 'run-id' } };

    await summaryReport(req, res);

    expect(res.status).toHaveBeenCalledWith(200);
    expect(res.json).toHaveBeenCalledWith({ success: true, runId: 'run-id', status: 'COMPLETED', summary: run.summary });
  });

  it('returns 404 when summary run is missing', async () => {
    RunMock.findById.mockReturnValue({
      select: jest.fn(() => ({ lean: jest.fn().mockResolvedValue(null) }))
    });
    const res = {
      status: jest.fn(() => res),
      json: jest.fn()
    };
    const req = { params: { runId: 'missing' } };

    await summaryReport(req, res);

    expect(res.status).toHaveBeenCalledWith(404);
    expect(res.json).toHaveBeenCalledWith({ success: false, message: 'Reconciliation run summary for ID missing could not be located.' });
  });

  it('returns unmatched records when the run exists', async () => {
    RunMock.exists.mockResolvedValue(true);
    TransactionMock.find.mockReturnValue({
      select: jest.fn(() => ({
        lean: jest.fn().mockResolvedValue([{ externalId: 'tx-x', source: 'USER' }])
      }))
    });
    const res = {
      status: jest.fn(() => res),
      json: jest.fn()
    };
    const req = { params: { runId: 'run-id' } };

    await unmatchedReport(req, res);

    expect(TransactionMock.find).toHaveBeenCalledWith({ runId: 'run-id', matchingStatus: 'UNMATCHED' });
    expect(res.status).toHaveBeenCalledWith(200);
    expect(res.json).toHaveBeenCalledWith({ success: true, runId: 'run-id', count: 1, records: [{ externalId: 'tx-x', source: 'USER' }] });
  });

  it('returns 404 when unmatched report run is missing', async () => {
    RunMock.exists.mockResolvedValue(false);
    const res = {
      status: jest.fn(() => res),
      json: jest.fn()
    };
    const req = { params: { runId: 'not-found' } };

    await unmatchedReport(req, res);

    expect(res.status).toHaveBeenCalledWith(404);
    expect(res.json).toHaveBeenCalledWith({ success: false, message: 'Reconciliation run matching records for ID not-found do not exist.' });
  });
});
