import express from 'express';
import request from 'supertest';
import { jest } from '@jest/globals';

const reconcileHandler = jest.fn((req, res) => res.status(200).json({ success: true }));
const downloadHandler = jest.fn((req, res) => res.status(200).json({ success: true }));
const summaryHandler = jest.fn((req, res) => res.status(200).json({ success: true }));
const unmatchedHandler = jest.fn((req, res) => res.status(200).json({ success: true }));

jest.unstable_mockModule('../../controllers/reconcile.controller.js', () => ({ startReconcile: reconcileHandler }));
jest.unstable_mockModule('../../controllers/report.controller.js', () => ({
  downloadReportController: downloadHandler,
  summaryReport: summaryHandler,
  unmatchedReport: unmatchedHandler
}));

const { reconcileRouter } = await import('../../routes/reconcile.route.js');
const { reportRouter } = await import('../../routes/report.route.js');

const createApp = () => {
  const app = express();
  app.use(express.json());
  app.use('/api/v1/reconcile', reconcileRouter);
  app.use('/api/v1/report', reportRouter);
  return app;
};

describe('router contract tests', () => {
  it('routes POST /api/v1/reconcile to startReconcile', async () => {
    const app = createApp();
    const response = await request(app)
      .post('/api/v1/reconcile')
      .send({});

    expect(response.status).toBe(200);
    expect(response.body).toEqual({ success: true });
    expect(reconcileHandler).toHaveBeenCalled();
  });

  it('validates runId path parameter for report routes', async () => {
    const app = createApp();
    const response = await request(app).get('/api/v1/report/not-a-valid-id');

    expect(response.status).toBe(400);
    expect(response.body).toEqual({
      success: false,
      message: "Malformed parameter input: 'not-a-valid-id' is not a valid MongoDB Hexadecimal ObjectId format."
    });
    expect(downloadHandler).not.toHaveBeenCalled();
  });

  it('routes report summary and unmatched paths', async () => {
    const validId = '000000000000000000000000';
    const app = createApp();

    await request(app).get(`/api/v1/report/${validId}/summary`).expect(200);
    await request(app).get(`/api/v1/report/${validId}/unmatched`).expect(200);

    expect(summaryHandler).toHaveBeenCalled();
    expect(unmatchedHandler).toHaveBeenCalled();
  });
});
