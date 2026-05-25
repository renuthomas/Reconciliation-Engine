import { jest } from '@jest/globals';
import mongoose from 'mongoose';
import { validateRunIdParam } from '../../utils/validationId.js';

describe('validateRunIdParam middleware', () => {
  it('calls next for a valid MongoDB ObjectId', () => {
    const req = { params: { runId: new mongoose.Types.ObjectId().toString() } };
    const res = { status: jest.fn(() => res), json: jest.fn() };
    const next = jest.fn();

    validateRunIdParam(req, res, next);

    expect(next).toHaveBeenCalledTimes(1);
    expect(res.status).not.toHaveBeenCalled();
  });

  it('returns 400 for malformed ObjectId parameters', () => {
    const req = { params: { runId: 'not-an-object-id' } };
    const json = jest.fn();
    const status = jest.fn(() => ({ json }));
    const res = { status, json };
    const next = jest.fn();

    validateRunIdParam(req, res, next);

    expect(next).not.toHaveBeenCalled();
    expect(status).toHaveBeenCalledWith(400);
    expect(json).toHaveBeenCalledWith({
      success: false,
      message: "Malformed parameter input: 'not-an-object-id' is not a valid MongoDB Hexadecimal ObjectId format."
    });
  });
});
