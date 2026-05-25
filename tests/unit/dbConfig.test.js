import { jest } from '@jest/globals';

const mongooseMock = {
  connect: jest.fn()
};

jest.unstable_mockModule('mongoose', () => ({ default: mongooseMock, connect: mongooseMock.connect }));
const { connectDB } = await import('../../config/db.config.js');

describe('db.config', () => {
  beforeEach(() => {
    mongooseMock.connect.mockReset();
  });

  it('connects successfully when mongoose resolves', async () => {
    mongooseMock.connect.mockResolvedValue({ connection: { host: 'localhost' } });
    await expect(connectDB()).resolves.toBeUndefined();
    expect(mongooseMock.connect).toHaveBeenCalled();
  });

  it('calls process.exit on database connection failure', async () => {
    const exitSpy = jest.spyOn(process, 'exit').mockImplementation(() => {});
    mongooseMock.connect.mockRejectedValue(new Error('db down'));

    await expect(connectDB()).resolves.toBeUndefined();
    expect(exitSpy).toHaveBeenCalledWith(1);

    exitSpy.mockRestore();
  });
});
