import { mapType, normalizeAsset } from '../../utils/normalizers.js';

describe('normalizers utility', () => {
  describe('normalizeAsset', () => {
    it('normalizes common long-form assets to their ticker aliases', () => {
      expect(normalizeAsset(' bitcoin ')).toBe('BTC');
      expect(normalizeAsset('Ether')).toBe('ETH');
      expect(normalizeAsset('xbt')).toBe('BTC');
      expect(normalizeAsset('Ethereum')).toBe('ETH');
    });

    it('uppercases and trims unknown assets', () => {
      expect(normalizeAsset(' sol ')).toBe('SOL');
      expect(normalizeAsset('usd-coin')).toBe('USD-COIN');
    });

    it('returns empty string for nullish asset values', () => {
      expect(normalizeAsset(null)).toBe('');
      expect(normalizeAsset(undefined)).toBe('');
      expect(normalizeAsset('')).toBe('');
    });
  });

  describe('mapType', () => {
    it('flips transfer directions while preserving buy/sell', () => {
      expect(mapType('TRANSFER_OUT')).toBe('TRANSFER_IN');
      expect(mapType('transfer_in')).toBe('TRANSFER_OUT');
      expect(mapType('BUY')).toBe('BUY');
      expect(mapType('sell')).toBe('SELL');
    });
  });
});
