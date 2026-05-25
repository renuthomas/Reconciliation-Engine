/**
 * Maps inverted workflow types to ensure the engine compares structural equivalents
 */
const mapType = (type) => {
  const normalized = type.toUpperCase();
  if (normalized === 'TRANSFER_OUT') return 'TRANSFER_IN';
  if (normalized === 'TRANSFER_IN') return 'TRANSFER_OUT';
  return normalized; // BUY, SELL remain matching from both views
};

/**
 * Normalizes ticker symbols and long-form asset labels
 */
const normalizeAsset = (asset) => {
  if (!asset) return '';
  const clean = asset.trim().toUpperCase();
  
  const aliasDictionary = {
    'BITCOIN': 'BTC',
    'XBT': 'BTC',
    'ETHER': 'ETH',
    'ETHEREUM': 'ETH'
  };

  return aliasDictionary[clean] || clean;
};

export { mapType, normalizeAsset };