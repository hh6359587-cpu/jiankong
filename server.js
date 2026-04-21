import http from 'node:http';

const PORT = Number(process.env.PORT || 8090);
const REFRESH_INTERVAL_MS = 10 * 1000;
const DISCOVERY_INTERVAL_MS = 10 * 60 * 1000;
const HIGH_MARKET_CAP_THRESHOLD = 50_000_000;
const COINGECKO_MARKET_PAGES = 10;

const targetChains = {
  solana: { gecko: 'solana', label: 'SOL', coingeckoPlatform: 'solana' },
  ethereum: { gecko: 'eth', label: 'ETH', coingeckoPlatform: 'ethereum' },
  bsc: { gecko: 'bsc', label: 'BSC', coingeckoPlatform: 'binance-smart-chain' },
  base: { gecko: 'base', label: 'Base', coingeckoPlatform: 'base' }
};

const alphaSymbols = new Set([
  'APT', 'SEI', 'SUI', 'ICP', 'NEAR', 'AXS', 'SAND', 'MANA', 'THETA', 'ENJ',
  'CHZ', 'AAVE', 'MKR', 'UNI', 'COMP', 'LINK', 'MATIC', 'SOL', 'AVAX', 'DOT'
]);

const stableAndNativeSymbols = new Set([
  'USDT', 'USDC', 'DAI', 'BUSD', 'FRAX', 'TUSD', 'USDD', 'USDP', 'LUSD',
  'FDUSD', 'PYUSD', 'USDS', 'AUSD', 'EURC', 'GHO', 'CRVUSD', 'DOLA', 'MIM',
  'USD0', 'USDE', 'SUSDE', 'RLUSD', 'USDY', 'USYC', 'USD1', 'EURT', 'EURS',
  'WETH', 'WSOL', 'WBNB', 'WBTC', 'CBBTC', 'CBETH', 'RETH', 'WEETH',
  'SOL', 'ETH', 'BNB'
]);

const exchangePlatformSymbols = new Set([
  'BNB', 'OKB', 'OKT', 'LEO', 'CRO', 'GT', 'KCS', 'BGB', 'HT', 'HTX', 'MX',
  'BIT', 'WBT', 'WRX', 'CET', 'LBK', 'BMX', 'BTR', 'FTT', 'FTN', 'NEXO',
  'BEST', 'BTMX', 'ASD', 'TKX', 'GATE', 'BYD', 'KUB', 'BITCI'
]);

const state = {
  loading: false,
  lastUpdated: null,
  lastDiscovery: null,
  candidates: [],
  tokens: [],
  stats: {
    candidates: 0,
    displayed: 0,
    binanceExcluded: 0,
    platformExcluded: 0,
    stableExcluded: 0,
    errors: []
  }
};

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function normalizeSymbol(symbol) {
  return String(symbol || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function baseSymbolFromPair(symbol) {
  return normalizeSymbol(symbol).replace(/(USDT|BUSD|USDC|FDUSD|TUSD)$/g, '');
}

function normalizeAddress(chainId, address) {
  const value = String(address || '').trim();
  if (!value) return '';
  return chainId === 'solana' ? value : value.toLowerCase();
}

function tokenKey(token) {
  return `${token.chainId}-${normalizeAddress(token.chainId, token.baseToken?.address)}`;
}

function getMarketCap(token) {
  return Number(token.marketCap || token.fdv || 0) || 0;
}

function getLiquidity(token) {
  return Number(token.liquidity?.usd || 0) || 0;
}

function getVolume24h(token) {
  return Number(token.volume?.h24 || 0) || 0;
}

function isStableSymbol(symbol) {
  const normalized = normalizeSymbol(symbol);
  if (!normalized) return false;
  if (stableAndNativeSymbols.has(normalized)) return true;

  return (
    normalized.startsWith('USD') ||
    normalized.endsWith('USD') ||
    normalized.endsWith('USDT') ||
    normalized.endsWith('USDC') ||
    normalized.startsWith('EUR') ||
    normalized.endsWith('EUR')
  );
}

function isExchangePlatformSymbol(symbol) {
  return exchangePlatformSymbols.has(normalizeSymbol(symbol));
}

function isStableOrExchangePlatform(symbol) {
  return isStableSymbol(symbol) || isExchangePlatformSymbol(symbol);
}

function getAlertScore(token) {
  return Math.max(
    Math.abs(Number(token.priceChange?.m5 || 0)),
    Math.abs(Number(token.priceChange?.h1 || 0)),
    Math.abs(Number(token.priceChange?.h6 || 0)),
    Math.abs(Number(token.priceChange?.h24 || 0))
  );
}

function compareTokens(a, b) {
  const trustedDiff = Number(b.marketCapSource === 'coingecko') - Number(a.marketCapSource === 'coingecko');
  if (trustedDiff !== 0) return trustedDiff;

  if (a.marketCapSource === 'coingecko' && b.marketCapSource === 'coingecko') {
    const capDiff = getMarketCap(b) - getMarketCap(a);
    if (capDiff !== 0) return capDiff;
  }

  const alertDiff = getAlertScore(b) - getAlertScore(a);
  if (alertDiff !== 0) return alertDiff;
  const volumeDiff = getVolume24h(b) - getVolume24h(a);
  if (volumeDiff !== 0) return volumeDiff;
  const liquidityDiff = getLiquidity(b) - getLiquidity(a);
  if (liquidityDiff !== 0) return liquidityDiff;
  return getMarketCap(b) - getMarketCap(a);
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    headers: { accept: 'application/json', 'user-agent': 'jiankong-local-monitor/0.2.0' },
    ...options
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}: ${url}`);
  }
  return response.json();
}

async function fetchBinanceExcludedSymbols() {
  const excluded = new Set(alphaSymbols);
  const spot = await fetchJson('https://api.binance.com/api/v3/exchangeInfo').catch(() => ({ symbols: [] }));
  for (const symbol of spot.symbols || []) {
    const base = normalizeSymbol(symbol.baseAsset);
    if (base) excluded.add(base);
  }

  const futures = await fetchJson('https://fapi.binance.com/fapi/v1/exchangeInfo').catch(() => ({ symbols: [] }));
  for (const symbol of futures.symbols || []) {
    const base = normalizeSymbol(symbol.baseAsset);
    if (base) excluded.add(base);
  }

  const alphaExchangeInfo = await fetchJson('https://www.binance.com/bapi/defi/v1/public/alpha-trade/get-exchange-info')
    .catch(() => ({ data: { symbols: [] } }));
  for (const symbol of alphaExchangeInfo.data?.symbols || []) {
    const base = normalizeSymbol(String(symbol.baseAsset || '').replace(/^ALPHA_/, ''));
    if (base) excluded.add(base);
  }

  const alphaTokenList = await fetchJson('https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list')
    .catch(() => ({ data: [] }));
  const alphaTokens = Array.isArray(alphaTokenList.data) ? alphaTokenList.data : alphaTokenList.data?.list || [];
  for (const token of alphaTokens) {
    const symbol = normalizeSymbol(token.symbol);
    const alphaId = normalizeSymbol(String(token.alphaId || '').replace(/^ALPHA_/, ''));
    if (symbol) excluded.add(symbol);
    if (alphaId) excluded.add(alphaId);
  }

  return excluded;
}

async function fetchCoinGeckoCandidates() {
  const coinsList = await fetchJson('https://api.coingecko.com/api/v3/coins/list?include_platform=true')
    .catch(error => {
      console.warn(`CoinGecko platform list failed: ${error.message}`);
      return [];
    });

  const marketPages = await Promise.all(
    Array.from({ length: COINGECKO_MARKET_PAGES }, async (_, index) => {
      await sleep(index * 500);
      return fetchJson(`https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=${index + 1}&sparkline=false&price_change_percentage=24h`)
        .catch(error => {
          console.warn(`CoinGecko market page ${index + 1} skipped: ${error.message}`);
          return [];
        });
    })
  );

  const platformById = new Map();
  for (const coin of Array.isArray(coinsList) ? coinsList : []) {
    platformById.set(coin.id, coin.platforms || {});
  }

  const candidates = [];
  for (const market of marketPages.flat()) {
    const marketCap = Number(market.market_cap || market.fully_diluted_valuation || 0);
    if (marketCap < HIGH_MARKET_CAP_THRESHOLD) continue;

    const platforms = platformById.get(market.id) || {};
    for (const [chainId, config] of Object.entries(targetChains)) {
      const address = normalizeAddress(chainId, platforms[config.coingeckoPlatform]);
      if (!address) continue;

      candidates.push({
        chainId,
        pairAddress: address,
        dexId: 'coingecko',
        baseToken: { address, name: market.name || market.symbol, symbol: normalizeSymbol(market.symbol) },
        priceUsd: Number(market.current_price || 0),
        priceChange: { h24: Number(market.price_change_percentage_24h_in_currency || market.price_change_percentage_24h || 0) },
        volume: { h24: Number(market.total_volume || 0) },
        liquidity: { usd: 0 },
        marketCap,
        fdv: Number(market.fully_diluted_valuation || marketCap),
        marketCapSource: 'coingecko',
        url: `https://www.coingecko.com/en/coins/${market.id}`,
        info: { imageUrl: market.image },
        source: 'coingecko'
      });
    }
  }

  return candidates;
}

function geckoPoolToToken(pool, chainId) {
  const attrs = pool.attributes || {};
  const rels = pool.relationships || {};
  const baseRef = rels.base_token?.data;
  if (!baseRef?.id) return null;

  const idParts = String(baseRef.id).split('_');
  if (idParts.length < 2) return null;

  const address = normalizeAddress(chainId, idParts.slice(1).join('_'));
  const symbol = normalizeSymbol(attrs.name ? attrs.name.split('/')[0].trim() : '');
  if (!address || !symbol) return null;

  const poolAddress = attrs.address || String(pool.id || '').split('_').slice(1).join('_') || address;
  const fdv = Number(attrs.fdv_usd || 0);
  const marketCap = Number(attrs.market_cap_usd || 0) || fdv;

  return {
    chainId,
    pairAddress: poolAddress,
    dexId: rels.dex?.data?.id || '',
    baseToken: { address, name: symbol, symbol },
    priceUsd: Number(attrs.base_token_price_usd || 0),
    priceChange: {
      m5: Number(attrs.price_change_percentage?.m5 || 0),
      h1: Number(attrs.price_change_percentage?.h1 || 0),
      h6: Number(attrs.price_change_percentage?.h6 || 0),
      h24: Number(attrs.price_change_percentage?.h24 || 0)
    },
    volume: { h24: Number(attrs.volume_usd?.h24 || 0) },
    liquidity: { usd: Number(attrs.reserve_in_usd || 0) },
    marketCap,
    fdv,
    marketCapSource: 'dex',
    url: `https://dexscreener.com/${chainId}/${poolAddress}`,
    source: 'geckoterminal'
  };
}

async function fetchGeckoCandidates() {
  const candidates = [];
  const plans = [
    { sort: 'h24_volume_usd_desc', pages: 12 },
    { sort: 'h24_tx_count_desc', pages: 8 }
  ];

  for (const [chainId, config] of Object.entries(targetChains)) {
    const trending = await fetchJson(`https://api.geckoterminal.com/api/v2/networks/${config.gecko}/trending_pools`)
      .catch(() => ({ data: [] }));
    for (const pool of trending.data || []) {
      const token = geckoPoolToToken(pool, chainId);
      if (token) candidates.push(token);
    }

    for (const plan of plans) {
      for (let page = 1; page <= plan.pages; page++) {
        await sleep(120);
        const data = await fetchJson(`https://api.geckoterminal.com/api/v2/networks/${config.gecko}/pools?page=${page}&sort=${plan.sort}`)
          .catch(() => ({ data: [] }));
        for (const pool of data.data || []) {
          const token = geckoPoolToToken(pool, chainId);
          if (token) candidates.push(token);
        }
      }
    }
  }

  return candidates;
}

async function fetchDexExtraCandidates() {
  const candidates = [];

  function addPair(pair) {
    if (!pair?.baseToken?.address || !targetChains[pair.chainId]) return;
    pair.baseToken.address = normalizeAddress(pair.chainId, pair.baseToken.address);
    candidates.push({ ...pair, source: 'dexscreener' });
  }

  const endpoints = [
    'https://api.dexscreener.com/token-boosts/latest/v1',
    'https://api.dexscreener.com/token-boosts/top/v1'
  ];

  const boostItems = [];
  for (const endpoint of endpoints) {
    const data = await fetchJson(endpoint).catch(() => []);
    boostItems.push(...(Array.isArray(data) ? data : [data]));
  }

  const byChain = {};
  for (const item of boostItems) {
    if (!item?.chainId || !item?.tokenAddress || !targetChains[item.chainId]) continue;
    if (!byChain[item.chainId]) byChain[item.chainId] = new Set();
    byChain[item.chainId].add(normalizeAddress(item.chainId, item.tokenAddress));
  }

  for (const [chainId, addresses] of Object.entries(byChain)) {
    const arr = Array.from(addresses);
    for (let i = 0; i < arr.length; i += 30) {
      const pairs = await fetchJson(`https://api.dexscreener.com/tokens/v1/${chainId}/${arr.slice(i, i + 30).join(',')}`)
        .catch(() => []);
      if (Array.isArray(pairs)) pairs.forEach(addPair);
    }
  }

  return candidates;
}

function mergeToken(base, next) {
  const baseMarketCap = getMarketCap(base);
  const nextMarketCap = getMarketCap(next);
  const baseHasTrustedCap = base.marketCapSource === 'coingecko';
  const nextHasTrustedCap = next.marketCapSource === 'coingecko';
  const marketCap = baseHasTrustedCap ? baseMarketCap : nextHasTrustedCap ? nextMarketCap : baseMarketCap || nextMarketCap;
  const marketCapSource = baseHasTrustedCap || nextHasTrustedCap ? 'coingecko' : base.marketCapSource || next.marketCapSource || 'dex';

  return {
    ...base,
    ...next,
    baseToken: next.baseToken || base.baseToken,
    quoteToken: next.quoteToken || base.quoteToken,
    priceUsd: Number(next.priceUsd || 0) || Number(base.priceUsd || 0),
    priceChange: next.priceChange || base.priceChange || {},
    volume: getVolume24h(next) > 0 ? next.volume : base.volume,
    liquidity: getLiquidity(next) > 0 ? next.liquidity : base.liquidity,
    marketCap,
    fdv: Number(next.fdv || 0) || Number(base.fdv || 0),
    marketCapSource,
    url: next.url || base.url,
    info: next.info || base.info,
    source: `${base.source || 'unknown'}+${next.source || 'unknown'}`
  };
}

function filterAndRank(tokens, excludedSymbols) {
  let binanceExcluded = 0;
  let platformExcluded = 0;
  let stableExcluded = 0;
  const merged = new Map();

  for (const token of tokens) {
    const symbol = normalizeSymbol(token.baseToken?.symbol);
    const address = normalizeAddress(token.chainId, token.baseToken?.address);
    if (!targetChains[token.chainId] || !symbol || !address) continue;
    if (isStableSymbol(symbol)) {
      stableExcluded++;
      continue;
    }
    if (isExchangePlatformSymbol(symbol)) {
      platformExcluded++;
      continue;
    }
    if (excludedSymbols.has(symbol)) {
      binanceExcluded++;
      continue;
    }

    token.baseToken.address = address;
    const key = tokenKey(token);
    merged.set(key, merged.has(key) ? mergeToken(merged.get(key), token) : token);
  }

  const bySymbolChain = new Map();
  for (const token of merged.values()) {
    const marketCap = getMarketCap(token);
    const volume24h = getVolume24h(token);
    const liquidity = getLiquidity(token);
    const isTrustedMarketCap = token.marketCapSource === 'coingecko';
    const isHighMarketCap = isTrustedMarketCap && marketCap >= HIGH_MARKET_CAP_THRESHOLD;
    const isActiveCandidate = liquidity >= 100_000 && volume24h >= 25_000;
    if (!isHighMarketCap && !isActiveCandidate) continue;

    const key = `${normalizeSymbol(token.baseToken?.symbol)}-${token.chainId}`;
    const existing = bySymbolChain.get(key);
    if (!existing || compareTokens(token, existing) < 0) {
      bySymbolChain.set(key, token);
    }
  }

  const ranked = Array.from(bySymbolChain.values()).sort(compareTokens);

  return { tokens: ranked, binanceExcluded, platformExcluded, stableExcluded };
}

async function discoverCandidates() {
  const errors = [];
  const [excludedSymbols, coinGecko, gecko, dexExtras] = await Promise.all([
    fetchBinanceExcludedSymbols(),
    fetchCoinGeckoCandidates().catch(error => {
      errors.push(`CoinGecko: ${error.message}`);
      return [];
    }),
    fetchGeckoCandidates().catch(error => {
      errors.push(`GeckoTerminal: ${error.message}`);
      return [];
    }),
    fetchDexExtraCandidates().catch(error => {
      errors.push(`DexScreener: ${error.message}`);
      return [];
    })
  ]);

  const candidates = [...coinGecko, ...gecko, ...dexExtras];
  const filtered = filterAndRank(candidates, excludedSymbols);

  state.candidates = filtered.tokens;
  state.tokens = filtered.tokens;
  state.lastDiscovery = new Date().toISOString();
  state.stats = {
    candidates: candidates.length,
    displayed: filtered.tokens.length,
    binanceExcluded: filtered.binanceExcluded,
    platformExcluded: filtered.platformExcluded,
    stableExcluded: filtered.stableExcluded,
    errors
  };
}

async function refreshDexQuotes() {
  const byChain = {};
  for (const token of state.candidates) {
    const address = normalizeAddress(token.chainId, token.baseToken?.address);
    if (!address) continue;
    if (!byChain[token.chainId]) byChain[token.chainId] = [];
    byChain[token.chainId].push(address);
  }

  const freshPairs = new Map();
  for (const [chainId, addresses] of Object.entries(byChain)) {
    const unique = Array.from(new Set(addresses));
    for (let i = 0; i < unique.length; i += 30) {
      const batch = unique.slice(i, i + 30);
      const pairs = await fetchJson(`https://api.dexscreener.com/tokens/v1/${chainId}/${batch.join(',')}`)
        .catch(() => []);
      if (!Array.isArray(pairs)) continue;
      for (const pair of pairs) {
        if (!pair?.baseToken?.address) continue;
        pair.baseToken.address = normalizeAddress(chainId, pair.baseToken.address);
        const key = `${chainId}-${pair.baseToken.address}`;
        const current = freshPairs.get(key);
        if (!current || getLiquidity(pair) > getLiquidity(current)) {
          freshPairs.set(key, { ...pair, source: 'dexscreener-live' });
        }
      }
      await sleep(80);
    }
  }

  state.tokens = state.candidates.map(token => {
    const fresh = freshPairs.get(tokenKey(token));
    return fresh ? mergeToken(token, fresh) : token;
  }).sort(compareTokens);
  state.lastUpdated = new Date().toISOString();
  state.stats.displayed = state.tokens.length;
}

async function refreshAll(forceDiscovery = false) {
  if (state.loading) return;
  state.loading = true;
  try {
    const discoveryStale = !state.lastDiscovery || Date.now() - Date.parse(state.lastDiscovery) > DISCOVERY_INTERVAL_MS;
    if (forceDiscovery || discoveryStale || state.candidates.length === 0) {
      await discoverCandidates();
    }
    await refreshDexQuotes();
  } finally {
    state.loading = false;
  }
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET,OPTIONS',
    'access-control-allow-headers': 'content-type',
    'cache-control': 'no-store'
  });
  res.end(JSON.stringify(payload));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (req.method === 'OPTIONS') {
    sendJson(res, 200, {});
    return;
  }

  if (url.pathname === '/api/health') {
    sendJson(res, 200, { ok: true, loading: state.loading, lastUpdated: state.lastUpdated });
    return;
  }

  if (url.pathname === '/api/onchain') {
    if (url.searchParams.get('refresh') === '1') {
      refreshAll(true).catch(error => state.stats.errors.push(error.message));
    } else if (!state.loading && (!state.lastUpdated || Date.now() - Date.parse(state.lastUpdated) > REFRESH_INTERVAL_MS)) {
      refreshAll(false).catch(error => state.stats.errors.push(error.message));
    }

    sendJson(res, 200, {
      ok: true,
      loading: state.loading,
      lastUpdated: state.lastUpdated,
      lastDiscovery: state.lastDiscovery,
      refreshIntervalMs: REFRESH_INTERVAL_MS,
      chains: Object.keys(targetChains),
      stats: state.stats,
      tokens: state.tokens
    });
    return;
  }

  sendJson(res, 404, { ok: false, error: 'Not found' });
});

server.listen(PORT, () => {
  console.log(`Onchain aggregator running at http://localhost:${PORT}`);
  refreshAll(true).catch(error => {
    state.stats.errors.push(error.message);
    console.error(error);
  });
  setInterval(() => {
    refreshAll(false).catch(error => {
      state.stats.errors.push(error.message);
      console.error(error);
    });
  }, REFRESH_INTERVAL_MS);
});
