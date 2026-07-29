const UA = {
  "Accept-Encoding": "identity",
  "User-Agent": "binance-web3/3.0 (MultichainLabRelay)",
  "content-type": "application/json",
};

const NETWORKS = [
  { network: "bsc", chainId: "56" },
  { network: "base", chainId: "8453" },
  { network: "solana", chainId: "CT_501" },
];

async function jsonFetch(url, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15_000);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    const text = await response.text();
    if (!response.ok) throw new Error(`${response.status}: ${text.slice(0, 200)}`);
    return JSON.parse(text);
  } finally {
    clearTimeout(timer);
  }
}

async function inflows(chainId) {
  const response = await jsonFetch(
    "https://www.binance.com/bapi/defi/v1/public/wallet-direct/tracker/wallet/token/inflow/rank/query/ai",
    {
      method: "POST",
      headers: UA,
      body: JSON.stringify({ chainId, period: "5m", tagType: 2 }),
    },
  );
  return Array.isArray(response?.data) ? response.data : [];
}

async function leaders(chainId) {
  const url = new URL(
    "https://www.binance.com/bapi/defi/v1/public/wallet-direct/market/leaderboard/query/ai",
  );
  for (const [key, value] of Object.entries({
    chainId, period: "30d", tag: "ALL", pageNo: 1, pageSize: 25,
  })) url.searchParams.set(key, String(value));
  const response = await jsonFetch(url, { headers: UA });
  return Array.isArray(response?.data?.data) ? response.data.data : [];
}

async function main() {
  const ingestUrl = process.env.BOT_INGEST_URL;
  const relayToken = process.env.BOT_RELAY_TOKEN;
  if (!ingestUrl || !relayToken) throw new Error(
    "BOT_INGEST_URL and BOT_RELAY_TOKEN are required",
  );
  const networks = [];
  for (const config of NETWORKS) {
    try {
      const [smartInflows, walletLeaders] = await Promise.all([
        inflows(config.chainId), leaders(config.chainId),
      ]);
      networks.push({ ...config, smartInflows, walletLeaders });
    } catch (error) {
      console.error(`${config.network}: ${error.message}`);
      networks.push({ ...config, smartInflows: [], walletLeaders: [] });
    }
  }
  const response = await jsonFetch(ingestUrl, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${relayToken}` },
    body: JSON.stringify({ generatedAt: new Date().toISOString(), networks }),
  });
  console.log(JSON.stringify(response));
}

await main();
