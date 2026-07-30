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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export async function jsonFetch(
  url,
  options = {},
  { timeoutMs = 15_000, retries = 3, fetchImpl = fetch } = {},
) {
  let lastError;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetchImpl(url, { ...options, signal: controller.signal });
      const text = await response.text();
      if (!response.ok) {
        const retryable = response.status === 429 || response.status >= 500;
        const error = new Error(`${response.status}: ${text.slice(0, 300)}`);
        error.retryable = retryable;
        throw error;
      }
      try {
        return JSON.parse(text);
      } catch {
        throw new Error(`Invalid JSON: ${text.slice(0, 300)}`);
      }
    } catch (error) {
      lastError = error;
      const retryable = error?.name === "AbortError" || error?.retryable ||
        error instanceof TypeError;
      if (!retryable || attempt === retries) throw error;
      await sleep(Math.min(8_000, 750 * (2 ** attempt) + Math.random() * 250));
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastError || new Error("Fetch retries exhausted");
}

async function inflows(chainId) {
  const response = await jsonFetch(
    "https://www.binance.com/bapi/defi/v1/public/wallet-direct/tracker/wallet/token/inflow/rank/query/ai",
    {
      method: "POST",
      headers: UA,
      body: JSON.stringify({ chainId, period: "5m", tagType: 2 }),
    },
    { timeoutMs: 10_000, retries: 2 },
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
  const response = await jsonFetch(url, { headers: UA }, {
    timeoutMs: 10_000,
    retries: 2,
  });
  return Array.isArray(response?.data?.data) ? response.data.data : [];
}

export async function runRelay({
  ingestUrl = process.env.BOT_INGEST_URL,
  relayToken = process.env.BOT_RELAY_TOKEN,
} = {}) {
  if (!ingestUrl || !relayToken) throw new Error(
    "BOT_INGEST_URL and BOT_RELAY_TOKEN are required",
  );
  const results = await Promise.all(NETWORKS.map(async (config) => {
    try {
      const [smartInflows, walletLeaders] = await Promise.all([
        inflows(config.chainId), leaders(config.chainId),
      ]);
      return {
        ...config, ok: true, smartInflows, walletLeaders,
        counts: { smartInflows: smartInflows.length, walletLeaders: walletLeaders.length },
      };
    } catch (error) {
      console.error(`${config.network}: ${error.message}`);
      return {
        ...config, ok: false, smartInflows: [], walletLeaders: [],
        counts: { smartInflows: 0, walletLeaders: 0 },
        error: String(error.message).slice(0, 500),
      };
    }
  }));
  const networks = results;
  const errors = results.filter((item) => !item.ok)
    .map((item) => ({ network: item.network, message: item.error }));
  if (errors.length === NETWORKS.length) {
    throw new Error(`All Binance networks failed: ${JSON.stringify(errors)}`);
  }
  const runId = process.env.GITHUB_RUN_ID || crypto.randomUUID();
  const response = await jsonFetch(ingestUrl, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${relayToken}` },
    body: JSON.stringify({
      schemaVersion: 2,
      runId,
      generatedAt: new Date().toISOString(),
      source: "github-actions",
      status: errors.length ? "partial" : "ok",
      errors,
      networks,
    }),
  }, { timeoutMs: 45_000, retries: 3 });
  const summary = {
    runId,
    status: errors.length ? "partial" : "ok",
    networks: Object.fromEntries(networks.map((item) => [item.network, item.counts])),
    accepted: response?.accepted || {},
  };
  console.log(JSON.stringify(summary));
  return summary;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await runRelay();
}
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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export async function jsonFetch(
  url,
  options = {},
  { timeoutMs = 15_000, retries = 3, fetchImpl = fetch } = {},
) {
  let lastError;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetchImpl(url, { ...options, signal: controller.signal });
      const text = await response.text();
      if (!response.ok) {
        const retryable = response.status === 429 || response.status >= 500;
        const error = new Error(`${response.status}: ${text.slice(0, 300)}`);
        error.retryable = retryable;
        throw error;
      }
      try {
        return JSON.parse(text);
      } catch {
        throw new Error(`Invalid JSON: ${text.slice(0, 300)}`);
      }
    } catch (error) {
      lastError = error;
      const retryable = error?.name === "AbortError" || error?.retryable ||
        error instanceof TypeError;
      if (!retryable || attempt === retries) throw error;
      await sleep(Math.min(8_000, 750 * (2 ** attempt) + Math.random() * 250));
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastError || new Error("Fetch retries exhausted");
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

export async function runRelay({
  ingestUrl = process.env.BOT_INGEST_URL,
  relayToken = process.env.BOT_RELAY_TOKEN,
} = {}) {
  if (!ingestUrl || !relayToken) throw new Error(
    "BOT_INGEST_URL and BOT_RELAY_TOKEN are required",
  );
  const networks = [];
  const errors = [];
  for (const config of NETWORKS) {
    try {
      const [smartInflows, walletLeaders] = await Promise.all([
        inflows(config.chainId), leaders(config.chainId),
      ]);
      networks.push({
        ...config, ok: true, smartInflows, walletLeaders,
        counts: { smartInflows: smartInflows.length, walletLeaders: walletLeaders.length },
      });
    } catch (error) {
      console.error(`${config.network}: ${error.message}`);
      errors.push({ network: config.network, message: String(error.message).slice(0, 500) });
      networks.push({
        ...config, ok: false, smartInflows: [], walletLeaders: [],
        counts: { smartInflows: 0, walletLeaders: 0 },
        error: String(error.message).slice(0, 500),
      });
    }
  }
  if (errors.length === NETWORKS.length) {
    throw new Error(`All Binance networks failed: ${JSON.stringify(errors)}`);
  }
  const runId = process.env.GITHUB_RUN_ID || crypto.randomUUID();
  const response = await jsonFetch(ingestUrl, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${relayToken}` },
    body: JSON.stringify({
      schemaVersion: 2,
      runId,
      generatedAt: new Date().toISOString(),
      source: "github-actions",
      status: errors.length ? "partial" : "ok",
      errors,
      networks,
    }),
  }, { timeoutMs: 45_000, retries: 3 });
  const summary = {
    runId,
    status: errors.length ? "partial" : "ok",
    networks: Object.fromEntries(networks.map((item) => [item.network, item.counts])),
    accepted: response?.accepted || {},
  };
  console.log(JSON.stringify(summary));
  return summary;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await runRelay();
}
