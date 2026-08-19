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
let lastRelayFingerprint = "";
let lastRelaySentAt = 0;

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
  continuous = false,
  heartbeatMs = 60_000,
  source = process.env.RELAY_SOURCE ||
    (process.env.GITHUB_ACTIONS ? "github-actions" : "continuous-relay"),
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
  const allFailed = errors.length === NETWORKS.length;
  const runId = process.env.GITHUB_RUN_ID || crypto.randomUUID();
  const fingerprint = JSON.stringify(networks.map((item) => ({
    network: item.network,
    ok: item.ok,
    smartInflows: item.smartInflows,
    walletLeaders: item.walletLeaders,
  })));
  if (continuous && fingerprint === lastRelayFingerprint &&
      Date.now() - lastRelaySentAt < heartbeatMs) {
    return {
      runId,
      status: "unchanged",
      skipped: true,
      networks: Object.fromEntries(networks.map((item) => [item.network, item.counts])),
    };
  }
  // Deliver each network independently. A large all-chain payload made one
  // slow D1 ingestion hold the whole relay until GitHub killed the job. This
  // also means BSC can remain tradable when Solana or Base is rate-limited.
  const deliveries = await Promise.all(networks.map(async (networkResult) => {
    const networkErrors = networkResult.ok ? [] : [{
      network: networkResult.network,
      message: networkResult.error,
    }];
    const status = networkResult.ok ? "ok" : "failed";
    try {
      const response = await jsonFetch(ingestUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${relayToken}`,
        },
        body: JSON.stringify({
          schemaVersion: 2,
          runId: `${runId}:${networkResult.network}`,
          generatedAt: new Date().toISOString(),
          source,
          status,
          errors: networkErrors,
          networks: [networkResult],
        }),
      }, { timeoutMs: 45_000, retries: 0 });
      return { network: networkResult.network, ok: true, accepted: response?.accepted || {} };
    } catch (error) {
      return {
        network: networkResult.network,
        ok: false,
        error: String(error?.message || error).slice(0, 500),
      };
    }
  }));
  const deliveryFailures = deliveries.filter((item) => !item.ok);
  const summary = {
    runId,
    status: deliveryFailures.length === networks.length ? "failed" :
      errors.length || deliveryFailures.length ? "partial" : "ok",
    networks: Object.fromEntries(networks.map((item) => [item.network, item.counts])),
    accepted: Object.assign({}, ...deliveries.filter((item) => item.ok)
      .map((item) => item.accepted)),
    deliveryFailures,
  };
  lastRelayFingerprint = fingerprint;
  lastRelaySentAt = Date.now();
  console.log(JSON.stringify(summary));
  if (allFailed || deliveryFailures.length === networks.length) {
    throw new Error(`Relay unavailable: ${JSON.stringify({ errors, deliveryFailures })}`);
  }
  return summary;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await runRelay();
}
