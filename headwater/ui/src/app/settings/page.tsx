"use client";

import { useEffect, useState, useCallback } from "react";
import {
  api,
  type LLMSettings,
  type LLMSettingsUpdate,
  type LLMVerifyResponse,
} from "@/lib/api";

const PROVIDERS = [
  {
    value: "none",
    label: "None (Heuristic Only)",
    description: "Pattern-based enrichment only. No LLM calls. Free.",
  },
  {
    value: "ollama",
    label: "Ollama (Local)",
    description: "Local LLM via Ollama. Good quality, no API cost.",
  },
  {
    value: "anthropic",
    label: "Anthropic (Cloud)",
    description: "Claude models via Anthropic API. Best quality.",
  },
  {
    value: "openai_compat",
    label: "OpenAI-Compatible",
    description: "Any OpenAI-compatible endpoint (vLLM, Together, Groq, etc.).",
  },
];

export default function SettingsPage() {
  const [settings, setSettings] = useState<LLMSettings | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [verifying, setVerifying] = useState(false);
  const [verifyResult, setVerifyResult] = useState<LLMVerifyResponse | null>(
    null
  );
  const [enriching, setEnriching] = useState(false);
  const [showReEnrich, setShowReEnrich] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  // Form state
  const [provider, setProvider] = useState("none");
  const [model, setModel] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [ollamaUrl, setOllamaUrl] = useState("http://localhost:11434");
  const [openaiUrl, setOpenaiUrl] = useState("");
  const [openaiKey, setOpenaiKey] = useState("");

  // Ollama model list
  const [ollamaModels, setOllamaModels] = useState<string[]>([]);
  const [ollamaLoading, setOllamaLoading] = useState(false);
  const [ollamaError, setOllamaError] = useState("");

  const fetchOllamaModels = useCallback(async () => {
    setOllamaLoading(true);
    setOllamaError("");
    try {
      const res = await api.ollamaModels();
      if (res.error) {
        setOllamaError(
          `Cannot reach Ollama at ${res.base_url}: ${res.error}`
        );
        setOllamaModels([]);
      } else {
        setOllamaModels(res.models);
        // Auto-select first model if current model is invalid
        if (
          res.models.length > 0 &&
          (!model || !res.models.includes(model))
        ) {
          setModel(res.models[0]);
        }
      }
    } catch {
      setOllamaError("Failed to fetch Ollama models. Is the API running?");
      setOllamaModels([]);
    }
    setOllamaLoading(false);
  }, [model]);

  // Load settings on mount
  useEffect(() => {
    api
      .llmSettings()
      .then((s) => {
        setSettings(s);
        setProvider(s.provider);
        setModel(s.model);
        setOllamaUrl(s.ollama_base_url || "http://localhost:11434");
        setOpenaiUrl(s.openai_compat_base_url || "");
      })
      .catch(() => setError("Could not load settings. Is the API running?"))
      .finally(() => setLoading(false));
  }, []);

  // Fetch Ollama models when provider switches to ollama
  useEffect(() => {
    if (provider === "ollama") {
      fetchOllamaModels();
    }
  }, [provider, fetchOllamaModels]);

  const handleSave = async () => {
    setSaving(true);
    setError("");
    setMessage("");
    setVerifyResult(null);
    try {
      const body: LLMSettingsUpdate = {
        provider: provider as LLMSettingsUpdate["provider"],
      };
      if (model) body.model = model;
      if (apiKey) body.api_key = apiKey;
      if (ollamaUrl) body.ollama_base_url = ollamaUrl;
      if (openaiUrl) body.openai_compat_base_url = openaiUrl;
      if (openaiKey) body.api_key = openaiKey;

      const prevProvider = settings?.provider;
      const updated = await api.updateLLMSettings(body);
      setSettings(updated);
      setApiKey("");
      setOpenaiKey("");
      setMessage("Settings saved.");

      // Auto-verify after save (for non-none providers)
      if (updated.provider !== "none") {
        setVerifying(true);
        try {
          const result = await api.verifyLLM();
          setVerifyResult(result);
        } catch (e) {
          setVerifyResult({
            status: "error",
            provider: updated.provider,
            model: updated.model,
            detail: e instanceof Error ? e.message : String(e),
            latency_ms: null,
          });
        }
        setVerifying(false);
      }

      // If provider changed, prompt re-enrichment
      if (
        prevProvider &&
        prevProvider !== updated.provider &&
        updated.provider !== "none"
      ) {
        setShowReEnrich(true);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setSaving(false);
  };

  if (loading) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-4">Settings</h1>
        <p className="text-sm text-muted">Loading...</p>
      </div>
    );
  }

  return (
    <div className="max-w-2xl">
      <h1 className="text-2xl font-bold mb-2">Settings</h1>
      <p className="text-sm text-muted mb-6">
        Configure the LLM provider used for semantic enrichment, catalog
        generation, and query decomposition. The system works at all tiers --
        heuristic-only is fully functional.
      </p>

      {error && (
        <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded text-sm text-red-700">
          {error}
        </div>
      )}
      {message && (
        <div className="mb-4 p-3 bg-green-50 border border-green-200 rounded text-sm text-green-800">
          {message}
        </div>
      )}

      {/* Provider selection */}
      <div className="bg-card border border-border rounded-lg p-5 mb-6">
        <h2 className="text-sm font-semibold mb-3">LLM Provider</h2>
        <div className="space-y-2">
          {PROVIDERS.map((p) => (
            <label
              key={p.value}
              className={`flex items-start gap-3 p-3 border rounded-lg cursor-pointer transition-colors ${
                provider === p.value
                  ? "border-blue-500 bg-blue-50/50"
                  : "border-border hover:border-blue-300"
              }`}
            >
              <input
                type="radio"
                name="provider"
                value={p.value}
                checked={provider === p.value}
                onChange={() => {
                  setProvider(p.value);
                  setVerifyResult(null);
                  setError("");
                  setMessage("");
                  // Reset model -- Ollama models fetched via useEffect
                  if (p.value === "anthropic") {
                    setModel("claude-sonnet-4-20250514");
                  } else if (p.value !== "ollama") {
                    setModel("");
                  }
                }}
                className="mt-0.5"
              />
              <div>
                <div className="text-sm font-medium">{p.label}</div>
                <div className="text-xs text-muted">{p.description}</div>
              </div>
            </label>
          ))}
        </div>
      </div>

      {/* Ollama config -- dropdown from live model list */}
      {provider === "ollama" && (
        <div className="bg-card border border-border rounded-lg p-5 mb-6">
          <h2 className="text-sm font-semibold mb-3">Ollama Configuration</h2>
          <div className="space-y-3">
            <div>
              <label className="block text-xs text-muted mb-1">Model</label>
              {ollamaLoading ? (
                <div className="px-3 py-2 border border-border rounded bg-background text-sm text-muted">
                  Detecting models...
                </div>
              ) : ollamaError ? (
                <div>
                  <div className="px-3 py-2 border border-red-300 rounded bg-red-50 text-sm text-red-700 mb-2">
                    {ollamaError}
                  </div>
                  <button
                    onClick={fetchOllamaModels}
                    className="text-xs text-blue-600 underline hover:text-blue-800"
                  >
                    Retry
                  </button>
                </div>
              ) : ollamaModels.length === 0 ? (
                <div>
                  <div className="px-3 py-2 border border-amber-300 rounded bg-amber-50 text-sm text-amber-800 mb-2">
                    No models found. Pull a model first:{" "}
                    <code className="font-mono">ollama pull mistral</code>
                  </div>
                  <button
                    onClick={fetchOllamaModels}
                    className="text-xs text-blue-600 underline hover:text-blue-800"
                  >
                    Refresh
                  </button>
                </div>
              ) : (
                <div className="flex gap-2">
                  <select
                    value={model}
                    onChange={(e) => setModel(e.target.value)}
                    className="flex-1 px-3 py-2 border border-border rounded bg-background text-sm font-mono"
                  >
                    {ollamaModels.map((m) => (
                      <option key={m} value={m}>
                        {m}
                      </option>
                    ))}
                  </select>
                  <button
                    onClick={fetchOllamaModels}
                    disabled={ollamaLoading}
                    className="px-3 py-2 border border-border rounded text-xs text-muted hover:text-foreground transition-colors"
                    title="Refresh model list"
                  >
                    Refresh
                  </button>
                </div>
              )}
            </div>
            <div>
              <label className="block text-xs text-muted mb-1">Base URL</label>
              <input
                type="text"
                value={ollamaUrl}
                onChange={(e) => setOllamaUrl(e.target.value)}
                placeholder="http://localhost:11434"
                className="w-full px-3 py-2 border border-border rounded bg-background text-sm font-mono"
              />
            </div>
          </div>
        </div>
      )}

      {/* Anthropic config */}
      {provider === "anthropic" && (
        <div className="bg-card border border-border rounded-lg p-5 mb-6">
          <h2 className="text-sm font-semibold mb-3">
            Anthropic Configuration
          </h2>
          <div className="space-y-3">
            <div>
              <label className="block text-xs text-muted mb-1">Model</label>
              <input
                type="text"
                value={model}
                onChange={(e) => setModel(e.target.value)}
                placeholder="claude-sonnet-4-20250514"
                className="w-full px-3 py-2 border border-border rounded bg-background text-sm font-mono"
              />
            </div>
            <div>
              <label className="block text-xs text-muted mb-1">
                API Key{" "}
                {settings?.has_api_key && (
                  <span className="text-green-600">(set)</span>
                )}
              </label>
              <input
                type="password"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                placeholder={
                  settings?.has_api_key
                    ? "Leave blank to keep current"
                    : "sk-ant-..."
                }
                className="w-full px-3 py-2 border border-border rounded bg-background text-sm font-mono"
              />
            </div>
          </div>
        </div>
      )}

      {/* OpenAI-compat config */}
      {provider === "openai_compat" && (
        <div className="bg-card border border-border rounded-lg p-5 mb-6">
          <h2 className="text-sm font-semibold mb-3">
            OpenAI-Compatible Endpoint
          </h2>
          <div className="space-y-3">
            <div>
              <label className="block text-xs text-muted mb-1">Model</label>
              <input
                type="text"
                value={model}
                onChange={(e) => setModel(e.target.value)}
                placeholder="model-name"
                className="w-full px-3 py-2 border border-border rounded bg-background text-sm font-mono"
              />
            </div>
            <div>
              <label className="block text-xs text-muted mb-1">Base URL</label>
              <input
                type="text"
                value={openaiUrl}
                onChange={(e) => setOpenaiUrl(e.target.value)}
                placeholder="https://api.together.xyz/v1"
                className="w-full px-3 py-2 border border-border rounded bg-background text-sm font-mono"
              />
            </div>
            <div>
              <label className="block text-xs text-muted mb-1">
                API Key{" "}
                {settings?.has_openai_compat_key && (
                  <span className="text-green-600">(set)</span>
                )}
              </label>
              <input
                type="password"
                value={openaiKey}
                onChange={(e) => setOpenaiKey(e.target.value)}
                placeholder={
                  settings?.has_openai_compat_key
                    ? "Leave blank to keep current"
                    : "Enter API key"
                }
                className="w-full px-3 py-2 border border-border rounded bg-background text-sm font-mono"
              />
            </div>
          </div>
        </div>
      )}

      {/* Current saved configuration */}
      {settings && (
        <div className="bg-card border border-border rounded-lg p-5 mb-6">
          <h2 className="text-sm font-semibold mb-3">Current Configuration</h2>
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div>
              <span className="text-xs text-muted block">Provider</span>
              <span className="font-mono">{settings.provider}</span>
            </div>
            <div>
              <span className="text-xs text-muted block">Model</span>
              <span className="font-mono">{settings.model}</span>
            </div>
            {settings.provider === "anthropic" && (
              <div>
                <span className="text-xs text-muted block">API Key</span>
                <span>
                  {settings.has_api_key ? "Configured" : "Not set"}
                </span>
              </div>
            )}
            {settings.provider === "ollama" && (
              <div>
                <span className="text-xs text-muted block">Ollama URL</span>
                <span className="font-mono text-xs">
                  {settings.ollama_base_url}
                </span>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Save (auto-verifies after save) */}
      <div className="flex items-center gap-3 mb-6">
        <button
          onClick={handleSave}
          disabled={
            saving ||
            verifying ||
            (provider === "ollama" && !model) ||
            (provider === "ollama" && ollamaModels.length === 0)
          }
          className="px-6 py-2 bg-foreground text-background rounded-lg text-sm font-medium disabled:opacity-50 hover:opacity-90 transition-opacity"
        >
          {saving
            ? "Saving..."
            : verifying
              ? "Verifying..."
              : "Save & Verify"}
        </button>
      </div>

      {/* Verify result */}
      {verifyResult && (
        <div
          className={`mb-6 p-3 border rounded text-sm ${
            verifyResult.status === "ok"
              ? "bg-green-50 border-green-200 text-green-800"
              : "bg-red-50 border-red-200 text-red-700"
          }`}
        >
          {verifyResult.status === "ok" ? (
            <span>
              Connected to {verifyResult.provider} ({verifyResult.model})
              {verifyResult.latency_ms != null &&
                ` -- ${verifyResult.latency_ms}ms`}
              {verifyResult.detail && ` -- ${verifyResult.detail}`}
            </span>
          ) : (
            <span>{verifyResult.detail || "Connection failed"}</span>
          )}
        </div>
      )}

      {/* Re-enrich prompt */}
      {showReEnrich && (
        <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
          <p className="text-sm text-blue-800 mb-3">
            LLM provider changed. Re-run enrichment to update column
            descriptions?
          </p>
          {enriching && (
            <p className="text-xs text-blue-700 mb-3">
              Enriching with local LLM -- this may take a few minutes.
              Check the backend logs for progress.
            </p>
          )}
          <div className="flex gap-2">
            <button
              onClick={async () => {
                setEnriching(true);
                setError("");
                try {
                  const result = await api.reEnrich();
                  const parts = [`Re-enriched ${result.columns_enriched} columns with ${result.provider}.`];
                  if (result.skipped > 0) {
                    parts.push(`Skipped ${result.skipped} already-enriched tables.`);
                  }
                  if (result.catalog_metrics != null) {
                    parts.push(
                      `Rebuilt catalog: ${result.catalog_metrics} metrics, ${result.catalog_dimensions} dims, ${result.catalog_entities} entities.`
                    );
                  }
                  if (result.relationships != null) {
                    parts.push(`${result.relationships} relationships detected.`);
                  }
                  parts.push("Dictionary, Models, and Graph pages now reflect updated metadata.");
                  setMessage(parts.join(" "));
                  setShowReEnrich(false);
                } catch (e) {
                  const msg = e instanceof Error ? e.message : String(e);
                  if (msg.includes("timed out") || msg.includes("504")) {
                    setMessage(
                      "Enrichment is still running on the backend. Check logs for progress and refresh the page when done."
                    );
                  } else {
                    setError(msg);
                  }
                }
                setEnriching(false);
              }}
              disabled={enriching}
              className="px-4 py-1.5 bg-blue-600 text-white rounded text-sm font-medium hover:opacity-90 disabled:opacity-50"
            >
              {enriching ? "Re-enriching (this may take a few minutes)..." : "Re-enrich Now"}
            </button>
            {!enriching && (
              <button
                onClick={() => setShowReEnrich(false)}
                className="px-4 py-1.5 border border-border rounded text-sm text-muted hover:text-foreground"
              >
                Later
              </button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
