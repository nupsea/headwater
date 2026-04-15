"use client";

import { useEffect, useState } from "react";
import { api, type LLMSettings, type LLMSettingsUpdate } from "@/lib/api";

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
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  // Form state
  const [provider, setProvider] = useState("none");
  const [model, setModel] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [ollamaUrl, setOllamaUrl] = useState("");
  const [openaiUrl, setOpenaiUrl] = useState("");
  const [openaiKey, setOpenaiKey] = useState("");

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

  const handleSave = async () => {
    setSaving(true);
    setError("");
    setMessage("");
    try {
      const body: LLMSettingsUpdate = { provider: provider as LLMSettingsUpdate["provider"] };
      if (model) body.model = model;
      if (apiKey) body.api_key = apiKey;
      if (ollamaUrl) body.ollama_base_url = ollamaUrl;
      if (openaiUrl) body.openai_compat_base_url = openaiUrl;
      if (openaiKey) body.api_key = openaiKey;

      const updated = await api.updateLLMSettings(body);
      setSettings(updated);
      setApiKey("");
      setOpenaiKey("");
      setMessage("Settings saved.");
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
                onChange={() => setProvider(p.value)}
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

      {/* Provider-specific config */}
      {provider === "ollama" && (
        <div className="bg-card border border-border rounded-lg p-5 mb-6">
          <h2 className="text-sm font-semibold mb-3">Ollama Configuration</h2>
          <div className="space-y-3">
            <div>
              <label className="block text-xs text-muted mb-1">Model</label>
              <input
                type="text"
                value={model}
                onChange={(e) => setModel(e.target.value)}
                placeholder="llama3.1:8b"
                className="w-full px-3 py-2 border border-border rounded bg-background text-sm font-mono"
              />
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

      {provider === "anthropic" && (
        <div className="bg-card border border-border rounded-lg p-5 mb-6">
          <h2 className="text-sm font-semibold mb-3">Anthropic Configuration</h2>
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
                API Key {settings?.has_api_key && <span className="text-green-600">(set)</span>}
              </label>
              <input
                type="password"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                placeholder={settings?.has_api_key ? "Leave blank to keep current" : "sk-ant-..."}
                className="w-full px-3 py-2 border border-border rounded bg-background text-sm font-mono"
              />
            </div>
          </div>
        </div>
      )}

      {provider === "openai_compat" && (
        <div className="bg-card border border-border rounded-lg p-5 mb-6">
          <h2 className="text-sm font-semibold mb-3">OpenAI-Compatible Endpoint</h2>
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
                API Key {settings?.has_openai_compat_key && <span className="text-green-600">(set)</span>}
              </label>
              <input
                type="password"
                value={openaiKey}
                onChange={(e) => setOpenaiKey(e.target.value)}
                placeholder={settings?.has_openai_compat_key ? "Leave blank to keep current" : "Enter API key"}
                className="w-full px-3 py-2 border border-border rounded bg-background text-sm font-mono"
              />
            </div>
          </div>
        </div>
      )}

      {/* Current status */}
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
                <span>{settings.has_api_key ? "Configured" : "Not set"}</span>
              </div>
            )}
            {settings.provider === "ollama" && (
              <div>
                <span className="text-xs text-muted block">Ollama URL</span>
                <span className="font-mono text-xs">{settings.ollama_base_url}</span>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Save */}
      <button
        onClick={handleSave}
        disabled={saving}
        className="px-6 py-2 bg-foreground text-background rounded-lg text-sm font-medium disabled:opacity-50 hover:opacity-90 transition-opacity"
      >
        {saving ? "Saving..." : "Save Settings"}
      </button>
    </div>
  );
}
