/**
 * OpenCode CLI Integration
 * ========================
 *
 * Integration with OpenCode (https://opencode.ai) via its HTTP server mode.
 *
 * Unlike claude/cursor/gemini/codex which spawn per-prompt subprocesses,
 * OpenCode runs a long-lived local HTTP+SSE daemon (`opencode serve`).
 * This module:
 *  - lazily starts a single daemon on first use
 *  - subscribes to a single SSE stream and routes events by sessionID
 *  - exposes the same `spawnOpencode` / `abortOpencodeSession` / `isOpencodeSessionActive` surface
 *    as other provider wrappers, so the rest of the codebase treats it the same way
 *
 * Event mapping (SSE → NormalizedMessage):
 *   session.created              → session_created
 *   session.status busy          → status {text:'Processing'}
 *   session.idle                 → complete
 *   message.part.delta text      → stream_delta / thinking (by parent part type)
 *   message.part.updated tool-*  → tool_use / tool_result
 *   message.updated finish       → complete
 *   error                        → error
 */

import { spawn } from 'child_process';
import { createNormalizedMessage, generateMessageId } from './providers/types.js';
import { notifyRunFailed, notifyRunStopped } from './services/notification-orchestrator.js';

const DEFAULT_HOST = '127.0.0.1';
const DEFAULT_PORT = Number(process.env.OPENCODE_PORT || 4096);
const READY_TIMEOUT_MS = 15000;
const READY_POLL_MS = 250;
const SSE_RECONNECT_DELAY_MS = 2000;

// ─── Daemon lifecycle ────────────────────────────────────────────────────────

/** @type {import('child_process').ChildProcess | null} */
let daemonProcess = null;
/** @type {Promise<string> | null} */
let daemonReadyPromise = null;
/** @type {string | null} */
let daemonBaseUrl = null;

function getBaseUrl() {
  return daemonBaseUrl || `http://${DEFAULT_HOST}:${DEFAULT_PORT}`;
}

async function pingHealth(baseUrl) {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 1500);
    const res = await fetch(`${baseUrl}/global/health`, { signal: controller.signal });
    clearTimeout(timeout);
    if (!res.ok) return false;
    const body = await res.json();
    return Boolean(body?.healthy);
  } catch {
    return false;
  }
}

async function waitForReady(baseUrl, timeoutMs = READY_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await pingHealth(baseUrl)) return true;
    await new Promise((r) => setTimeout(r, READY_POLL_MS));
  }
  return false;
}

async function startDaemon() {
  const baseUrl = `http://${DEFAULT_HOST}:${DEFAULT_PORT}`;

  // External daemon already running? Reuse it.
  if (await pingHealth(baseUrl)) {
    daemonBaseUrl = baseUrl;
    return baseUrl;
  }

  const child = spawn('opencode', ['serve', '--port', String(DEFAULT_PORT), '--hostname', DEFAULT_HOST], {
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: false,
    env: { ...process.env },
  });

  daemonProcess = child;

  child.stderr.on('data', (buf) => {
    const line = String(buf).trim();
    if (line) console.log('[opencode-daemon]', line);
  });

  child.on('exit', (code, signal) => {
    console.log(`[opencode-daemon] exited (code=${code}, signal=${signal})`);
    if (daemonProcess === child) {
      daemonProcess = null;
      daemonReadyPromise = null;
      daemonBaseUrl = null;
    }
  });

  const ok = await waitForReady(baseUrl);
  if (!ok) {
    try { child.kill('SIGTERM'); } catch { /* noop */ }
    daemonProcess = null;
    daemonReadyPromise = null;
    throw new Error(`opencode serve failed to become healthy within ${READY_TIMEOUT_MS}ms`);
  }

  daemonBaseUrl = baseUrl;
  return baseUrl;
}

async function ensureDaemon() {
  if (daemonBaseUrl && await pingHealth(daemonBaseUrl)) return daemonBaseUrl;
  if (!daemonReadyPromise) {
    daemonReadyPromise = startDaemon().catch((err) => {
      daemonReadyPromise = null;
      throw err;
    });
  }
  return daemonReadyPromise;
}

// ─── SSE consumer (single subscriber, routes by sessionID) ──────────────────

/** @type {Map<string, { ws: any, partTypeById: Map<string, string> }>} */
const sessionRoutes = new Map();
let sseAbortController = null;

async function ensureSseSubscription() {
  if (sseAbortController) return;
  const baseUrl = await ensureDaemon();
  const controller = new AbortController();
  sseAbortController = controller;

  (async () => {
    while (!controller.signal.aborted) {
      try {
        const res = await fetch(`${baseUrl}/event`, { signal: controller.signal });
        if (!res.body) throw new Error('no SSE body');

        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buf = '';

        while (!controller.signal.aborted) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });

          let idx;
          while ((idx = buf.indexOf('\n\n')) !== -1) {
            const frame = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            const dataLine = frame.split('\n').find((l) => l.startsWith('data: '));
            if (!dataLine) continue;
            const json = dataLine.slice(6);
            try {
              const evt = JSON.parse(json);
              dispatchEvent(evt);
            } catch (e) {
              console.warn('[opencode] bad SSE frame:', e.message);
            }
          }
        }
      } catch (err) {
        if (controller.signal.aborted) break;
        console.warn('[opencode] SSE stream error, reconnecting:', err.message);
        await new Promise((r) => setTimeout(r, SSE_RECONNECT_DELAY_MS));
      }
    }
  })();
}

function dispatchEvent(evt) {
  const sessionID = evt?.properties?.sessionID;
  if (!sessionID) return;

  const route = sessionRoutes.get(sessionID);
  if (!route || !route.ws) return;

  const normalized = normalizeSseEvent(evt, sessionID, route);
  for (const msg of normalized) {
    try {
      // writer.send() is a WebSocketWriter that handles readyState + JSON.stringify.
      // Pass the raw NormalizedMessage — the client unpacks by `kind`.
      route.ws.send(msg);
    } catch (e) {
      console.warn('[opencode] ws send failed:', e.message);
    }
  }

  if (normalized.some((m) => m.kind === 'complete' || m.kind === 'error')) {
    sessionRoutes.delete(sessionID);
  }
}

/**
 * Translate an OpenCode SSE event into zero-or-more NormalizedMessages.
 * Tracks part types per session so delta events can be classified (text vs reasoning).
 */
export function normalizeSseEvent(evt, sessionID, route) {
  const provider = 'opencode';
  const ts = new Date().toISOString();
  const type = evt?.type;
  const props = evt?.properties || {};

  if (type === 'session.created') {
    return [createNormalizedMessage({
      id: generateMessageId('opencode'),
      sessionId: sessionID,
      timestamp: ts,
      provider,
      kind: 'session_created',
      newSessionId: sessionID,
    })];
  }

  if (type === 'session.status') {
    const statusType = props?.status?.type;
    if (statusType === 'busy') {
      return [createNormalizedMessage({
        id: generateMessageId('opencode'),
        sessionId: sessionID,
        timestamp: ts,
        provider,
        kind: 'status',
        text: 'Processing',
        canInterrupt: true,
      })];
    }
    return [];
  }

  if (type === 'session.idle') {
    return [createNormalizedMessage({
      id: generateMessageId('opencode'),
      sessionId: sessionID,
      timestamp: ts,
      provider,
      kind: 'complete',
    })];
  }

  if (type === 'message.part.updated') {
    const part = props?.part;
    if (!part?.id || !part?.type) return [];
    route?.partTypeById?.set(part.id, part.type);

    if (part.type === 'tool-call') {
      return [createNormalizedMessage({
        id: part.id,
        sessionId: sessionID,
        timestamp: ts,
        provider,
        kind: 'tool_use',
        toolName: part.toolName || part.tool || 'Unknown',
        toolInput: part.input || part.arguments || {},
        toolId: part.id,
      })];
    }
    if (part.type === 'tool-result') {
      return [createNormalizedMessage({
        id: generateMessageId('opencode'),
        sessionId: sessionID,
        timestamp: ts,
        provider,
        kind: 'tool_result',
        toolId: part.toolCallId || part.id,
        content: typeof part.output === 'string' ? part.output : JSON.stringify(part.output || ''),
        isError: Boolean(part.isError),
      })];
    }
    return [];
  }

  if (type === 'message.part.delta') {
    const partID = props?.partID;
    if (!partID || props?.field !== 'text') return [];
    const partType = route?.partTypeById?.get(partID) || 'text';
    const kind = partType === 'reasoning' ? 'thinking' : 'stream_delta';
    return [createNormalizedMessage({
      id: generateMessageId('opencode'),
      sessionId: sessionID,
      timestamp: ts,
      provider,
      kind,
      content: props?.delta || '',
    })];
  }

  if (type === 'message.updated') {
    const info = props?.info;
    if (info?.role === 'assistant' && info?.finish) {
      return [createNormalizedMessage({
        id: generateMessageId('opencode'),
        sessionId: sessionID,
        timestamp: ts,
        provider,
        kind: 'complete',
      })];
    }
    return [];
  }

  if (type === 'error') {
    return [createNormalizedMessage({
      id: generateMessageId('opencode'),
      sessionId: sessionID,
      timestamp: ts,
      provider,
      kind: 'error',
      content: props?.message || 'Unknown error',
    })];
  }

  return [];
}

// ─── Public API ─────────────────────────────────────────────────────────────

/** @type {Map<string, { startedAt: number, projectPath?: string }>} */
const activeOpencodeSessions = new Map();

function parseModel(modelStr) {
  if (!modelStr) return { providerID: 'ollama', modelID: 'gpt-oss:latest' };
  const slashIdx = modelStr.indexOf('/');
  if (slashIdx === -1) return { providerID: 'ollama', modelID: modelStr };
  return {
    providerID: modelStr.slice(0, slashIdx),
    modelID: modelStr.slice(slashIdx + 1),
  };
}

/**
 * Spawn an OpenCode prompt. Returns a Promise that resolves when the prompt
 * has been accepted; streaming continues via WebSocket.
 *
 * @param {string} command - User prompt
 * @param {object} options - { sessionId?, projectPath?, cwd?, model? }
 * @param {any} ws - Client WebSocket for streaming responses
 */
export async function spawnOpencode(command, options = {}, ws) {
  const { sessionId, projectPath, cwd, model } = options;

  let baseUrl;
  try {
    baseUrl = await ensureDaemon();
    await ensureSseSubscription();
  } catch (err) {
    ws?.send(createNormalizedMessage({
      id: generateMessageId('opencode'),
      sessionId: sessionId || '',
      timestamp: new Date().toISOString(),
      provider: 'opencode',
      kind: 'error',
      content: `OpenCode daemon start failed: ${err.message}`,
    }));
    throw err;
  }

  let targetSessionId = sessionId;

  if (!targetSessionId) {
    try {
      const directory = cwd || projectPath || process.cwd();
      const res = await fetch(`${baseUrl}/session`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: command.slice(0, 80), directory }),
      });
      if (!res.ok) throw new Error(`session create failed: HTTP ${res.status}`);
      const session = await res.json();
      targetSessionId = session.id;
    } catch (err) {
      ws?.send(createNormalizedMessage({
        id: generateMessageId('opencode'),
        sessionId: '',
        timestamp: new Date().toISOString(),
        provider: 'opencode',
        kind: 'error',
        content: `Session create failed: ${err.message}`,
      }));
      throw err;
    }
  }

  sessionRoutes.set(targetSessionId, { ws, partTypeById: new Map() });
  activeOpencodeSessions.set(targetSessionId, { startedAt: Date.now(), projectPath });

  ws?.send(createNormalizedMessage({
    id: generateMessageId('opencode'),
    sessionId: targetSessionId,
    timestamp: new Date().toISOString(),
    provider: 'opencode',
    kind: 'session_created',
    newSessionId: targetSessionId,
  }));

  try {
    const { providerID, modelID } = parseModel(model);
    const body = {
      model: { providerID, modelID },
      parts: [{ type: 'text', text: command }],
    };
    const res = await fetch(`${baseUrl}/session/${encodeURIComponent(targetSessionId)}/prompt_async`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok && res.status !== 204) {
      throw new Error(`prompt failed: HTTP ${res.status}`);
    }
  } catch (err) {
    sessionRoutes.delete(targetSessionId);
    activeOpencodeSessions.delete(targetSessionId);
    ws?.send(createNormalizedMessage({
      id: generateMessageId('opencode'),
      sessionId: targetSessionId,
      timestamp: new Date().toISOString(),
      provider: 'opencode',
      kind: 'error',
      content: `Prompt failed: ${err.message}`,
    }));
    try { notifyRunFailed({ provider: 'opencode', sessionId: targetSessionId, error: err.message }); } catch { /* noop */ }
    throw err;
  }

  return { sessionId: targetSessionId };
}

export async function abortOpencodeSession(sessionId) {
  if (!sessionId) return false;
  const baseUrl = getBaseUrl();
  try {
    const res = await fetch(`${baseUrl}/session/${encodeURIComponent(sessionId)}/abort`, { method: 'POST' });
    sessionRoutes.delete(sessionId);
    activeOpencodeSessions.delete(sessionId);
    try { notifyRunStopped({ provider: 'opencode', sessionId }); } catch { /* noop */ }
    return res.ok;
  } catch (e) {
    console.warn('[opencode] abort failed:', e.message);
    return false;
  }
}

export function isOpencodeSessionActive(sessionId) {
  return activeOpencodeSessions.has(sessionId);
}

export function getActiveOpencodeSessions() {
  return Array.from(activeOpencodeSessions.keys());
}

export async function getOpencodeProviders() {
  try {
    const baseUrl = await ensureDaemon();
    const res = await fetch(`${baseUrl}/provider`);
    if (!res.ok) return null;
    return await res.json();
  } catch (e) {
    console.warn('[opencode] provider list failed:', e.message);
    return null;
  }
}

export async function fetchOpencodeSessionMessages(sessionId) {
  try {
    const baseUrl = await ensureDaemon();
    const res = await fetch(`${baseUrl}/session/${encodeURIComponent(sessionId)}/message`);
    if (!res.ok) return [];
    return await res.json();
  } catch (e) {
    console.warn('[opencode] fetch messages failed:', e.message);
    return [];
  }
}

process.on('exit', () => {
  try { sseAbortController?.abort(); } catch { /* noop */ }
  try { daemonProcess?.kill('SIGTERM'); } catch { /* noop */ }
});
process.on('SIGINT', () => { try { daemonProcess?.kill('SIGTERM'); } catch { /* noop */ } });
process.on('SIGTERM', () => { try { daemonProcess?.kill('SIGTERM'); } catch { /* noop */ } });
