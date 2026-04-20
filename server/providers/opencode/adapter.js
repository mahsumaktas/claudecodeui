/**
 * OpenCode provider adapter.
 *
 * Normalizes OpenCode session history (from the daemon's
 * `GET /session/:id/message` response) into NormalizedMessage format.
 * Live SSE events are normalized inline in server/opencode-cli.js because
 * they require per-session part-type state.
 *
 * Calling sites:
 *   - server/providers/registry.js imports `opencodeAdapter` and registers it.
 *   - Analogous to server/providers/codex/adapter.js.
 *
 * Data read: HTTP GET http://127.0.0.1:4096/session/:id/message
 * Response shape (verified against a live daemon):
 *   [{ info: {role, id, sessionID, time:{created, completed}, model, finish, tokens},
 *      parts: [{type:'text'|'reasoning'|'step-start'|'step-finish'|'tool-call'|'tool-result', ...}] }]
 *
 * @module adapters/opencode
 */

import { createNormalizedMessage, generateMessageId } from '../types.js';

const PROVIDER = 'opencode';
const DEFAULT_PORT = Number(process.env.OPENCODE_PORT || 4096);

function baseUrl() {
  return `http://127.0.0.1:${DEFAULT_PORT}`;
}

function normalizeHistoryMessage(raw, sessionId) {
  const info = raw?.info;
  const parts = Array.isArray(raw?.parts) ? raw.parts : [];
  if (!info?.role) return [];

  const baseTs = info.time?.created
    ? new Date(info.time.created).toISOString()
    : new Date().toISOString();
  const msgId = info.id || generateMessageId('opencode');

  if (info.role === 'user') {
    const text = parts
      .filter((p) => p?.type === 'text')
      .map((p) => p.text || '')
      .join('');
    if (!text.trim()) return [];
    return [createNormalizedMessage({
      id: msgId,
      sessionId,
      timestamp: baseTs,
      provider: PROVIDER,
      kind: 'text',
      role: 'user',
      content: text,
    })];
  }

  const out = [];
  for (const part of parts) {
    const partId = part?.id || generateMessageId('opencode');
    const partTs = part?.time?.start
      ? new Date(part.time.start).toISOString()
      : baseTs;

    switch (part?.type) {
      case 'text':
        if ((part.text || '').trim()) {
          out.push(createNormalizedMessage({
            id: partId,
            sessionId,
            timestamp: partTs,
            provider: PROVIDER,
            kind: 'text',
            role: 'assistant',
            content: part.text,
          }));
        }
        break;
      case 'reasoning':
        if ((part.text || '').trim()) {
          out.push(createNormalizedMessage({
            id: partId,
            sessionId,
            timestamp: partTs,
            provider: PROVIDER,
            kind: 'thinking',
            content: part.text,
          }));
        }
        break;
      case 'tool-call':
        out.push(createNormalizedMessage({
          id: partId,
          sessionId,
          timestamp: partTs,
          provider: PROVIDER,
          kind: 'tool_use',
          toolName: part.toolName || part.tool || 'Unknown',
          toolInput: part.input || part.arguments || {},
          toolId: part.id,
        }));
        break;
      case 'tool-result':
        out.push(createNormalizedMessage({
          id: generateMessageId('opencode'),
          sessionId,
          timestamp: partTs,
          provider: PROVIDER,
          kind: 'tool_result',
          toolId: part.toolCallId || part.id,
          content: typeof part.output === 'string' ? part.output : JSON.stringify(part.output || ''),
          isError: Boolean(part.isError),
        }));
        break;
      case 'step-start':
      case 'step-finish':
        break;
      default:
        break;
    }
  }

  const toolResultByCallId = new Map();
  for (const m of out) {
    if (m.kind === 'tool_result' && m.toolId) toolResultByCallId.set(m.toolId, m);
  }
  for (const m of out) {
    if (m.kind === 'tool_use' && m.toolId && toolResultByCallId.has(m.toolId)) {
      const tr = toolResultByCallId.get(m.toolId);
      m.toolResult = { content: tr.content, isError: tr.isError };
    }
  }

  return out;
}

export function normalizeMessage(raw, sessionId) {
  if (raw?.info && Array.isArray(raw?.parts)) {
    return normalizeHistoryMessage(raw, sessionId);
  }
  return [];
}

export const opencodeAdapter = {
  normalizeMessage,
  async fetchHistory(sessionId, opts = {}) {
    const { limit = null, offset = 0 } = opts;
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 5000);
      const res = await fetch(
        `${baseUrl()}/session/${encodeURIComponent(sessionId)}/message`,
        { signal: controller.signal },
      );
      clearTimeout(timeout);
      if (!res.ok) {
        return { messages: [], total: 0, hasMore: false, offset: 0, limit: null };
      }
      const raw = await res.json();
      const rawList = Array.isArray(raw) ? raw : [];

      const normalized = [];
      const totalTokens = { input: 0, output: 0, reasoning: 0 };
      for (const entry of rawList) {
        normalized.push(...normalizeHistoryMessage(entry, sessionId));
        const t = entry?.info?.tokens;
        if (t) {
          totalTokens.input += t.input || 0;
          totalTokens.output += t.output || 0;
          totalTokens.reasoning += t.reasoning || 0;
        }
      }

      const total = normalized.length;
      let paged = normalized;
      if (limit !== null && Number.isFinite(limit)) {
        paged = normalized.slice(offset, offset + limit);
      }

      return {
        messages: paged,
        total,
        hasMore: limit !== null ? offset + (limit || 0) < total : false,
        offset,
        limit,
        tokenUsage: totalTokens,
      };
    } catch (err) {
      console.warn('[OpencodeAdapter] fetchHistory failed:', err?.message || err);
      return { messages: [], total: 0, hasMore: false, offset: 0, limit: null };
    }
  },
};
