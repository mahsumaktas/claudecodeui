/**
 * OpenCode Provider Status
 *
 * Checks whether the opencode binary is installed and reachable.
 * OpenCode handles its own credentials (per-provider, via `opencode providers login`),
 * so "authenticated" here means: the user has at least one configured provider,
 * OR the daemon is already running and reachable.
 *
 * @module providers/opencode/status
 */

import { promises as fs, accessSync } from 'fs';
import path from 'path';
import os from 'os';

export function checkInstalled() {
  const candidates = [
    path.join(os.homedir(), '.opencode', 'bin', 'opencode'),
    '/usr/local/bin/opencode',
    '/opt/homebrew/bin/opencode',
  ];
  for (const p of candidates) {
    try {
      accessSync(p);
      return true;
    } catch { /* try next */ }
  }
  const pathDirs = (process.env.PATH || '').split(path.delimiter);
  for (const dir of pathDirs) {
    if (!dir) continue;
    try {
      accessSync(path.join(dir, 'opencode'));
      return true;
    } catch { /* try next */ }
  }
  return false;
}

export async function checkStatus() {
  const installed = checkInstalled();
  if (!installed) {
    return {
      installed: false,
      authenticated: false,
      email: null,
      error: 'opencode binary not found in PATH or ~/.opencode/bin',
    };
  }

  const port = Number(process.env.OPENCODE_PORT || 4096);
  const baseUrl = `http://127.0.0.1:${port}`;
  let daemonReachable = false;
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 800);
    const res = await fetch(`${baseUrl}/global/health`, { signal: controller.signal });
    clearTimeout(timeout);
    daemonReachable = res.ok;
  } catch { /* daemon not running yet is fine */ }

  let hasConfiguredProvider = false;
  try {
    const cfgPath = path.join(os.homedir(), '.config', 'opencode', 'opencode.json');
    const raw = await fs.readFile(cfgPath, 'utf8');
    const cfg = JSON.parse(raw);
    hasConfiguredProvider = Boolean(cfg?.provider && Object.keys(cfg.provider).length > 0);
  } catch { /* no config is fine — opencode ships with defaults */ }

  return {
    installed: true,
    authenticated: hasConfiguredProvider || daemonReachable,
    email: hasConfiguredProvider ? 'Configured via ~/.config/opencode/opencode.json' : null,
    method: 'config_file',
    error: null,
  };
}
