---
title: OpenCode plugin cache corruption (opencode-mem self-update)
tags: [opencode, plugins, troubleshooting, windows, tooling]
sourceRefs:
  - C:\Users\David\.local\share\opencode\log\opencode.log
  - C:\Users\David\.cache\opencode\npm\opencode-mem@latest\1793239141963\node_modules\opencode-mem\dist\services\auto-update.js
  - C:\Users\David\.cache\opencode\npm\opencode-mem@latest\1793239141963\node_modules\opencode-mem\dist\index.js
  - C:\Users\David\.cache\opencode\npm\opencode-mem@latest\1793239141963\node_modules\opencode-mem\dist\services\turso\engine-migrator.js
  - C:\Users\David\.config\opencode\opencode.json
  - C:\Users\David\.config\opencode\opencode-mem.jsonc
lastReviewed: 2026-11-01
---

# OpenCode plugin cache corruption (opencode-mem self-update)

Diagnosis of the `opencode-mem` "Plugin failed to load" error (`ref=err_c34376a8`) seen in this
project. The project codebase is not involved — this is a host-level OpenCode/plugin-cache fault.

## Symptom

```
Server plugin error
Plugin: opencode-mem
Status: failed
Runtime: server
Error: Plugin failed to load
Reference: err_c34376a8
```

The matching entry in `~/.local/share/opencode/log/opencode.log`:

```
level=WARN message="failed to load plugin" target=opencode-mem ref=err_c34376a8
cause="Cause([Die(ResolveMessage: Cannot find package '@libsql/client' imported from
  C:\Users\David\.cache\opencode\npm\opencode-mem@latest\1793239141963\node_modules\opencode-mem\dist\services\turso\engine-migrator.js)])"
```

The same cause was logged three times under three different refs (`err_b22003cc` at 16:48:51Z,
`err_52ba3aa9` at 16:48:54Z, `err_c34376a8` at 16:48:54Z) — the ref is per-event, the cause is
identical.

## Root cause

`opencode-mem` deletes its own install directory when it thinks an update is available, and on
Windows that delete fails halfway.

1. Global `opencode.json` lists `"opencode-mem"` **unpinned**, so OpenCode installs it into the
   wrapper `~/.cache/opencode/npm/opencode-mem@latest/<epoch-ms>/node_modules/opencode-mem`.
2. `opencode-mem.jsonc` sets `"autoUpdate": true` (also the compiled default in `config.js`).
3. `OpenCodeMemPlugin` calls `startAutoUpdate()` on **every** plugin init (`dist/index.js:274`).
   It fetches `https://registry.npmjs.org/opencode-mem/latest`; installed was `2.36.1`, registry
   latest `2.39.0`, so `isVersionNewer()` is true.
4. `updateRemoveDir()` walks up from the package dir and returns the wrapper dir
   (`.../opencode-mem@latest`) because the spec `latest` satisfies `isAutoUpdatableSpec()`. It then
   runs `rm(wrapper, { recursive: true, force: true })`.
5. Windows refuses to delete files the running OpenCode process has loaded/mapped (the plugin's own
   ESM modules, `.node` binaries). Node's `fs.rm` `force:true` suppresses only `ENOENT`, not
   `EBUSY`/`EPERM`, so the recursive delete **aborts partway**. The failure is swallowed:
   `{ updated: false, error: "remove_failed" }` — no log line, no toast.

Result: a half-deleted `node_modules`. Measured on the live install: **84 of 394** lockfile packages
were gone, including every non-platform casualty — `@libsql/client`, `@libsql/core`,
`@libsql/hrana-client`, `@libsql/isomorphic-ws`, `@huggingface/transformers`,
`@huggingface/jinja`, `@huggingface/tokenizers`, the `@aws-sdk/*` set, `@img/colour`.
`node_modules/.package-lock.json` and `node_modules/.bin` were also missing.

The already-imported module graph kept working in that process (the plugin still logged at
16:20:02Z), so the damage only surfaced on the **next** server start at 16:48:51Z, when the static
ESM import `import { createClient } from "@libsql/client"` in `turso/engine-migrator.js` (reached via
`turso/legacy-migrator.js`) could no longer resolve.

### Timeline (local time, UTC+1)

| Time | Event |
| --- | --- |
| 29-Oct 02:59 | `opencode-mem@2.36.1` installed into cache dir `1793239141963` |
| 01-Nov 17:20:00.825 | plugin init; auto-update check starts |
| 01-Nov 17:20:01 | `rm -rf` on the wrapper begins; `@libsql/*`, `@huggingface/*` etc. deleted, then EBUSY |
| 01-Nov 17:20:01.138 | same deletion surfaces as `Failed to initialize embedding model: Cannot resolve @huggingface/transformers` |
| 01-Nov 17:48:51 | fresh server process → `@libsql/client` unresolvable → `err_c34376a8` |

The `@huggingface/transformers` warning was **not** a separate bug; it was the first visible
symptom of the same deletion.

## Fix

Repair (required — pinning alone also triggers a fresh install, see below):

1. Quit OpenCode so no plugin files are locked.
2. Delete the wrapper: `C:\Users\David\.cache\opencode\npm\opencode-mem@latest`.
3. Restart OpenCode; it reinstalls the plugin.

Alternative in-place repair (verified with `npm install --dry-run`, which planned to re-add exactly
the 42 missing packages incl. `@libsql/client` and `@huggingface/transformers`):

```
cd %USERPROFILE%\.cache\opencode\npm\opencode-mem@latest\<epoch-ms>
npm install --no-audit --no-fund
```

Prevent recurrence (pick one, ideally both):

- **Pin the version** in `~/.config/opencode/opencode.json`:
  `"opencode-mem@2.39.0"` instead of `"opencode-mem"`. A pinned spec makes
  `isAutoUpdatableSpec()` false, so the plugin never self-deletes; OpenCode's own installer owns
  updates. This also lands in a new wrapper dir, which repairs the broken tree.
- **Disable the self-updater**: `"autoUpdate": false` in `opencode-mem.jsonc`.

## Related observations

- Windows file locking is a recurring theme here: the same session logged
  `EBUSY: resource busy or locked, rename ...` for `npm install opencode-rag-plugin@2.12.2`
  (16:43Z, retried at 16:44Z). Repair plugin trees with OpenCode closed.
- `~/.config/opencode/package.json` pins `@opencode-ai/plugin@1.18.30` while `opencode-mem`
  declares `^1.19.0`; this resolves inside the plugin's own tree and is **not** the cause.
- The quirk-memory backend (`add_quirk`) was unavailable during this investigation — its embedding
  endpoint returns `400 INVALID_ARGUMENT: Unknown name "input_type"`, i.e. the OpenAI-style
  `input_type` parameter is being sent to a Google embeddings endpoint that does not accept it.

See also [[wiki/index]].
