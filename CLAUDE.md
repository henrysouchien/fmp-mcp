NEVER run `git checkout -- <files>`, `git checkout .`, `git checkout HEAD`, `git restore .`, `git reset --hard`, `git clean -f`, or ANY command that discards uncommitted changes. NO EXCEPTIONS. Multiple sessions may be running in parallel. If Codex or any tool modifies unexpected files, TELL the user which files and ASK what to do — do NOT revert them.

# fmp-mcp — Synced Package Repo

**DO NOT edit code in this repo directly.**

This repo is a deployment artifact synced from the source of truth at `risk_module/fmp/`. All code changes must be made there first, then synced here using `risk_module/scripts/sync_fmp_mcp.sh`.

See the deploy checklist in the source repo: `risk_module/docs/DEPLOY_CHECKLIST.md`
