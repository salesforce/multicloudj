# AGENTS.md

This repository supports both Claude Code and Codex. `CLAUDE.md` remains the
Claude Code entry point and the shared source of repository architecture,
commands, testing conventions, and development rules.

## Shared repository guidance

- Before doing repository work, read `CLAUDE.md` completely and follow its
  project rules. Treat references to Claude Code as applying to the active
  coding agent unless this file provides a Codex-specific override.
- Do not delete, rename, or replace `CLAUDE.md` or `.claude/`; they must remain
  usable by Claude Code.
- If shared repository guidance changes, keep `CLAUDE.md`, this file, and the
  corresponding Claude/Codex skills aligned.

## Codex skills

Codex-compatible repository skills live under `.agents/skills/`:

- `$multicloudj-feature-dev` is required before implementing any new feature,
  operation, service module, cross-provider behavior, or conformance test.
- `$docs-guides` handles documentation-site guide creation and updates.

Claude Code continues to use the equivalent skills under `.claude/skills/`.
Do not remove either skill tree. When changing a shared workflow, update both
copies in the same change.

## Codex-specific adaptations

- Use `$skill-name` or the Codex skill selector instead of Claude slash-command
  syntax.
- Translate Claude-specific tool names into their Codex equivalents while
  preserving the workflow's intent.
- The instruction in `CLAUDE.md` to consult `~/.claude/config.md` is only for
  Claude Code. Codex should use the configured Git and GitHub authentication
  available in its environment and follow its approval and sandbox rules.
- If this file conflicts with `CLAUDE.md` only on Codex behavior, this file
  takes precedence for Codex.
