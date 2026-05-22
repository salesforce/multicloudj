---
layout: default
title: AI-Powered Development Setup
nav_order: 2
parent: Getting Started
---

# AI-Powered Development Setup

This guide walks you through installing and configuring the AI development toolchain for working with MultiCloudJ. You'll set up DevBar (the management layer), Claude Code (the AI coding assistant), and two plugins that supercharge your workflow: **ai-dev-workflow** and **superpowers**.

---

## Prerequisites

- macOS (DevBar is a macOS system tray application)
- Salesforce SSO credentials (for gateway authentication)
- Git configured with SSH access to GitHub

---

## Step 1: Install DevBar

DevBar is the central management tool that handles AI coding tool installation, LLM gateway routing, plugin marketplaces, and usage tracking.

### Install via Homebrew

```bash
brew install --cask devbar
```

### First-Time Setup

1. **Launch DevBar** — it appears as a system tray icon:

   ```bash
   devbar gui
   ```

2. **Authenticate with SSO:**

   ```bash
   devbar auth login
   ```

   This opens your browser for Salesforce SSO. For headless/SSH environments, use:

   ```bash
   devbar auth login --device
   ```

3. **Verify authentication:**

   ```bash
   devbar auth status
   ```

---

## Step 2: Install Claude Code

If Claude Code isn't already installed, DevBar can set it up with all required addons (gateway config, MCP adaptor, hooks, marketplace):

```bash
devbar tools install claude-code
```

If Claude Code is already installed externally, DevBar detects it and configures the addons automatically. Verify the setup:

```bash
devbar tools addon list claude-code
```

You should see all required addons showing `✓ ready`:
- **Awesome Context Marketplace** — plugin marketplace for extensions
- **Claude Code Hooks** — usage metrics tracking
- **Claude Model Bedrock Routing** — model selection routing
- **Gateway Config** — LLM gateway API key via SSO
- **MCP Adaptor** — corporate gateway routing
- **MCP Adaptor Cleanup** — legacy config repair

### Post-Install Init

Run the post-install setup to ensure everything is configured:

```bash
devbar tools init claude-code
```

---

## Step 3: Install the Superpowers Plugin

Superpowers is a complete software development methodology plugin. It provides skills for brainstorming, TDD, systematic debugging, plan writing, code review, and autonomous agent workflows.

### Install from the Official Marketplace

Inside a Claude Code session, run:

```
/plugin install superpowers@claude-plugins-official
```

### What You Get

Superpowers automatically activates relevant skills based on what you're doing:

| Skill | Triggers When |
|-------|--------------|
| **brainstorming** | You start building something new |
| **test-driven-development** | You implement any feature or bugfix |
| **systematic-debugging** | You encounter a bug or test failure |
| **writing-plans** | You have a multi-step task to plan |
| **subagent-driven-development** | Executing implementation plans with parallel agents |
| **requesting-code-review** | You complete a feature, before merging |
| **using-git-worktrees** | You need isolation for feature work |
| **finishing-a-development-branch** | Implementation is done, deciding how to integrate |

### Core Workflow

The Superpowers methodology follows this sequence:

1. **Brainstorm** — refine rough ideas through questions, explore alternatives
2. **Plan** — break work into bite-sized tasks with exact file paths and verification steps
3. **Implement (TDD)** — RED-GREEN-REFACTOR cycle enforced per task
4. **Review** — code review against the plan before finishing
5. **Finish** — merge, PR, or discard with worktree cleanup

---

## Step 4: Install the AI Dev Workflow Plugin

The ai-dev-workflow plugin adds 18 specialized agents that handle end-to-end feature development across 6 phases — from idea to merged PR — with human approval gates at every boundary.

### Install from the Salesforce Marketplace

Inside a Claude Code session, run:

```
/plugin install ai-dev-workflow@salesforce-native-ai-stack
```

Or via DevBar:

```bash
devbar marketplace install ai-dev-workflow
```

### Available Skills

| Skill | Command | What It Does |
|-------|---------|-------------|
| Spec-Driven Development | `/spec-driven-development` | Plan a feature: requirements → design → task breakdown |
| Autonomous Loop | `/autonomous-loop` | Hands-off task execution with stall detection |
| PR Review | `/pr-review` | Deep 5-phase structured review for large PRs |
| PR Summary | `/pr-summary` | Generate a PR description from current diff |
| PR Feedback | `/pr-feedback` | Collect, triage, and resolve PR reviewer comments |
| AGENTS.md | `/agents-md` | Create or audit AGENTS.md / CLAUDE.md project context |
| Doc Co-authoring | `/doc-coauthoring` | Co-author design docs, PRDs, RFCs |
| Docs Site Management | `/docs-site-management` | Maintain documentation site config and navigation |
| Agent Teams | `/agent-teams` | Multi-session coordination with specialized agents |

---

## Use Cases

### Use Case 1: Build a New Feature End-to-End

Let the team-lead agent orchestrate everything — planning, implementation, quality checks, and PR creation:

```
Build a new ListBuckets API for the blob service end-to-end
```

The team-lead will:
1. Verify project context (AGENTS.md/CLAUDE.md)
2. Create a feature branch
3. Generate requirements, design, and task breakdown
4. Implement tasks with atomic commits
5. Run quality checks (tests, refactoring, security)
6. Create and push the PR

### Use Case 2: Plan Before You Build

Use spec-driven development to produce a detailed plan without writing code:

```
/spec-driven-development

Plan the object-tagging feature for blob-gcp
```

This produces three artifacts in `.ai/specs/<feature>/`:
- `requirements.md` — EARS-format user stories and acceptance criteria
- `design.md` — architecture, data flows, component designs
- `tasks.md` — ordered checklist with acceptance criteria per task

### Use Case 3: Autonomous Execution

After your plan is approved, let the autonomous loop execute all tasks hands-off:

```
/autonomous-loop

Execute all tasks in .ai/specs/object-tagging/tasks.md
```

The executor picks up each task, implements it, verifies acceptance criteria, marks it complete, and commits — with session management and stall detection built in.

### Use Case 4: Review a Large PR

For PRs with 15+ changed files, use the structured 5-phase PR review:

```
/pr-review

Review PR #42
```

This dispatches code-reviewer sub-agents per file group and produces a comprehensive review covering bugs, security, conventions, and cross-cutting concerns.

### Use Case 5: Resolve PR Feedback

After reviewers leave comments on your PR:

```
/pr-feedback

Resolve the feedback on PR #42
```

The resolver collects all comments, deduplicates them, classifies each (fix / refactor / explain / defer), and acts on them.

### Use Case 6: Debug a Failing Test

When you hit a test failure, the systematic debugging skill activates automatically (via Superpowers):

```
Fix the failing test in AwsBlobStoreIT.testCopyObject
```

Superpowers enforces a 4-phase root cause process: reproduce → hypothesize → verify → fix. No guessing.

### Use Case 7: Write Design Docs

Co-author structured documents with iterative refinement:

```
/doc-coauthoring

Write a design doc for the new retry policy across all providers
```

The doc-coauthoring skill walks through Context Gathering, Refinement & Structure, and Reader Testing stages.

### Use Case 8: Quick Bug Fix (No Full Workflow Needed)

For simple fixes, skip the planning phases entirely. Just describe the bug:

```
The BucketClient.download() method throws NPE when the blob doesn't exist instead of returning BlobNotFoundException
```

The agent fixes it directly. Superpowers still activates TDD and verification, but no spec-driven planning overhead.

---

## Plugin Interaction

The two plugins complement each other:

| Concern | Superpowers | AI Dev Workflow |
|---------|-------------|-----------------|
| **Methodology** | TDD, debugging, brainstorming, code review | Spec-driven planning, approval gates |
| **Execution** | Single-session subagent dispatch | Multi-phase orchestration with 18 agents |
| **Scope** | Per-task discipline | End-to-end feature lifecycle |
| **Autonomy** | Plan + execute in current session | Autonomous loop with stall detection |

Both plugins activate automatically based on context. You don't need to choose — they work together. Superpowers handles the micro-discipline (TDD per task, systematic debugging), while ai-dev-workflow handles the macro-coordination (planning phases, quality gates, PR lifecycle).

---

## Verifying Your Setup

Run these commands to confirm everything is working:

```bash
# Check DevBar is running
devbar status

# Check Claude Code addons
devbar tools addon list claude-code

# Inside a Claude Code session, verify plugins:
/plugin list
```

You should see both `superpowers@claude-plugins-official` and `ai-dev-workflow@salesforce-native-ai-stack` listed.

---

## Troubleshooting

### DevBar auth expires

Re-authenticate:

```bash
devbar auth login
```

### Plugin not activating

Ensure the plugin is installed at user scope:

```
/plugin list
```

If missing, reinstall:

```
/plugin install superpowers@claude-plugins-official
/plugin install ai-dev-workflow@salesforce-native-ai-stack
```

### MCP Adaptor connection issues

Check adaptor status and re-authenticate if needed:

```bash
devbar tools addon list claude-code
```

If MCP Adaptor shows issues, run the device auth flow inside Claude Code:

```
/mcp-auth
```

### Marketplace not found

The Salesforce marketplace must be registered. DevBar's `Awesome Context Marketplace` addon handles this automatically. If it's missing:

```bash
devbar tools init claude-code
```

---

## Next Steps

- **Explore Guides**: Check out our [service-specific guides](/multicloudj/guides/) for blob, docstore, pubsub, and more
- **API Reference**: Browse the complete [Java API documentation](/multicloudj/api/java/latest/index.html)
- **Contributing**: Learn how to [contribute to MultiCloudJ](https://github.com/salesforce/multicloudj/blob/main/CONTRIBUTING.md)
