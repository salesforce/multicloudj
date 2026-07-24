# Deprecate Java 11, Add Java 25 — Design

**Date:** 2026-07-24
**Status:** Approved
**Type:** Build / CI / docs change (no source code changes)

## Goal

Perform a **hard deprecation** of Java 11 in the multicloudj SDK and add Java 25 to
the CI test matrix. "Hard deprecation" means the compiled bytecode target is raised,
so consumers of the SDK must now run on Java 17 or higher.

## Background

Current state at time of writing:

- `pom.xml` (root parent) sets `compileSource=11` and `releaseTarget=11`. The property
  `java.min.version` is derived as `${compileSource}`, and the maven-compiler-plugin
  uses `<release>${releaseTarget}</release>`. So both the compiler target and the
  enforced minimum Java version cascade from these two properties.
- CI has per-version build workflows: `java11-build.yml`, `java17-build.yml`,
  `java21-build.yml`.
- `build-test-codecov.yml` runs `mvn -B clean verify` plus JaCoCo coverage on **JDK 11**.
- `README.md` shows Java 11 / 17 / 21 build badges and states "Java 11 or higher".
- `CLAUDE.md` states "Requires Java 11+, targets Java 11 bytecode".

## Decisions

- **Bytecode target:** 11 → **17** (hard deprecation; breaking change for consumers).
- **Codecov / verify JDK:** 11 → **17**.
- **CI test matrix:** **17, 21, 25** (drop 11, add 25).

## Changes

### 1. `pom.xml`
- `compileSource`: `11` → `17`
- `releaseTarget`: `11` → `17`

`java.min.version` follows automatically via `${compileSource}`, and the
maven-compiler-plugin `<release>` follows via `${releaseTarget}`. No other pom edits
required.

### 2. CI test matrix (`.github/workflows/`)
- **Delete** `java11-build.yml`.
- **Add** `java25-build.yml` — a mirror of `java21-build.yml` with `java-version: '25'`
  and the job/name labels updated to 25.
- Keep `java17-build.yml` and `java21-build.yml` unchanged.

Resulting per-version matrix: **17, 21, 25**.

### 3. `build-test-codecov.yml`
- Move the `verify` + coverage job from JDK 11 → **JDK 17** (update the "Set up JDK"
  step name and `java-version: "17"`).

### 4. `README.md`
- Remove the Java 11 build badge.
- Add a Java 25 build badge (pointing to `java25-build.yml`).
- Change "Java 11 or higher" → "Java 17 or higher".

### 5. `CLAUDE.md`
- "Requires Java 11+, targets Java 11 bytecode" → "Requires Java 17+, targets Java 17
  bytecode".

## Out of Scope

- No source code changes. The bump to a Java 17 target does not require adopting any
  Java 12–17 language features; existing Java 11 source compiles unchanged under a 17
  target.
- No dependency version bumps.

## Risks

- **Java 25 availability in CI:** The `java25-build.yml` job depends on
  `actions/setup-java@v3` + Temurin publishing a `25` distribution available on the
  GitHub runner. If Temurin 25 is not yet published, this job will fail. The workflow
  mirrors the existing pattern regardless; pinning/guarding can be revisited if the job
  fails.

## Verification

- Run `mvn clean install -DskipTests` locally to confirm the project builds under the
  new Java 17 target.
