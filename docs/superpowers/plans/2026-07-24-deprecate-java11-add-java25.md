# Deprecate Java 11, Add Java 25 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hard-deprecate Java 11 (raise bytecode target to 17) and add Java 25 to the CI matrix, updating build config, workflows, and docs.

**Architecture:** The root `pom.xml` drives the compiler target and enforced minimum Java version via two properties (`compileSource`, `releaseTarget`); bumping them cascades everywhere. CI uses one workflow file per Java version plus a separate codecov/verify job. Docs (README badges, requirements, CLAUDE.md) mirror the supported versions.

**Tech Stack:** Maven 3.8+, maven-compiler-plugin (`<release>`), GitHub Actions (`actions/setup-java@v3`, Temurin).

## Global Constraints

- Bytecode target = **17** (`compileSource` and `releaseTarget` both `17`).
- CI per-version matrix = **17, 21, 25**.
- Codecov/verify job runs on **JDK 17**.
- No source code changes; no dependency version bumps.
- Consumers must run **Java 17+**.

---

### Task 1: Bump bytecode target and enforced minimum to 17

**Files:**
- Modify: `pom.xml:51-52`

**Interfaces:**
- Consumes: nothing.
- Produces: `compileSource=17`, `releaseTarget=17`. `java.min.version` (`${compileSource}`) and maven-compiler-plugin `<release>${releaseTarget}</release>` follow automatically.

- [ ] **Step 1: Edit the two properties in `pom.xml`**

Change lines 51-52 from:

```xml
        <compileSource>11</compileSource>
        <releaseTarget>11</releaseTarget>
```

to:

```xml
        <compileSource>17</compileSource>
        <releaseTarget>17</releaseTarget>
```

- [ ] **Step 2: Verify the project builds under the new target**

Run: `mvn clean install -DskipTests`
Expected: `BUILD SUCCESS`. (Confirms the Java 17 target compiles all modules.)

- [ ] **Step 3: Commit**

```bash
git add pom.xml
git commit -m "build: raise bytecode target to Java 17 (deprecate Java 11)"
```

---

### Task 2: Replace Java 11 CI job with Java 25

**Files:**
- Delete: `.github/workflows/java11-build.yml`
- Create: `.github/workflows/java25-build.yml`

**Interfaces:**
- Consumes: nothing.
- Produces: a `java25-build.yml` workflow named "Java 25 Build" (README Task 4 references this file for its badge).

- [ ] **Step 1: Delete the Java 11 workflow**

Run: `git rm .github/workflows/java11-build.yml`

- [ ] **Step 2: Create `.github/workflows/java25-build.yml`**

```yaml
name: Java 25 Build

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]
  workflow_dispatch:

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v4
      
    - name: Set up JDK 25
      uses: actions/setup-java@v3
      with:
        java-version: '25'
        distribution: 'temurin'
        cache: maven
        
    - name: Build with Maven
      run: mvn -B clean test
      
    - name: Upload Test Results
      if: always()
      uses: actions/upload-artifact@v4
      with:
        name: test-results
        path: target/surefire-reports
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/java25-build.yml
git commit -m "ci: replace Java 11 build job with Java 25"
```

---

### Task 3: Move codecov/verify job from JDK 11 to JDK 17

**Files:**
- Modify: `.github/workflows/build-test-codecov.yml:18-23`

**Interfaces:**
- Consumes: nothing.
- Produces: coverage/verify job running on JDK 17.

- [ ] **Step 1: Edit the JDK setup step**

Change lines 18-23 from:

```yaml
      - name: Set up JDK 11
        uses: actions/setup-java@v3
        with:
          java-version: "11"
          distribution: "temurin"
          cache: maven
```

to:

```yaml
      - name: Set up JDK 17
        uses: actions/setup-java@v3
        with:
          java-version: "17"
          distribution: "temurin"
          cache: maven
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/build-test-codecov.yml
git commit -m "ci: run codecov/verify job on JDK 17"
```

---

### Task 4: Update README badges and requirements

**Files:**
- Modify: `README.md:3` (badges), `README.md:32` (requirements)

**Interfaces:**
- Consumes: `java25-build.yml` from Task 2 (badge target).
- Produces: nothing downstream.

- [ ] **Step 1: Swap the Java 11 badge for a Java 25 badge**

Change line 3 from:

```markdown
[![Java 11 Build](https://github.com/salesforce/multicloudj/actions/workflows/java11-build.yml/badge.svg)](https://github.com/salesforce/multicloudj/actions/workflows/java11-build.yml)
```

to:

```markdown
[![Java 25 Build](https://github.com/salesforce/multicloudj/actions/workflows/java25-build.yml/badge.svg)](https://github.com/salesforce/multicloudj/actions/workflows/java25-build.yml)
```

Then move it below the Java 21 badge (line 5) so badges read 17, 21, 25 in order. Final badge order:

```markdown
[![Java 17 Build](https://github.com/salesforce/multicloudj/actions/workflows/java17-build.yml/badge.svg)](https://github.com/salesforce/multicloudj/actions/workflows/java17-build.yml)
[![Java 21 Build](https://github.com/salesforce/multicloudj/actions/workflows/java21-build.yml/badge.svg)](https://github.com/salesforce/multicloudj/actions/workflows/java21-build.yml)
[![Java 25 Build](https://github.com/salesforce/multicloudj/actions/workflows/java25-build.yml/badge.svg)](https://github.com/salesforce/multicloudj/actions/workflows/java25-build.yml)
```

- [ ] **Step 2: Update the requirements line**

Change line 32 from:

```markdown
- Java 11 or higher
```

to:

```markdown
- Java 17 or higher
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: update README for Java 17 minimum and Java 25 build badge"
```

---

### Task 5: Update CLAUDE.md build configuration note

**Files:**
- Modify: `CLAUDE.md:179`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing downstream.

- [ ] **Step 1: Update the Java version line**

Change line 179 from:

```markdown
- **Java Version**: Requires Java 11+, targets Java 11 bytecode
```

to:

```markdown
- **Java Version**: Requires Java 17+, targets Java 17 bytecode
```

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md to Java 17 requirement"
```

---

## Self-Review

**Spec coverage:**
- Bytecode target 11→17 → Task 1. ✓
- Drop Java 11 CI, add Java 25 → Task 2. ✓
- Codecov job → JDK 17 → Task 3. ✓
- README badge + requirements → Task 4. ✓
- CLAUDE.md → Task 5. ✓

**Placeholder scan:** None — every step has exact content and commands.

**Type/name consistency:** Workflow filename `java25-build.yml` is consistent between Task 2 (create) and Task 4 (badge reference). Property names `compileSource`/`releaseTarget` match the spec.

**Risk carried from spec:** Task 2's Java 25 job depends on Temurin 25 being available on the runner; if the job fails at CI time, that is the known risk, not a plan defect.
