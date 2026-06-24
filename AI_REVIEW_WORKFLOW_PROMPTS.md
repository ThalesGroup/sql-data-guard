

## PROMPT 1 — Analyze the project & create/update the Memory Bank (Cline format)

```
ROLE: Senior software architect doing brownfield analysis.
GOAL: Produce a source-verified Cline-format Memory Bank so any agent can rebuild context fast.
INPUTS: The repository. Optional: if a skill/workflow "architecture-discovery" or
  "technical-documentation-workflow" exists (.agents/skills/ or .clinerules/), use it.
METHOD:
  1. Map repo: READMEs, layout, entry points, public APIs/CLIs, config, build/deploy, deps, tests.
  2. Read core source end-to-end; trace the main execution flow(s); mark security-critical
     components, trust boundaries, design patterns.
  3. VERIFY empirically — don't trust docs/comments: run the test suite, grep to confirm
     features exist, record exact tool/library versions, note advertised-vs-actual gaps.
  4. Write ./memory-bank/ with six files: projectbrief.md, productContext.md, systemPatterns.md,
     techContext.md, activeContext.md, progress.md (purpose / flows / decisions / known issues).
CONSTRAINTS: Prefer grep/glob over full reads; cite file:line, don't paste code; for broad
  exploration dispatch parallel read-only sub-agents returning conclusions only. Distinguish
  FACTS (verified) from ASSUMPTIONS. No code changes. No commits.
OUTPUT: the six files + a <=15-line chat summary (architecture + top 3-5 risks/gaps).
STOP WHEN: the six files exist and the summary is posted.
```

---

## PROMPT 2 — Find correctness & security issues

```
ROLE: Security researcher + secure-code reviewer + penetration tester.
GOAL: A prioritized, de-duplicated backlog of real correctness & security issues.
INPUTS: ./memory-bank/ if present (else skim code). Optional skills: "sql-injection-researcher",
  "refactoring-advisor", or a security-persona/"Security-Rule" file — use if present.
METHOD:
  1. Read code before judging; trace each security decision end-to-end; reason over the
     AST/parse tree, not string/regex matching.
  2. SECURITY: injection bypasses (UNION/EXCEPT/INTERSECT, sub-queries, stacked/multi-statement,
     comment & encoding evasion, parser/dialect confusion), authz bypasses (row/column/allow-list),
     untrusted input reaching a sink unsanitized.
  3. CORRECTNESS/ROBUSTNESS: validation that crashes the caller (uncaught exceptions/500s),
     wrong comparisons (string vs numeric), non-deterministic output, dead code, import-time
     failures, build/packaging defects.
  4. Per issue: stable ID, Title, Severity, Component(file:line), Description, Root Cause,
     concrete repro/attack example, Risk/Impact, Recommended Fix direction.
  5. De-duplicate; order by priority (crashes/security first); tag "code-confirmed" vs "suspected".
CONSTRAINTS: Targeted search over full reads; cite file:line; don't fabricate. No code changes yet.
OUTPUT: write the backlog table to ./ISSUES_AND_FINDINGS.md (do NOT commit) + a <=15-line summary
  (counts by severity, the top fixes).
STOP WHEN: the backlog file is written and summarized.
```

---

## PROMPT 3 — Replicate findings against the running local app (real vs false positive)

```
ROLE: Verification engineer.
GOAL: Empirically classify each finding as VALIDATED / FALSE POSITIVE / NEEDS-INVESTIGATION.
INPUTS: ./ISSUES_AND_FINDINGS.md, ./memory-bank/.
CRITICAL: run LOCAL source, never a published/installed package
  (Python: `pip install -e .` or `PYTHONPATH=src ...`; launch the API/CLI on local code and
  confirm a local-only marker/fix is observable).
METHOD (per finding):
  1. Build the MINIMAL reproducing input (API payload, function call, or test).
  2. Run it; capture the EXACT actual output (status, returned object, error).
  3. Compare to expected; classify with the captured evidence.
  4. Be precise about version-/dialect-specific behavior; if a bug is only blocked incidentally
     (not by an explicit control), say so. Don't over-claim.
CONSTRAINTS: Reference payloads by ID; don't paste large outputs verbatim — trim to the deciding
  lines. No code changes.
OUTPUT: write ./REPLICATION_REPORT.md (do NOT commit): per finding — payload, observed output,
  verdict, one-line root-cause confirmation; plus validated/false-positive counts in chat.
STOP WHEN: every finding has a verdict with evidence.
```

---

## PROMPT 4 — Propose a fix plan; implement fixes + unit tests after approval

```
ROLE: Senior engineer (security-aware), TDD-minded.
GOAL: A reviewable fix plan, then — only after approval — the fixes plus regression tests.
INPUTS: VALIDATED findings in ./REPLICATION_REPORT.md. Optional skills: "security-test-generator",
  "refactoring-advisor".
PHASE A — PLAN (no code changes): per issue — fix approach, exact files/functions, backward-compat
  & risk notes, the regression test(s) to add; group + order; flag any test files whose
  expectations will change. Then STOP and wait for approval.
PHASE B — IMPLEMENT (only after approval):
  1. Read the affected tests BEFORE editing so you don't silently break expectations.
  2. Incremental, backward-compatible changes; AST-based over regex; keep the public contract stable.
  3. Every fix gets a regression test (positive + negative) that fails before / passes after.
  4. After each batch run the FULL suite; fix regressions before continuing.
  5. Per fix follow: Finding -> Root Cause -> Fix -> Impact -> Tests -> Docs (update
     ./memory-bank/progress.md).
CONSTRAINTS: Minimal diffs; cite file:line in the plan, don't paste whole files. Don't commit.
OUTPUT: Phase A = the plan (chat). Phase B = diffs + new/updated test file + a green test run summary.
STOP WHEN: Phase A awaits approval; Phase B ends on a green suite.
```

---

## PROMPT 5 — Re-validate the fixes by building & running the app

```
ROLE: QA / release validator.
GOAL: Prove the fixes work end-to-end and nothing regressed.
INPUTS: ./REPLICATION_REPORT.md, the test suite, the local app.
METHOD:
  1. Run the FULL suite on local code (e.g. `PYTHONPATH=src python -m pytest <tests>
     --ignore=<live-network-tests>`); report pass/skip/fail counts faithfully.
  2. Re-launch the app on local code (as Stage 3) and re-run the EXACT replication payloads;
     build a before->after table proving each VALIDATED issue is fixed AND legit inputs still
     work (no new false positives).
  3. Spot-check integration surfaces (REST/CLI/plugin) for the headline fixes.
CONSTRAINTS: Report real numbers; never paper over a failure — stop and show the output.
OUTPUT: test summary + before/after verification table (chat). No commits.
STOP WHEN: suite is green and the before/after table is complete.
```

---

## PROMPT 6 — Commit, push, and raise the MR/PR

```
ROLE: Release engineer.
GOAL: Ship the work on a branch with a complete PR/MR.
INPUTS: the working tree, Stage-5 evidence, ./REPLICATION_REPORT.md.
METHOD:
  1. Never commit to the default branch — create `fix/<short-topic>`.
  2. Stage ONLY source + test changes (list paths explicitly; exclude *.md and memory-bank/
     unless I say otherwise). Show `git status` so I can confirm nothing unintended is staged.
  3. Commit: one-line summary + body grouping every issue fixed (by ID) with its one-line fix +
     the test result + any repo-required trailer/sign-off.
  4. Push + set upstream. Prefer `gh`/`glab` if available, else `git push`; if auth needs an
     interactive prompt you can't complete, STOP and hand me the exact command.
  5. Open the PR/MR with: Summary, Issues-fixed tables by category, mapping to analysis docs,
     the Stage-5 before/after evidence, Tests, reviewer notes, out-of-scope items. Return the URL.
CONSTRAINTS: If you obtain a token (e.g. `git credential fill`) NEVER print it. Code + tests only.
OUTPUT: branch name, commit hash, PR/MR URL.
STOP WHEN: the PR/MR is open and its URL is returned.
```

---

## Will the existing skills/workflows help?

Yes — as **optional accelerators**. The prompts already embed the method, so these are speed-ups, not requirements.

| Stage | Skills / workflows that help | Reusability |
|------|------------------------------|-------------|
| 1 Analyze + Memory Bank | `architecture-discovery`, `technical-documentation-workflow`, `documentation-engineer` | `technical-documentation-workflow` is generic; others lightly project-tuned |
| 2 Find issues / security | `sql-injection-researcher`, `refactoring-advisor`, `Security-Rule.md` (persona + Finding→Root Cause→Fix→Impact→Tests→Docs deliverable shape) | `refactoring-advisor` generic; injection one is SQL-specific |
| 3 Replicate locally | `benchmark-suite-builder` (attack-corpus harness) | project-tuned |
| 4 Plan + implement + tests | `security-test-generator`, `refactoring-advisor`, `Security-Rule.md` | test-gen SQL-tuned; advisor generic |
| 5 Re-validate | `benchmark-suite-builder`, `security-test-generator` | as above |
| 6 Commit / push / MR | none (handled by the prompt) | n/a |

**How they're invoked:** Cline auto-loads `.clinerules/` and can run `.agents/skills/*`; Claude Code invokes skills via its Skill tool / subagents. Either way the prompts say *"if such a skill exists, use it; otherwise follow the steps below,"* so they degrade gracefully.

**Housekeeping (worth a separate cleanup):**
- `.agents/skills/sql-injection-reseacher/` is a **misspelled duplicate** of `sql-injection-researcher/` — delete it.
- The correctly-spelled `sql-injection-researcher/SKILL.md` has trailing junk on its last line (`-sta-console-nakul.`) — trim it.

## Will these prompts work on a clean project (no md / clean branch)?

**Yes**, by design — with three things to know:
1. **Self-contained.** Stage 1 *creates* the Memory Bank from scratch (no pre-existing `.md` needed); Stages 2–3 generate their own `ISSUES_AND_FINDINGS.md` / `REPLICATION_REPORT.md`. The optional-skill lines are no-ops when `.agents/.clinerules` are absent. To get the accelerators on a new project, copy the `.agents/` and `.clinerules/` folders across.
2. **Stages 3 & 5 need a runnable app.** Dependencies must be installed, and the prompts force running **local code, not the published package** (the `PYTHONPATH=src` / `pip install -e .` gotcha — otherwise you'll test the released version and miss your own fixes). For non-Python stacks, replace the run/test commands.
3. **Clean branch / no-md preference honored.** Stage 6 commits **code + tests only** and branches off the default branch; the Memory Bank and analysis artifacts stay uncommitted (add them to `.git/info/exclude` if you want them ignored locally).

## Appendix — stage → artifact map
| Stage | Reads | Writes |
|------|-------|--------|
| 1 | repo | `memory-bank/` (6 files) |
| 2 | `memory-bank/` | `ISSUES_AND_FINDINGS.md` |
| 3 | `ISSUES_AND_FINDINGS.md` | `REPLICATION_REPORT.md` |
| 4 | `REPLICATION_REPORT.md` | code + tests; updates `memory-bank/progress.md` |
| 5 | `REPLICATION_REPORT.md`, tests, app | before/after evidence (chat) |
| 6 | working tree, Stage-5 evidence | branch, commit, PR/MR URL |
