# SKILL.md — A4I Hackathon Submission Markdown Generator

A specification for producing the **A4I Hackathon submission markdown** — a clear,
evidence-backed write-up of the work done, aimed at **upper management and technical
reviewers**. It follows the official template format **exactly** and is filled **only** with
facts that can be verified from the repository or supplied by the team.

> Persona: a senior solution designer (20+ yrs) presenting delivered work to leadership.
> Every claim is concrete, sourced, and skimmable. No marketing language.

---

## 1. Non-negotiable rules (read first)

1. **No hallucination. No assumptions.** State only what is (a) supplied by the user, or
   (b) directly verifiable in the repo (code, docs, tests, CI, git history). If neither, do
   **not** invent — insert an explicit placeholder:
   `> ⚠️ NEEDS INPUT: <exactly what is missing>`
2. **Cite the evidence.** Where practical, point to the source: a file path
   (`src/...`), a test count, a CI workflow, a PR/commit link. Reviewers trust traceable claims.
3. **Follow the attached format exactly** — same H1/H2/H3 headings, same order, same section
   numbers (1–8). Do not add, rename, reorder, or drop sections. Section 8 is optional and may
   be omitted only if empty.
4. **Distinguish fact from intent.** Delivered work = present tense, evidenced. Plans/ideas =
   clearly labelled as future/next-step, never stated as done.
5. **Management-readable.** Lead each section with the point; keep paragraphs short; prefer
   bullets and tables. Define any acronym on first use.

---

## 2. The exact output format

Reproduce this structure verbatim (headings and numbering must match the official template):

```
# {Project Name}
## 1. Pull/Merge request
## 2. Problem & Scope
### Context
### Problem to solve
### Why it matters
### In scope
### Out of scope
### Assumptions
## 3. Use Cases Delivered
### Use case 1   (Description / Expected outcome / What was implemented)
### Use case 2   …
## 4. Implementation Overview
### Architecture
### Key technical choices
## 5. Coding Assistant Usage
### Tools used
### Approach
### Agentic features implemented
### Key learnings
## 6. Quality & Testing
### Testing approach
### Code quality
## 7. Technical Documentation
## 8. Appendix (optional)
```

### What each section must contain — and where to source it

| Section | Must contain | Primary source | If unknown |
|---|---|---|---|
| **Project Name** (H1) | The actual project/repo name | repo name, `README`, manifest | placeholder |
| **1. Pull/Merge request** | The PR/MR link(s) for the work | user, or `git` remote + branch → compare URL (confirm) | `⚠️ NEEDS INPUT` |
| **2 · Context** | Existing system, stakeholders, constraints | README "overview", docs | `⚠️ NEEDS INPUT` |
| **2 · Problem to solve** | The concrete problem | README "why" section | `⚠️ NEEDS INPUT` |
| **2 · Why it matters** | Business/technical impact, risks | README, SECURITY, domain docs | `⚠️ NEEDS INPUT` |
| **2 · In scope** | What the hackathon work covered | git diff/log of the branch, user | `⚠️ NEEDS INPUT` |
| **2 · Out of scope** | What was deliberately excluded | user (a decision, not derivable) | `⚠️ NEEDS INPUT` |
| **2 · Assumptions** | Key team assumptions | user (a decision, not derivable) | `⚠️ NEEDS INPUT` |
| **3. Use Cases Delivered** | One block per use case: *Description / Expected outcome / What was implemented* | code, README features, examples, tests | `⚠️ NEEDS INPUT` |
| **4 · Architecture** | Components and how they fit | code structure, README architecture, diagrams | describe only what exists |
| **4 · Key technical choices** | Frameworks, tools, design decisions + rationale | manifests/deps, code, README | state choice; flag rationale if unstated |
| **5 · Tools used** | Coding assistant(s) used | user; corroborate via `.agents`/`.claude`, commit co-authors | `⚠️ NEEDS INPUT` |
| **5 · Approach** | Prompting, context usage, workflows | user | `⚠️ NEEDS INPUT` |
| **5 · Agentic features implemented** | Agentic features used + for which use cases | user; corroborate via skills/workflows in repo | `⚠️ NEEDS INPUT` |
| **5 · Key learnings** | What worked, limitations | user | `⚠️ NEEDS INPUT` |
| **6 · Testing approach** | Test strategy (unit/integration/…) | `test/`, CI workflows, test configs | describe what exists |
| **6 · Code quality** | Refactoring, readability, maintainability work | linters/formatters config, CI, git history | state only what's evidenced |
| **7. Technical Documentation** | README-style: setup, usage, structure, key details | README, ONBOARDING, docs/ | summarize + link |
| **8. Appendix** | Extra scripts, links, prompts | user, repo | omit if empty |

> Sections **2 (Out of scope, Assumptions)** and **5 (Approach, Agentic features, Key learnings)**
> describe human decisions and process — they are **rarely derivable from the repo**. Expect to
> ask the user or leave `⚠️ NEEDS INPUT`. Never fabricate them.

---

## 3. Intake questions (ask the user — optional, but strongly advised for §2 and §5)

Ask once, in a single batched prompt. Make clear each is optional, and apply the precedence
**user answer → verifiable repo evidence → `⚠️ NEEDS INPUT` placeholder** (never invention):

- **PR/MR link?** (for §1) — else derive a compare URL from the remote/branch and ask to confirm.
- **Out of scope?** and **Assumptions?** (for §2) — decisions only the team knows.
- **Coding assistant(s) used, and how** — tools, prompting/context approach, workflows (§5).
- **Agentic features used, and for which use cases?** (§5)
- **Key learnings — what worked, what didn't?** (§5)
- **Anything for the Appendix?** (§8)

Everything else (problem, why, use cases, architecture, tech choices, testing, docs) should be
**drafted from the repo first**, then shown to the user to confirm or correct.

---

## 4. Repo-sourcing playbook (for everything not answered)

Mine, in order, and record where each fact came from:

1. `README*` / `ONBOARDING*` / `docs/` → project name, context, problem, why, features, docs.
2. Manifests / deps (`pyproject.toml`, `package.json`, `pom.xml`, `go.mod`, …) → tech choices.
3. Code tree (`src/`, packages) → architecture, components, delivered use cases.
4. `examples/` → concrete use cases and expected outcomes.
5. `test/` + test configs → testing approach; **count** suites/files for evidence.
6. CI workflows (`.github/workflows`) → testing automation, quality gates, release.
7. Lint/format configs (`.editorconfig`, ruff/eslint/black, pre-commit) → code-quality claims.
8. `.agents/` `.claude/` + commit co-authors + git log on the branch → corroborate §5 and scope.

**Rule:** if mining doesn't surface a fact and the user didn't give it, write
`⚠️ NEEDS INPUT: …` — do not approximate.

---

## 5. Ready-to-fill template

Copy this, fill it, and save per §6. Keep headings/numbers exactly as below. Replace each
`{{…}}`. Replace any field you cannot source with the `⚠️ NEEDS INPUT` line. Duplicate the
"Use case" block as many times as there are real use cases.

```markdown
# {{Project Name}}

## 1. Pull/Merge request
{{PR/MR link — e.g. https://github.com/<org>/<repo>/pull/<n>}}

## 2. Problem & Scope

### Context
{{Existing system, stakeholders, constraints — sourced from README/docs.}}

### Problem to solve
{{The concrete problem this work addresses.}}

### Why it matters
{{Business, technical, and risk impact.}}

### In scope
{{What the hackathon work covered (back with branch diff/commits).}}

### Out of scope
{{What was deliberately not covered.}}

### Assumptions
{{Key assumptions the team made.}}

---

## 3. Use Cases Delivered

### Use case 1
**Description:** {{what it is}}
**Expected outcome:** {{the intended result}}
**What was implemented:** {{what actually shipped — cite files/examples/tests}}

### Use case 2
**Description:** {{…}}
**Expected outcome:** {{…}}
**What was implemented:** {{…}}

---

## 4. Implementation Overview

### Architecture
{{Overall architecture and main components. Include a diagram only if one exists or can be
drawn faithfully from the code.}}

### Key technical choices
{{Frameworks, tools, and main design decisions — with rationale where stated.}}

---

## 5. Coding Assistant Usage

### Tools used
{{Coding assistant(s) used.}}

### Approach
{{How they were used — prompting, context usage, workflows.}}

### Agentic features implemented
{{Agentic features used, and for which use cases.}}

### Key learnings
{{What worked well and the limitations encountered.}}

---

## 6. Quality & Testing

### Testing approach
{{Testing strategy — unit/integration/etc. Cite suite/file counts and CI.}}

### Code quality
{{Refactoring, readability, maintainability improvements — only what's evidenced.}}

---

## 7. Technical Documentation
{{README-style detail: setup, usage, structure, key implementation details. Summarize and
link to the in-repo docs.}}

---

## 8. Appendix (optional)
{{Additional scripts, links, prompts, or supporting materials. Omit this section if empty.}}
```

---

## 6. File naming & output

- Name the file using the official convention:
  **`A4I Hackathon_<TeamName>_MarkdownFile.md`** (ask for the team name; placeholder if unknown).
- Output one markdown file. Do not split across files.
- After generating, **list every `⚠️ NEEDS INPUT` item** back to the user so they can fill gaps.

---

## 7. Writing principles (leadership audience)

- **Lead with the answer**, then support it — reviewers skim.
- **Quantify** ("9 test suites", "4 integrations") rather than qualify ("robust", "extensive").
- **Tables and bullets** over long prose; one idea per bullet.
- **Plain language**; expand acronyms on first use.
- **Traceable**: link PRs, files, and docs so claims can be checked.
- **Honest scope**: separate delivered work from intent; don't overstate.

---

## 8. Validation checklist

- [ ] H1 + sections 1–8 present, named and numbered exactly as the official template.
- [ ] Section 1 contains a real PR/MR link (or `⚠️ NEEDS INPUT`).
- [ ] Every factual claim is sourced from the repo or the user — nothing invented.
- [ ] Unknowns are explicit `⚠️ NEEDS INPUT` lines, not guesses.
- [ ] Delivered vs. planned work is clearly distinguished.
- [ ] Use-case blocks each have Description / Expected outcome / What was implemented.
- [ ] §5 reflects only what the team confirmed about assistant usage.
- [ ] Testing/quality claims cite concrete evidence (counts, configs, CI).
- [ ] File named `A4I Hackathon_<TeamName>_MarkdownFile.md`.
- [ ] All `⚠️ NEEDS INPUT` items surfaced to the user after generation.
