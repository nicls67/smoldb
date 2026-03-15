---
trigger: always_on
---

# AGENTS

## Purpose
This file orients coding agents working in this repo. Keep changes focused, avoid unrelated formatting, and follow existing patterns.
The repo contains the `smoldb` library which is a simple databse librairy written in Rust.

## Repo layout
1. `src/` contains the source code for the library (`lib.rs`).
2. `tests/` contains integration tests.
3. `Cargo.toml` defines the package and dependencies.

## Build & test
- Build the project: `cargo build`
- Run tests: `cargo test`

## Change guidelines
- Prefer small, targeted edits; avoid sweeping refactors unless asked.
- Keep ASCII in new content unless the file already uses non-ASCII.
- Add comments only when logic is non-obvious.
- Update or create documentation when needed. Methods and functions documentation needs to include : functionality description, parameters description, return description, error handling, panicking (when concerned).
- If you need to touch multiple crates, explain why in the final response.
- Always ask for user review after generating a action plan. Never update code by yourself.
- Update and run tests after each code update
- Update the package version after each change following SEMVER rules by comparison with the last tag, version shall not be necessarily updated after each commit.

## Naming rules
1. Follow standard Rust naming conventions (snake_case for variables/functions, CamelCase for types/traits, SCREAMING_SNAKE_CASE for constants).
2. Use the following rule to name variables and constants : constants shall start with 'K_', global variables shall start with 'G_', local variables shall start with 'l', functions and methods parameters shall start with 'p_'

## Git rules
This rule applies each time a git branch needs to be created or renamed

### Instructions
1. Never ask for a branch name, define it by yourself
2. Analyse the task :
  - In case of a new feature : feat/task-name
  - In case of bug fix : fix/task-name
3. Always lower case
4. When the pull request is linked to a Github issue, add 'Closes #ID' to the pull request message.

### End of task
After each successful merge :
1. Always delete the associated branch
2. Confirm that the associated GitHub issue is closed

## Review rules

### Trigger
This rule applies when the user asks to review ("review", "my_review")

### Instructions
You are a strict reviewer focused on bugs and code quality. Your mission is to produce an actionable report.
Follow the steps below in order, without skipping any.
Before any code update (except imports cleanup and documentation update), propose a modification plan to the user. You need his agrrement before proceeding.

### 1) Git state and review scope
1. Run `git status -sb` and summarize what is modified.
2. The review is done by default on the uncomitted changes. If the user asks to review the branch, work on all the changes made in the current branch. If the user asks to review all the code base, work on all the files in the current repository.

### 2) Human-style code review
Read the diff and focus primarily on:
  - Check for potential bugs or performance issues.
  - Ensure code is properly documented and documentation is up-to-date (including README.md).
  - Check error handling and error messages.
  - Ensure code is well-structured and follows best practices.
  - Verify that the code is easy to understand and maintain.
  - Check all imports are used and remove unused imports.
  - Check the package version has been correctly updated (compare with previous tag)
  - Check all tests are consistents with the code and all functions are tested

### 3) Documentation update and code cleanup. You are allowed to update the modified files for that.
- First update documentation (or create it if it is missing) for each modified item (method, function, structure, etc...).
- Correct any comment or documentation that is inconsistent with the code
- Clean imports : remove unused imports

### 4) Automated checks (read-only)
- If a `Cargo.toml` is present:
  - `cargo fmt --check`
  - `cargo clippy --all-targets --all-features -- -D warnings`
  - `cargo check`
- Otherwise, adapt to the detected ecosystem (package.json, pyproject, etc.) using the standard lint/test commands in check mode.

### 5) Automated checks corrections
- If any issue has been found on a modified file by an automated check, you are allowed to correct it.

### 6) Run tests
- Run `cargo test`

### 7) Required output format
Produce a report with **exactly** the following sections:

#### Summary
- 3–6 bullet points describing what changed and the overall risk level.

#### Blocking issues (must-fix)
- Numbered list.
- For each item: file/line if possible, explanation, and concrete suggestion.

#### Important issues (should-fix)
- Same structure as above.

#### Improvements (nice-to-have)
- Same structure as above.

#### Check results
- Summarize the results of the automatic checks
- Give explanation/concrete suggestion

#### Final recommendation
- “OK to merge” / “OK with fixes” / “Not OK”
- Short justification (2–3 sentences).

### 7) Trigger examples
- `review`
- “Run review before I push”
- “Review this diff and run fmt/clippy/tests”
- "@my_review"

### 8) When the review is finished, remove any temporary file you created (for example check reports ot git diffs)