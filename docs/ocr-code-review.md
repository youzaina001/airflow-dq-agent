# AI code review with OpenCodeReview

This repo runs [OpenCodeReview](https://github.com/alibaba/open-code-review) (`ocr`)
for AI-assisted review. The LLM is OpenRouter, model `z-ai/glm-5.3-flash`.

OCR is an advisory reviewer, not a gate. The deterministic loop (`make check`,
`make ci`) and human review remain the authority; OCR findings are extra input.

## How it is wired

| Layer | File | Committed? |
| --- | --- | --- |
| Project review rules | `.opencodereview/rule.json` | Yes |
| Local Make targets | `Makefile` (`review-preview`, `review`, `review-branch`) | Yes |
| PR review workflow | `.github/workflows/ocr-review.yml` | Yes |
| LLM provider + key | `~/.opencodereview/config.json` (machine-local) | No |

`.opencodereview/rule.json` is the highest-priority project layer in OCR's
four-layer rule chain (`--rule` > project > `~/.opencodereview/rule.json` >
embedded system rules). It adds the governed-data-quality boundaries from
`CONTEXT.md` on top of the built-in Python rules:

- the live proposer stays read-only and a Candidate Proposal stays untrusted;
- `GovernedAction` registrations own their full lifecycle;
- planning and apply fail closed, and admission is single-use and time-bounded;
- Audit Lineage carries IDs, fingerprints, counts, and sanitized reasons only;
- XCom payloads never carry samples or model-authored text;
- tests encode the authority boundary.

Run `ocr rules check <path>` to see which rule wins for a file.

## One-time local setup

Git >= 2.41 and Node are required. Install the CLI and point it at OpenRouter:

```bash
npm install -g @alibaba-group/open-code-review

ocr config set provider                             openrouter
ocr config set custom_providers.openrouter.url       https://openrouter.ai/api/v1
ocr config set custom_providers.openrouter.protocol  openai
ocr config set custom_providers.openrouter.model     z-ai/glm-5.3-flash
# Read the key from the environment at run time; never store it in config.json.
ocr config set custom_providers.openrouter.api_key_cmd 'printenv OPENROUTER_API_KEY'

export OPENROUTER_API_KEY=sk-or-...
ocr llm test
```

`api_key_cmd` is executed per `ocr` invocation and must exit 0 with a single
line on stdout. Use it (or `providers.<name>.api_key_cmd` for a built-in
provider) instead of `api_key` so the secret stays out of `~/.opencodereview/config.json`.

## Local loop

```bash
make review-preview   # which changed files would be reviewed (no LLM call, no cost)
make review           # review staged + unstaged + untracked changes
make review-branch    # review the current branch against origin/master
```

Other useful invocations:

```bash
# Review one commit
ocr review --commit <sha>

# JSON for tooling, and resume an interrupted run
ocr review --from origin/master --to HEAD --format json --output /tmp/ocr.json
ocr session list
ocr review --from origin/master --to HEAD --resume <session-id>

# Full-file audit of an area with no meaningful diff
ocr scan --path src/airflow_dq_agent/planning

# Give the model the PR's intent (highest-leverage flag)
ocr review --background "feat(apply): bind admission to a single plan"
```

Cost controls: `--effort low|medium|high`, `--max-tokens-budget <n>`,
`--concurrency <n>`, and `--max-tokens <n>`. `--preview` is always free.

## CI: on-demand PR review

`.github/workflows/ocr-review.yml` does **not** run on push or on PR
open/update. Start it yourself, then it posts findings as inline review
comments.

**Actions UI.** *Actions -> OpenCodeReview PR Review -> Run workflow*. Pick the
branch that contains the workflow (after merge, `master`), the pull request
number, and an OpenRouter model from the dropdown. Default model is
`z-ai/glm-5.3-flash`.

**PR comment.** On a pull request, a MEMBER/OWNER/COLLABORATOR can comment
`/ocreview`. That is a slash command, not a GitHub @mention, so it does not
tag a user. Optionally pass a dropdown model id:
`/ocreview x-ai/grok-4.6`. Any other token after `/ocreview` is ignored and
the default model is used. Comment triggers use the workflow file on the
default branch, so they work only after this workflow has landed on `master`.

```bash
gh workflow run ocr-review.yml --ref master -f pr_number=81 -f model=z-ai/glm-5.3-flash
```

Set one secret under **Settings -> Secrets and variables -> Actions**:

| Name | Value |
| --- | --- |
| `OPENROUTER_API_KEY` | OpenRouter key (`sk-or-...`) |

The workflow pins the action to OCR v1.12.7 and the npm CLI via `ocr_version`.
Manual runs and `/ocreview` comments have access to repository secrets; OCR only
reads the diff.

**Pilot mode.** The review step is `continue-on-error: true`, so findings never
block a merge. Once precision is trusted, remove that line to make it a soft
gate, and only then consider failing the job on high-severity findings.

## SDLC placement

1. **Local, before push** — `make review` (or a `pre-push` hook) to catch issues
   before they cost a CI cycle. `make review-preview` costs nothing.
2. **On-demand PR review** — `workflow_dispatch` or `/ocreview` on the PR, advisory
   during the pilot.
3. **Brownfield audit** — `ocr scan --path <dir>` for code that predates the
   diff-based loop.
4. **Standards drift** — extend `.opencodereview/rule.json` when a class of
   defect keeps slipping through; rules are cheaper and more stable than
   re-prompting.

## Observed performance

Measured on this machine with OpenRouter + `z-ai/glm-5.3-flash` at default
settings: a one-file `ocr scan` took **7m59s** and **41.3K tokens** (20.9K input,
20.5K output, 5 tool calls, 4 findings). Budget roughly that per file, and treat
a 15-minute review as normal rather than hung.

Cost and latency levers, cheapest first:

- `ocr review --preview` / `ocr scan --preview` — free, no LLM call.
- `--effort low` — one review round instead of two.
- `--max-tokens-budget <n>` — hard cap on total run tokens; partial results are
  still published.
- `--concurrency <n>` — parallel subtasks (default 8); lower it if OpenRouter
  rate-limits, raise it for many-file diffs.
- Model dropdown on `workflow_dispatch` (or `/ocreview <model>` on a PR) — point CI
  at a faster model without editing the workflow.

The CI job has `timeout-minutes: 60` and `review_task_timeout: 30` (per
group). Raise both if a grouped OpenRouter review still classifies files as
`timeout`. Keep the job cap above the per-group deadline.

## Notes and limits

- Review findings are a trade-off toward precision: OCR reports fewer false
  positives and misses some real issues. Keep human review.
- The workflow never checks out PR code. OCR reads the diff through the API.
  The untrusted part is the diff *content* fed to the model, not code
  execution; keep rule text and review context free of secrets.
- `continue-on-error: true` is deliberate for the pilot, but it also hides
  genuine failures (a missing key, an OpenRouter outage). Check the workflow log
  while the pilot runs, and remove the line once the review is a real gate.
- Code leaves the machine to OpenRouter. Do not add rules that pull secrets or
  row samples into review context; `.env` paths are excluded by OCR's built-in
  secret filter, but keep sample data out of committed files regardless.
- `.opencodereview/rule.json` is committed and reviewed like any other file;
  treat it as code. It must not contain secrets.
- `OCR_LLM_*` environment variables and the legacy `llm.*` config block are an
  alternative to the provider block for CI-only setups; the committed workflow
  uses the reusable action, which writes that block itself.
