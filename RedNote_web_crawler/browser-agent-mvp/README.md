# Browser Agent MVP

This MVP implements the deterministic runtime core from:

```text
Browser Agent = CUA + CDP + Recipe + Network Extractor + Feedback Memory
```

Current scope:

- Recipe DSL validation
- HAR-like network snapshot replay
- Network JSON extraction
- Field-level fallback hooks for DOM/runtime
- Field transforms
- Source audit
- Dedupe keys and content hashes
- Health checks and feedback events
- Optional live browser runtime with Playwright
- Named datasets, for example `post` and `comments`

Run the example:

```bash
python3 -m browser_agent.cli validate-recipe examples/search_recipe.json
python3 -m browser_agent.cli extract \
  --recipe examples/search_recipe.json \
  --snapshots examples/search_snapshots.json
```

Run tests:

```bash
python3 -m unittest discover -s tests
```

Optional live recording, when Playwright is installed:

```bash
python3 -m browser_agent.cli record "https://example.com" \
  --output work/example_snapshots.json
```

Run a real browser task with a Recipe:

```bash
python3 -m browser_agent.cli run "https://example.com" \
  --recipe examples/search_recipe.json \
  --output work/run-result.json
```

Analyze captured Network snapshots to discover candidate API sources:

```bash
python3 -m browser_agent.cli analyze-snapshots work/rednote-note-result.json \
  --keyword comment \
  --keyword note \
  --keyword cursor \
  --limit 20
```

Export extracted dataset records:

```bash
python3 -m browser_agent.cli export work/rednote-note-result.json \
  --dataset post \
  --output work/rednote-post.jsonl

python3 -m browser_agent.cli export work/rednote-note-result.json \
  --dataset comments \
  --output work/rednote-comments.csv
```

For RedNote/Xiaohongshu note detail capture, use the template as a starting
point and adjust URL patterns / JSON paths after recording a page you are
authorized to access:

```bash
python3 -m browser_agent.cli open-session "https://www.xiaohongshu.com" \
  --user-data-dir work/rednote-browser-profile

python3 -m browser_agent.cli run "https://www.xiaohongshu.com/explore/NOTE_ID" \
  --recipe examples/rednote_note_recipe.template.json \
  --user-data-dir work/rednote-browser-profile \
  --output work/rednote-note-result.json \
  --wait-ms 5000
```

The first run with `--user-data-dir` opens a persistent browser profile. Log in
manually if needed, then run the same command again to reuse that authorized
session. This project does not implement captcha bypass, signature cracking, or
account/proxy automation.

The RedNote template uses `scroll_until_stable` to keep scrolling until the page
height stops changing for several rounds. That is a best-effort way to trigger
comment pagination from the browser UI; if the site requires explicit "show
more replies" buttons, add `click` actions for those controls after recording
the page behavior.

Recommended RedNote tuning loop:

1. Run with `examples/rednote_note_recipe.template.json` and save the result.
2. Run `analyze-snapshots` on the saved result.
3. Find endpoints whose paths contain `feed`, `comment`, `page`, or whose JSON
   array paths look like `$.data.items[*]` / `$.data.comments[*]`.
4. Adjust `url_pattern`, `item_path`, and `fields` in the Recipe.
5. Re-run `extract` or `run`, then export `post` and `comments`.

Next implementation layer:

- SQLite/PostgreSQL persistence for tasks, results, snapshots, feedback
- Explorer Agent that proposes Recipe drafts from screenshots, DOM summaries, and Network candidates
