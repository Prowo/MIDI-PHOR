# Running the demo

## Requirements

- Python 3.11+ recommended (3.12 works)
- See `requirements.txt` for Python packages
- Optional: General MIDI SoundFont for nicer audio (see `midi_models/README.md`)
- Optional: `OPENAI_API_KEY` for LLM-written captions (otherwise template captions are used)

## Local (Windows note)

PowerShell may buffer Python output. If you do not see the Gradio URL, run:

```bat
cmd /c "python -u app.py"
```

Then open the printed URL (often `http://127.0.0.1:7860` or another port if 7860 is busy).

To pin the port:

```bash
set GRADIO_SERVER_PORT=7860
python app.py
```

(On Linux/macOS use `export`.)

## Hugging Face Spaces

How the browser, container, and API keys relate: [HF_SPACES.md](HF_SPACES.md).

### Push this repo from your machine (AI agent / CLI)

Use a **write-capable** token: [Hugging Face → Settings → Access Tokens](https://huggingface.co/settings/tokens).  
Classic token: role **Write**. Fine-grained: include permissions to **create and write** the Space repository.

```powershell
set HF_TOKEN=hf_xxxxxxxx
cd path\to\MIDI-PHOR
python scripts/hf_push_space.py --repo-id StevenAu/MIDI-PHOR
```

This creates the Docker Space (if missing) and uploads the project. If `create_repo` returns **403**, your token cannot create Spaces — create the Space once in the web UI (Docker SDK), then either connect **GitHub** `Prowo/MIDI-PHOR` in Space settings or run the script again after fixing token permissions.

### Why there is no “GitHub repo” in Space Settings

Hugging Face **does not** show a one-click “connect this Space to GitHub” control in Settings. The Hub repo for a Space **is** a Git repo (`https://huggingface.co/spaces/<user>/<name>`), but linking it to GitHub is done by **you**, not by a Settings toggle.

**Option A — GitHub Action (recommended)**  
This repo includes `.github/workflows/sync-to-hf-space.yml` (at repository root). On every push to **`main`**, it mirrors the GitHub tree to your Space (same files as Git — respects `.gitignore`).

1. On **GitHub** (`Prowo/MIDI-PHOR`): **Settings → Secrets and variables → Actions** → New repository secret **`HF_TOKEN`** (HF access token with **write** access to `StevenAu/MIDI-PHOR`).
2. If your Space path is not `StevenAu/MIDI-PHOR`, edit `huggingface_repo_id` in that workflow file.
3. Push to `main` (or run **Actions → Sync to Hugging Face Space → Run workflow**).

Official docs: [Managing Spaces with GitHub Actions](https://huggingface.co/docs/hub/spaces-github-actions).

**Option B — Manual `git push` to the Space remote**

```bash
# one-time: add remote (use a WRITE token; do not commit the URL)
git remote add hf https://huggingface.co/spaces/StevenAu/MIDI-PHOR
export HF_TOKEN=hf_...   # write token
git push https://StevenAu:${HF_TOKEN}@huggingface.co/spaces/StevenAu/MIDI-PHOR main:main
```

**Option C — `scripts/hf_push_space.py` from your PC**  
Uploads from disk; the script **ignores** `cache/`, `.venv/`, `data/`, etc. (not the same as `.gitignore`, but aligned). Never paste tokens in chat.

If you use `hf_push_space.py`, never paste API tokens into chat — use `HF_TOKEN` in your shell only, then **revoke** the token if it was exposed.

1. Create a **Docker** Space and push this repository.
2. The `Dockerfile` installs FluidSynth and the Debian `fluid-soundfont-gm` package, and sets `SF2_PATH` for the container.
3. **OpenAI (optional):** You do **not** need to set `OPENAI_API_KEY`. Without it, the Space runs in **template-only** caption mode and the LLM checkbox is hidden.
4. If you **do** add `OPENAI_API_KEY` as a Space secret, optional LLM captions appear. To cap cost on a public Space, set:
   - `OPENAI_MAX_CALLS` — e.g. `20` for twenty successful LLM completions total (counter stored under `CACHE_DIR`, usually `cache/.openai_llm_calls`).
   - `LLM_QUOTA_PATH` — optional absolute path if you mount persistent storage and want the counter to survive restarts.
5. With a cap, set `USE_LLM=false` in Space variables if you want the checkbox default **off** so visitors use templates until they opt in.

## Embedding the Space elsewhere

- **iframe:** point `src` at your Space URL (e.g. `https://YOURNAME-midiphor.hf.space`).
- **Gradio embed:** see `web/embed-snippet.html`.

Replace placeholder URLs with your real Space URL before sharing.

## Command-line pipeline (batch)

For processing folders of files with DuckDB, use `cli.py` and the scripts under `scripts/`. See the main `README.md` for layout and environment variables.
