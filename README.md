# MIDIPHOR — MIDI captioning demo

Symbolic MIDI analysis → structured features → natural-language captions (template or LLM). This repository is trimmed for a **web demo** (Gradio), embedding on other sites, and optional Hugging Face Spaces deployment.

**Paper:** [MIDI-PHOR: Multi-View Distillation for Music Understanding and Captioning](https://aclanthology.org/2026.nlp4musa-1.6/) (NLP4MusA 2026) — [PDF](https://aclanthology.org/2026.nlp4musa-1.6.pdf) · [DOI](https://doi.org/10.18653/v1/2026.nlp4musa-1.6)

## Quick start

```bash
pip install -r requirements.txt
export OPENAI_API_KEY=...   # optional, for LLM captions
python app.py
```

Open the URL printed in the terminal (Gradio). On Windows, if you see no URL, use `cmd /c "python -u app.py"` — see [docs/DEMO.md](docs/DEMO.md).

**SoundFont:** For FluidSynth rendering, install a GM `.sf2` or use the system package on Linux. Details: [midi_models/README.md](midi_models/README.md).

## Documentation

| Doc | Purpose |
|-----|---------|
| [docs/DEMO.md](docs/DEMO.md) | Local run, Hugging Face Spaces, embed snippets |
| [docs/HF_SPACES.md](docs/HF_SPACES.md) | How Spaces run your app and when API keys are used |
| [docs/PIPELINE.md](docs/PIPELINE.md) | Processing stages and schema overview |
| [midi_models/README.md](midi_models/README.md) | SoundFont and optional Essentia weights |

The Gradio UI includes **Bring your own LLM**: copy the generated **caption prompt** and **feature JSON** into any model you control, so visitors can try LLM wording **without** using your OpenAI key.

## Project layout

```
├── app.py              # Gradio web demo (pipeline UI + tables)
├── cli.py              # Batch CLI over MIDI folders + DuckDB
├── assemble/           # Slots, captions, LLM prompts, section merge
├── extractors/         # Symbolic MIDI, optional audio, graph
├── db/                 # DuckDB helpers
├── schema/             # SQL DDL
├── scripts/            # render_midi, build_db_from_manifest, list_songs, llm_caption_json
├── web/                # iframe / embed examples for your portfolio
├── docs/               # Detailed guides
└── midi_models/        # Small config/metadata; large *.sf2/*.pb not in git
```

## Configuration

| Variable | Description |
|----------|-------------|
| `OPENAI_API_KEY` | LLM captions via OpenAI (`assemble/llm_prompt.py`) |
| `OPENAI_MODEL` | Model id (default `gpt-4o-mini`) |
| `SF2_PATH` | Path to General MIDI SoundFont for WAV preview |
| `CACHE_DIR` | Temp DB/audio for the demo (default `cache`) |
| `USE_LLM` | `true` / `false` for Gradio checkbox default |
| `GRADIO_SERVER_NAME` | e.g. `0.0.0.0` in Docker |
| `GRADIO_SERVER_PORT` | e.g. `7860` on Hugging Face Spaces |
| `OPENAI_MAX_CALLS` | Optional cap on successful LLM API calls (public demos) |
| `LLM_QUOTA_PATH` | Optional file path for persisting the LLM call counter |

## Deploy (Hugging Face Spaces)

Create a **Docker** Space and push this repo. The `Dockerfile` installs FluidSynth and `fluid-soundfont-gm`. Add `OPENAI_API_KEY` under Space secrets if you want LLM captions.

## Embed on another site

```html
<iframe
  src="https://YOUR_HF_USERNAME-YOUR_SPACE_NAME.hf.space"
  width="100%"
  height="720"
  style="border:0;border-radius:12px"
  title="MIDIPHOR demo"></iframe>
```

Replace the `src` with your real Space URL after you create it on [Hugging Face Spaces](https://huggingface.co/spaces).

More options: [web/embed-snippet.html](web/embed-snippet.html).

## License

MIT — see [LICENSE](LICENSE).

## Citation

If you use this work academically, cite the NLP4MusA 2026 paper:

**ACL Anthology:** [https://aclanthology.org/2026.nlp4musa-1.6/](https://aclanthology.org/2026.nlp4musa-1.6/)

```bibtex
@inproceedings{au-2026-midi,
    title = "{MIDI}-{PHOR}: Multi-View Distillation for Music Understanding and Captioning",
    author = "Au, Steven",
    editor = "Epure, Elena V.  and
      Oramas, Sergio  and
      Doh, SeungHeon  and
      Ramoneda, Pedro  and
      Kruspe, Anna  and
      Sordo, Mohamed",
    booktitle = "Proceedings of the 4th Workshop on {NLP} for Music and Audio ({NLP}4{M}us{A} 2026)",
    month = mar,
    year = "2026",
    address = "Rabat, Morocco",
    publisher = "Association for Computational Linguistics",
    url = "https://aclanthology.org/2026.nlp4musa-1.6/",
    doi = "10.18653/v1/2026.nlp4musa-1.6",
    pages = "33--43",
    isbn = "979-8-89176-369-2",
}
```

**Informal (Markdown):** [MIDI-PHOR: Multi-View Distillation for Music Understanding and Captioning](https://aclanthology.org/2026.nlp4musa-1.6/) (Au, NLP4MusA 2026)

### Where to update links later

| What | File(s) |
|------|---------|
| Paper URL, BibTeX, DOI | `README.md` (top blurb + **Citation**) |
| Gradio footer (GitHub + Paper) | `app.py` (bottom `gr.Markdown` inside `with gr.Blocks`) |
| Portfolio embed + widget footer | `web/embed-snippet.html`, `web/embed.html` (set `YOUR_SPACE_URL` / `API_URL` after you create a Hugging Face Space) |

If the Anthology fixes metadata (e.g. abstract), refresh the **Citation** block from [the paper’s Anthology page](https://aclanthology.org/2026.nlp4musa-1.6/) (Export citation → BibTeX).
