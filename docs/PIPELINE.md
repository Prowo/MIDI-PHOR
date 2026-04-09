# MIDIPHOR processing pipeline

End-to-end flow from MIDI to caption, as shown in the Gradio demo.

## Stages

1. **Symbolic extraction** (`extractors/symbolic.py`)  
   Reads MIDI into DuckDB: songs, tempo/time signature, bars, tracks, notes, chords, keys, bar-level metrics, motifs.

2. **Section merge** (`assemble/section_merge.py`)  
   Builds or merges section rows used for span queries and downstream features.

3. **Graph** (`extractors/graph.py`)  
   Optional orchestration graph: nodes (tracks/roles) and edges (e.g. co-occurrence, rhythmic relationships).

4. **Slots** (`assemble/slots.py`)  
   Aggregates DB rows into a single feature dictionary (meter, tempo, progression, texture, tags, events, etc.).

5. **Caption**  
   - **Template:** `assemble/caption.py`  
   - **LLM:** `assemble/llm_prompt.py` (OpenAI or Anthropic via env vars)

6. **Audio preview** (`scripts/render_midi.py`)  
   Renders MIDI to WAV with FluidSynth when a SoundFont path exists; otherwise PrettyMIDI’s built-in synth.

## Database schema

Table definitions live in `schema/ddl.sql`. The app uses an ephemeral per-request DuckDB file under `cache/` for the demo.

## What is not in the default demo path

- Full **audio feature** extraction (`extractors/audio.py`) with Essentia models is optional and needs extra weights in `midi_models/` (see `midi_models/README.md`).
- **Legacy ScoreSpec** offline generators (historical `scorespec_json/` tooling) are not in this trimmed repo. The **Gradio demo** instead writes **derived** paper-style exports from the live DuckDB state: see `assemble/paper_exports.py` (`scorespec.json`, `scorespec_lite.json`, `enhanced_facts.txt`, `hierarchical_facts.json`). `assemble/llm_prompt.py` may still *read* pre-made `exports/*.hierarchical_facts.json` when composing prompts from disk; the demo generates its own copies under `cache/`.
