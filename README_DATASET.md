# MIDI Reasoning Stack — Dataset Descriptions and Interactive Chat Agent

This README covers **two related but distinct requirements**:

1. **Text Description Ensemble (Base Dataset)** — generating long, producer‑grade textual descriptions of MIDI files, fully traced to symbolic components.
2. **Interactive Chat Agent (Next Layer)** — building an interactive, fact‑checked chatbot over the same data, powered by a semantic graph and DuckDB queries.

The text description ensemble (1) is the **base case**: it produces dataset entries (MIDI file ↔ long description ↔ provenance). The interactive agent (2) extends this foundation by making the information queryable, traceable, and verifiable in real time.

---

## 0. High‑Level Architecture

```
      .mid files (raw)
            │
        [Parser/Extractors]
            │
     ┌─────────────┬─────────────┐
     │             │             │
 Time‑Series   Symbolic Facts   Graph Nodes/Edges
   Tables      (sections, etc.)  (relations over time)
  (DuckDB)        (DuckDB)         (DuckDB)
     │             │             │
     └──── Facts_Text (RAG chunks, bar‑aligned sentences)
                   │
             Text Description Ensemble (Base Dataset)
                   │
        ┌──────────┴──────────┐
        │                     │
   Long Descriptions    Chat Agent Layer
   (dataset release)    (retrieval, verification,
                         interactive reasoning)
```

---

## Part 1 — Text Description Ensemble (Base Dataset)

**Goal:** Build a dataset of long, producer‑oriented descriptions of MIDI files, each claim tied back to symbolic evidence.

### 1. Components to Build

- **Time‑Series Tables** (DuckDB)
  - `files, tracks, notes, controllers, tempo_ts, tsigs, keys`
- **Symbolic Facts**
  - `sections, chords(roman), ensemble, motifs, motif_occ, layering`
  - Expression summaries (velocity arcs, CC usage)
- **Facts_Text Table**
  - Short bar‑aligned sentences (objective claims with provenance)
- **Descriptions Table**
  - Long descriptions per file: paragraphs covering form, ensemble, harmony, rhythm, expression.
  - Each sentence references facts via `(file_id, fact_id)` or graph node/edge IDs.

### 2. Generation Pipeline

1. **Extract → Populate DuckDB** with time‑series + symbolic tables.
2. **Generate Facts_Text** sentences (templated or scripted).
3. **Planner** (deterministic): outline description in sections: FORM → ENSEMBLE → HARMONY → RHYTHM → EXPRESSION → PRODUCER NOTES.
4. **Composer** (LLM or templates): fill outline using Facts_Text + symbolic tables.
5. **Verifier**: check every claim against DuckDB; attach provenance pointers.
6. **Output**: store long description in `descriptions` table with claim‑evidence map.

### 3. Example Dataset Entry

```json
{
  "file_id": "song_001",
  "description": "Piece in 4/4, ~92 BPM, F minor. Intro (bars 0–8) is rubato piano. Chorus (24–40) introduces brass ensemble doubling the violin motif while harmony moves I–V6–vi–IV.",
  "claims": [
    {"text": "4/4, ~92 BPM, F minor", "evidence": ["tempo_ts:row1","keys:row3"]},
    {"text": "Intro bars 0–8 rubato piano", "evidence": ["sections:intro","tracks:piano"]},
    {"text": "Chorus 24–40 brass ensemble doubling violin motif", "evidence": ["sections:chorus","layering:brass_enter_24","motif_occ:m1_24"]}
  ]
}
```

---

## Part 2 — Interactive Chat Agent (Next Layer)

**Goal:** Build a chatbot that answers natural questions about a MIDI file, with verifiable and traceable outputs.

### 1. Components to Build

- **Retriever**
  - Search `facts_text` (BM25/embeddings) scoped by file_id (and bars/instruments if mentioned).
- **Graph Layer** (DuckDB tables `graph_nodes`, `graph_edges`)
  - Encodes time‑scoped relations: OCCURS_IN, DOUBLES, CALLS, ANSWERS, CONTROLS.
- **Tool Calls**
  - `graph.query({filters,time_scope})` to fetch facts.
  - `chunks.get({start_bar,end_bar})` to fetch context windows.
- **Verifier**
  - Parse LLM output into predicates; check against graph edges/tables.
  - Auto‑correct or remove unverified claims.
- **Answerer (LLM)**
  - Composes answer using retrieved facts; required to include bars/instruments in claims.

### 2. Query Flow

1. User asks: *“Where do the strings first enter?”*
2. Retriever: finds fact in `facts_text` + graph edge `(TextureEvent:enter_str_8)`.
3. Answerer: *“Strings (prog 48) enter at bar 8, sustaining pads.”*
4. Verifier: confirms via `layering` + `graph_edges`.
5. Provenance: attach `(layering:event_id, bars=8)` to the claim.

### 3. Why the Graph Matters

- **Information**: Encodes musical **steps** (sections, chord spans, motifs) and relations (DOUBLES, CALLS). This goes beyond instrument linking.
- **Visuals**: Graph slices render into timelines, call‑response diagrams, motif recurrence maps, making arrangement structure legible.
- **Verification**: Every chatbot claim can be matched to a graph edge or fact table, ensuring trustworthy answers.

---

## Roadmap

- **Phase 1**: Implement Part 1 — Text Description Ensemble. Build tables, facts_text, description generator + verifier. Release dataset.
- **Phase 2**: Extend to Part 2 — Interactive Chat Agent. Add graph tables, retrieval, tool calls, verification loop, provenance.
- **Phase 3**: Visualization layer. Show graph slices (section timelines, doubling maps) alongside chatbot answers.

---

**Summary:**
- The **Text Description Ensemble** produces long, fact‑checked descriptions — the base dataset.
- The **Chat Agent Layer** makes those same facts interactive, enabling natural language queries with traceable, visualizable evidence.

