# MIDI Reasoning Stack — Time‑Series, Symbolic Facts, and a Semantic Graph in DuckDB

This README gives a coding agent (and future contributors) a complete overview of the components to build for:

1) **MIDI time‑series event data** (notes, controllers, tempo)
2) **Symbolic musical facts** (form, harmony, ensemble, motifs, texture, expression)
3) A **semantic ensemble graph** integrated with **DuckDB** for retrieval, reasoning, visualization, and fact‑checking.

It also explains *why the graph matters*—informationally and visually—and how it relates to time‑series and musical **steps** (sections, chords, motifs), beyond simple instrument linking.

---

## 0. High‑Level Architecture

```
        .mid files (raw)
              │
          [Parser]
              │             ┌─────────────── Symbolic Extractors ───────────────┐
              ├──▶ Time‑Series Tables (DuckDB/Parquet):                         │
              │      notes, controllers, tempo_ts, tsigs, keys                  │
              │                                                                 │
              └──▶ Symbolic Facts (DuckDB):                                     │
                     sections, chords(roman), ensemble, motifs, layering, expr  │
                              │                                                 │
                              ├────────────┬───────────────┬────────────────────┘
                              │            │               │
                         Facts Text     Graph Nodes     Graph Edges
                         (RAG chunks)     (DuckDB)        (DuckDB)
                              │            │               │
                              └───── Retriever + Join + Verifier (LLM/tool calls)
                                              │
                                         Chatbot Answers
                                   (bar‑aligned + provenance)
```

---

## 1. Components to Build — **MIDI Time‑Series Event Data**

> Goal: lossless, bar‑aligned, columnar tables for fast filtering, joins, and provenance.

### 1.1 Parsing & Normalization
- **Libraries**: `pretty_midi` + `mido` (for raw CC/pitch‑bend), optional `music21` for harmony.
- **Outputs**: seconds + ticks, bar/beat indices (computed from tempo & time signatures).
- **Stable IDs**: `(file_id, track_id, note_id)` and `(file_id, track_id, tick_range)` for provenance.

### 1.2 Core Tables (DuckDB)
- **files**: file metadata (title, path, duration_s, ppq).
- **tracks**: `(file_id, track_id, program, is_drum, name)`.
- **notes**: `(file_id, track_id, note_id, start_s, end_s, start_tick, end_tick, bar, beat, pitch, velocity)`.
- **controllers**: `(file_id, track_id, cc, time_s, tick, value)` for CC1/7/10/11/64 + pitch‑bend.
- **tempo_ts** (tempo changes): `(file_id, time_s, bpm)`.
- **tsigs** (time signatures): `(file_id, time_s, num, den)`.
- **keys** (key changes): `(file_id, time_s, key)`.

### 1.3 Performance Notes
- **Columnar**: store as Parquet, query via DuckDB; or insert into DuckDB tables directly.
- **Ordering**: write by `(file_id, start_s)` to maximize zone‑map pruning.
- **Downsampling** (optional): resample controllers to beat/16th grid for compactness.

---

## 2. Components to Build — **Symbolic Musical Facts**

> Goal: derived, producer‑relevant facts for arrangement, composition, and analysis.

### 2.1 Harmony
- **Chords** (symbol + roman numerals) with bar spans and harmonic rhythm.
- **Keys & Modulations**; cadence detection (V–I, iv–i, etc.).

### 2.2 Form / Sections
- Segmentation into **intro / verse / chorus / bridge / outro**.
- **Transitions**: fills, pickups, turnarounds—bar markers with types.

### 2.3 Ensemble / Orchestration
- Per track: program, role (lead/pad/bass/percussion), register (low/high), entrance/exit bars.
- **Relations**: doubling, call‑and‑response, counterpoint, divisi.

### 2.4 Motifs & Themes
- Identification via interval/IOI n‑grams; **motif occurrences** with bar/track scopes.
- Transformations: sequence, inversion, augmentation/diminution (optional stage 2).

### 2.5 Texture & Rhythm
- **Layering events**: enter/drop/double/call/answer (bar‑scoped).
- **Density curves**: notes per bar; active tracks per bar.
- Rhythm: swing ratio, syncopation index, polyrhythms/ostinati markers.

### 2.6 Expression
- **Velocity arcs** per section; dynamics summaries.
- **Controller usage summaries**: sustain (CC64), mod (CC1), expression (CC11), pitch‑bend ranges.

### 2.7 Facts Text (for RAG)
- Short, **objective sentences** backed by bar spans and pointers, e.g.:
  - "Bars 24–40 (chorus): strings (prog 48) enter at 24 and sustain pads; chords I–V6–vi–IV."
- Store with provenance: `(file_id, fact_id, start_bar, end_bar, score_path/json_pointer, text)`.

---

## 3. Semantic Ensemble Graph — **Why & What**

**Why the graph matters**
- **Information**: Music reasoning relies on *relations* across time—who plays with whom, when, under which harmony, and how motifs recur. Tables are great for metrics; graphs make **relationships first‑class**.
- **Visuals**: Graphs render naturally into timelines + connection diagrams (e.g., who doubles whom in a chorus), making arrangement structure **legible** to humans.
- **Beyond instrument linking**: Instrument IDs merely join events to tracks; graphs encode **musical steps**—sections, chord spans, motif occurrences—and the relations between them (e.g., *Track A doubles Lead during Chorus*), enabling “why” answers.

### 3.1 Core Node Types (per file)
- **Section**: `{section_id, name, start_bar, end_bar}`
- **Track**: `{track_id, program, is_drum, role, register_low, register_high}`
- **ChordSpan**: `{span_id, start_bar, end_bar, symbol, roman, in_key}`
- **Motif** / **MotifOcc**: motif identity and occurrences `{motif_id, track_id, start_bar, end_bar}`
- **TextureEvent**: `{event_id, type∈{enter,drop,double,call,answer}, bar, track_id}`
- **ControllerSummary**: `{track_id, cc, active_bars/ranges}`
- **KeyChange**, **TempoChange** (optional if needed as nodes)

All nodes carry provenance: `file_id` and pointers to tables (e.g., `(file_id, track_id, note_id)` or bar spans).

### 3.2 Edge Types (time‑scoped)
- `(Track)-[:OCCURS_IN {start_bar,end_bar}]->(Section)`
- `(ChordSpan)-[:OCCURS_IN]->(Section)`
- `(MotifOcc)-[:OCCURS_IN]->(Section)`; `(MotifOcc)-[:PLAYED_BY]->(Track)`
- `(Track)-[:DOUBLES {bars}]->(Track)`; `(Track)-[:CALLS {bars}]->(Track)`; `(Track)-[:ANSWERS {bars}]->(Track)`
- `(ChordSpan)-[:SUPPORTS_HARMONY_OF]->(Section)`
- `(ControllerSummary)-[:CONTROLS {bars,cc}]->(Track)`
- `(Section)-[:FOLLOWS]->(Section)`

Time scopes turn relationships into **musical steps** on the bar grid, enabling precise, explainable answers.

---

## 4. DuckDB Integration — Tables & Views

> Keep everything in DuckDB so retrieval and joins stay simple and fast; store heavy data as Parquet if desired.

### 4.1 DDL — Time‑Series
```sql
CREATE TABLE files (
  file_id TEXT PRIMARY KEY,
  path TEXT NOT NULL,
  title TEXT,
  duration_s DOUBLE,
  ppq INTEGER
);

CREATE TABLE tracks (
  file_id TEXT,
  track_id INTEGER,
  program INTEGER,
  is_drum BOOLEAN,
  name TEXT,
  PRIMARY KEY (file_id, track_id)
);

CREATE TABLE notes (
  file_id TEXT,
  track_id INTEGER,
  note_id INTEGER,
  start_s DOUBLE,
  end_s DOUBLE,
  start_tick BIGINT,
  end_tick BIGINT,
  bar INTEGER,
  beat DOUBLE,
  pitch INTEGER,
  velocity INTEGER,
  PRIMARY KEY (file_id, track_id, note_id)
);

CREATE TABLE controllers (
  file_id TEXT,
  track_id INTEGER,
  cc INTEGER,
  time_s DOUBLE,
  tick BIGINT,
  value INTEGER
);

CREATE TABLE tempo_ts (
  file_id TEXT,
  time_s DOUBLE,
  bpm DOUBLE
);

CREATE TABLE tsigs (
  file_id TEXT,
  time_s DOUBLE,
  num INTEGER,
  den INTEGER
);

CREATE TABLE keys (
  file_id TEXT,
  time_s DOUBLE,
  key TEXT
);
```

### 4.2 DDL — Symbolic Facts
```sql
CREATE TABLE sections (
  file_id TEXT,
  section_id TEXT,
  name TEXT,
  start_bar INTEGER,
  end_bar INTEGER
);

CREATE TABLE chords (
  file_id TEXT,
  span_id TEXT,
  start_bar INTEGER,
  end_bar INTEGER,
  symbol TEXT,
  roman TEXT,
  in_key TEXT
);

CREATE TABLE ensemble (
  file_id TEXT,
  track_id INTEGER,
  role TEXT,
  register_low INTEGER,
  register_high INTEGER,
  enter_bar INTEGER,
  exit_bar INTEGER
);

CREATE TABLE motifs (
  file_id TEXT,
  motif_id TEXT,
  pattern_repr TEXT,
  n_occ INTEGER
);

CREATE TABLE motif_occ (
  file_id TEXT,
  motif_id TEXT,
  track_id INTEGER,
  start_bar INTEGER,
  end_bar INTEGER
);

CREATE TABLE layering (
  file_id TEXT,
  event_id TEXT,
  type TEXT, -- enter, drop, double, call, answer
  bar INTEGER,
  track_id INTEGER
);

CREATE TABLE facts_text (
  file_id TEXT,
  fact_id TEXT,
  start_bar INTEGER,
  end_bar INTEGER,
  score_path TEXT, -- json pointer / graph node id
  text TEXT
);
```

### 4.3 DDL — Graph in DuckDB (Relational Graph)
```sql
CREATE TABLE graph_nodes (
  file_id TEXT,
  node_id TEXT,
  type TEXT,  -- Section, Track, ChordSpan, Motif, MotifOcc, TextureEvent, ControllerSummary
  ref_key TEXT, -- e.g., section_id, track_id, span_id, motif_id, event_id
  payload JSON
);

CREATE TABLE graph_edges (
  file_id TEXT,
  src TEXT,
  rel TEXT,  -- OCCURS_IN, DOUBLES, CALLS, ANSWERS, SUPPORTS_HARMONY_OF, CONTROLS, FOLLOWS, PLAYED_BY
  dst TEXT,
  start_bar INTEGER,
  end_bar INTEGER,
  props JSON
);
```

> **Tip**: You can materialize nodes/edges from the symbolic facts via INSERT‑SELECTs, and also serialize a per‑file JSON graph blob if your chatbot tool prefers JSON.

### 4.4 RAG Chunks View (8‑bar windows)
```sql
CREATE TABLE chunks AS
SELECT
  n.file_id,
  (n.bar/8)*8 AS start_bar,
  (n.bar/8)*8 + 8 AS end_bar,
  MIN(n.start_s) AS start_s,
  MAX(n.end_s)   AS end_s,
  COUNT(*)       AS note_count,
  LIST_DISTINCT(t.program) AS programs
FROM notes n
JOIN tracks t USING (file_id, track_id)
GROUP BY n.file_id, (n.bar/8);
```

---

## 5. Tooling Contracts (for the Chatbot)

### 5.1 Graph Query Tool (examples)
- `graph.query({ file_id, select:"first_entry", filters:{ program:48, section:"chorus" } })`
- `graph.query({ file_id, select:"chords_in_section", filters:{ section:"bridge" } })`
- Returns **facts + provenance**: node/edge ids, bar spans, and pointers back to tables.

### 5.2 Verifier
- Parse claims in model output (regex/JSON) → predicates (`ENTER(track=48, bar=24)`)
- Check against graph_edges/time scopes and/or symbolic facts; auto‑edit mismatches.

### 5.3 Retriever
- Search `facts_text` (BM25 or embeddings) filtered by `file_id` (and bars if present),
- Join to `chunks` / symbolic facts for context payload.

---

## 6. Why the Graph is Crucial (Information & Visuals)

- **Relations as first‑class citizens**: Music meaning lives in *relations over time* (who doubles whom, motif recurrences, harmony under a section). A plain instrument link (track → notes) can’t express *why* events belong together.
- **Musical steps (bar‑scoped edges)**: Time‑scoped relations (e.g., `DOUBLES{24–32}`) align with human concepts of **steps** in form and arrangement. This yields explainable answers ("Brass doubles the lead from bars 24–32").
- **Visual legibility**: Graphs render to layered timelines, call‑and‑response networks, chord support maps—vital for producers.
- **Query power**: Graph + tables let you ask mixed questions: *When the motif returns, what harmony and which instruments play, and is sustain pedal on?* (join edges + facts back to controllers/notes).

> **In short**: Time‑series tables are the *data*, symbolic facts are the *meaning*, and the graph is the *glue* that ties meaning to time, enabling both trustworthy answers and interpretable visualizations.

---

## 7. Minimal ETL Plan (for a Coding Agent)

1. **Parse MIDI** → write `files`, `tracks`, `notes`, `controllers`, `tempo_ts`, `tsigs`, `keys`.
2. **Derive facts** → `sections`, `chords(roman)`, `ensemble`, `layering`, `motifs`, `motif_occ`, `expression summaries`.
3. **Emit facts_text** sentences with bar spans + score_path.
4. **Materialize graph** → fill `graph_nodes`/`graph_edges` from facts.
5. **Build chunks** view; (optional) build embeddings for `facts_text` (DuckDB `vss` or external FAISS).
6. **Expose tools**: `graph.query`, `db.query`, `play_segment` (by bars), `get_chunks`.
7. **Chat loop**: retrieve → answer → verify (against graph) → attach provenance.

---

## 8. Example Queries

- **Strings first enter?**
```sql
SELECT bar
FROM layering L
JOIN tracks T USING (file_id, track_id)
WHERE L.file_id = $file AND L.type='enter' AND T.program BETWEEN 48 AND 51
ORDER BY bar LIMIT 1;
```

- **Chords (roman) in chorus**
```sql
SELECT C.start_bar, C.end_bar, C.symbol, C.roman
FROM sections S JOIN chords C USING (file_id)
WHERE S.file_id=$file AND S.name='chorus'
  AND C.start_bar>=S.start_bar AND C.end_bar<=S.end_bar
ORDER BY C.start_bar;
```

- **Graph: who doubles the lead in chorus?**
```sql
SELECT E.src AS doubler, E.dst AS lead, E.start_bar, E.end_bar
FROM graph_edges E
JOIN graph_nodes NS ON NS.node_id=E.src
JOIN graph_nodes ND ON ND.node_id=E.dst
WHERE E.file_id=$file AND E.rel='DOUBLES'
  AND E.start_bar>= (SELECT start_bar FROM sections WHERE file_id=$file AND name='chorus')
  AND E.end_bar  <= (SELECT end_bar   FROM sections WHERE file_id=$file AND name='chorus');
```

---

## 9. JSON Shapes (Snippet)

**Instrument/Track (shared across layers)**
```json
{"file_id":"song_001","track_id":2,"program":48,"is_drum":false,
 "role":"pad","register_low":50,"register_high":72,
 "enter_bar":24,"exit_bar":40}
```

**Graph Node/Edge**
```json
{"node_id":"trk:strings","type":"Track","ref_key":"track_id:2","payload":{...}}
{"src":"trk:strings","rel":"OCCURS_IN","dst":"sec:chorus","start_bar":24,"end_bar":40}
```

**Facts Text**
```json
{"file_id":"song_001","fact_id":"f123","start_bar":24,"end_bar":40,
 "score_path":"/sections[chorus]","text":"Bars 24–40 chorus; strings (prog 48) enter at 24; I–V6–vi–IV."}
```

---

## 10. Roadmap
- v0: Parsing, core tables, chords/sections, basic graph, facts_text + RAG.
- v1: Motif mining, texture relations, controller summaries, verifier loop.
- v2: Visuals (timeline, doubling graph, motif recurrence map), DuckDB `vss` integration.
- v3: Preference‑tuned describer (LoRA SFT) + strict fact‑checking against graph.

---

**Contact / Contributing**
- Add unit tests for each extractor (notes, harmony, sections, motifs).
- Lint SQL and ensure idempotent ETL (safe re‑runs).
- Keep strict provenance: every fact or edge must map back to bars and track IDs.

