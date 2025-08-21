# MIDIPHOR Visual Data Flow & Relationships

## 1. High-Level System Architecture

```
                    MIDI FILES
                         │
                         ▼
                ┌─────────────────┐
                │   PARSER        │
                │ (pretty_midi +  │
                │   mido)         │
                └─────────────────┘
                         │
                         ▼
        ┌─────────────────────────────────────┐
        │           DUCKDB                   │
        │      (Columnar Database)           │
        └─────────────────────────────────────┘
                         │
                         ▼
                ┌─────────────────┐
                │  SCORESPEC      │
                │     JSON        │
                └─────────────────┘
```

## 2. Three-Layer Data Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SYMBOLIC REPRESENTATION                          │
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │  sections   │    │ ensemble   │    │  motifs     │    │pitch_class  │  │
│  │             │    │             │    │             │    │   spans     │  │
│  │ • seg_0     │    │ • track_id  │    │ • pattern   │    │ • bars      │  │
│  │ • bars 0-8  │    │ • register  │    │ • n_occ     │    │ • pcs       │  │
│  │ • name      │    │ • enter/exit│    │ • track_id  │    │ • span_id   │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              TIME SERIES                                   │
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │    files    │    │   tracks    │    │    notes    │    │controllers  │  │
│  │             │    │             │    │             │    │             │  │
│  │ • file_id   │    │ • track_id  │    │ • pitch     │    │ • cc        │  │
│  │ • duration  │    │ • program   │    │ • velocity  │    │ • value     │  │
│  │ • ppq       │    │ • is_drum   │    │ • start_s   │    │ • time_s    │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐                                        │
│  │  tempo_ts   │    │   tsigs     │                                        │
│  │             │    │             │                                        │
│  │ • time_s    │    │ • num/den   │                                        │
│  │ • bpm       │    │ • time_s    │                                        │
│  └─────────────┘    └─────────────┘                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              GRAPH LINKAGE                                 │
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │graph_nodes  │    │graph_edges │    │ facts_text  │    │descriptions │  │
│  │             │    │             │    │             │    │             │  │
│  │ • node_id   │    │ • src/dst   │    │ • fact_id   │    │ • file_id   │  │
│  │ • type      │    │ • rel       │    │ • text      │    │ • desc      │  │
│  │ • payload   │    │ • bars      │    │ • bars      │    │ • claims    │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 3. Data Flow Relationships

```
                    ┌─────────────────────────────────────┐
                    │           MIDI INPUT                │
                    │      (Dancing Queen.mid)            │
                    └─────────────────────────────────────┘
                                    │
                                    ▼
                    ┌─────────────────────────────────────┐
                    │         TIME SERIES                 │
                    │      (Raw MIDI Events)              │
                    │                                     │
                    │  notes: 1,847 notes                 │
                    │  controllers: 269 CC messages        │
                    │  tempo: 105 BPM                     │
                    │  duration: ~3.5 minutes             │
                    └─────────────────────────────────────┘
                                    │
                                    ▼
                    ┌─────────────────────────────────────┐
                    │       SYMBOLIC ANALYSIS             │
                    │     (Musical Structure)             │
                    │                                     │
                    │  sections: 12 segments (8 bars)     │
                    │  pitch_classes: 96 spans            │
                    │  ensemble: 11 instruments           │
                    │  motifs: repeated patterns          │
                    └─────────────────────────────────────┘
                                    │
                                    ▼
                    ┌─────────────────────────────────────┐
                    │         GRAPH CREATION              │
                    │      (Relationships)                │
                    │                                     │
                    │  nodes: sections, tracks, motifs    │
                    │  edges: OCCURS_IN, DOUBLES, etc.    │
                    │  facts: human-readable statements   │
                    └─────────────────────────────────────┘
                                    │
                                    ▼
                    ┌─────────────────────────────────────┐
                    │        SCORESPEC OUTPUT             │
                    │      (Structured JSON)              │
                    │                                     │
                    │  • Global metadata                  │
                    │  • Segments with bar ranges         │
                    │  • Instruments with timing          │
                    │  • Harmonic content                 │
                    │  • Controller usage                 │
                    │  • Graph nodes and edges            │
                    └─────────────────────────────────────┘
```

## 4. Key Relationships Between Tables

### 4.1 Symbolic ↔ Time Series
```
sections.start_bar/end_bar ←→ notes.bar
ensemble.track_id ←→ tracks.track_id
pitch_class_spans.bars ←→ notes.bar (aggregated)
motif_occ.track_id ←→ tracks.track_id
```

### 4.2 Time Series ↔ Graph
```
graph_nodes.ref_key ←→ table references
graph_edges.bars ←→ temporal scope
facts_text.bars ←→ bar ranges
```

### 4.3 Cross-Layer Dependencies
```
notes → sections (via bar calculation)
controllers → ensemble (via track_id)
tempo_ts → notes (via time conversion)
tsigs → sections (via bar calculation)
```

## 5. Example Data Flow for "Dancing Queen"

```
1. MIDI File (Dancing Queen.mid)
   ↓
2. Time Series Extraction:
   - 1,847 notes across 11 tracks
   - 269 controller messages
   - 105 BPM tempo
   - 4/4 time signature
   ↓
3. Symbolic Analysis:
   - 12 sections (0-95 bars, 8-bar windows)
   - 96 pitch class spans
   - 11 instruments with register/entrance data
   - Motif patterns (repeated pitch bigrams)
   ↓
4. Graph Creation:
   - 35+ graph nodes (sections, tracks, motifs)
   - 50+ graph edges (relationships)
   - 25+ fact statements
   ↓
5. ScoreSpec JSON:
   - Structured musical representation
   - Queryable relationships
   - Provenance tracking
```

## 6. Query Patterns

### 6.1 Symbolic + Time Series
```sql
-- Find all notes in a specific section
SELECT n.*, t.program
FROM notes n
JOIN tracks t USING (file_id, track_id)
JOIN sections s USING (file_id)
WHERE s.name = 'seg_0' 
  AND n.bar >= s.start_bar 
  AND n.bar < s.end_bar;
```

### 6.2 Graph + Symbolic
```sql
-- Find tracks that play in the same section
SELECT e.src, e.dst, e.start_bar, e.end_bar
FROM graph_edges e
JOIN graph_nodes n1 ON e.src = n1.node_id
JOIN graph_nodes n2 ON e.dst = n2.node_id
WHERE e.rel = 'OCCURS_IN' 
  AND n1.type = 'Track' 
  AND n2.type = 'Section';
```

### 6.3 Cross-Layer Analysis
```sql
-- Harmonic analysis with timing
SELECT pcs.pcs, pcs.start_bar, pcs.end_bar,
       COUNT(n.note_id) as note_count
FROM pitch_class_spans pcs
JOIN notes n ON n.bar >= pcs.start_bar 
             AND n.bar < pcs.end_bar
WHERE pcs.file_id = 'Dancing Queen'
GROUP BY pcs.span_id, pcs.pcs, pcs.start_bar, pcs.end_bar;
```

## 7. Benefits of This Architecture

1. **Separation of Concerns**: Raw data, analysis, and relationships are clearly separated
2. **Query Flexibility**: Can query at any level (raw events, symbolic facts, or relationships)
3. **Scalability**: Columnar storage for large MIDI collections
4. **Provenance**: Every fact can be traced back to source data
5. **Integration**: Graph structure enables complex musical analysis
6. **Export**: ScoreSpec provides structured output for external tools
