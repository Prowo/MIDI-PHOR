# MIDIPHOR Database Architecture & ScoreSpec Output

## Overview
MIDIPHOR processes MIDI files through a pipeline that creates a comprehensive DuckDB database with three main data layers:
1. **Symbolic Representation** - Musical structure and analysis
2. **Time Series** - Raw MIDI events and temporal data  
3. **Graph Linkage** - Relationships between musical elements

The system then exports a ScoreSpec JSON that provides a structured view of the musical content.

---

## 1. DuckDB Database Schema

### 1.1 Symbolic Representation Tables

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SYMBOLIC TABLES                               │
├─────────────────────────────────────────────────────────────────────────────┤
│ sections                                                                   │
│ ├── file_id (TEXT)                                                        │
│ ├── section_id (TEXT)                                                     │
│ ├── name (TEXT)                                                           │
│ ├── start_bar (INTEGER)                                                   │
│ └── end_bar (INTEGER)                                                     │
│                                                                             │
│ pitch_class_spans                                                          │
│ ├── file_id (TEXT)                                                        │
│ ├── span_id (TEXT)                                                        │
│ ├── start_bar (INTEGER)                                                   │
│ ├── end_bar (INTEGER)                                                     │
│ └── pcs (TEXT) - JSON array of pitch classes                              │
│                                                                             │
│ ensemble                                                                   │
│ ├── file_id (TEXT)                                                        │
│ ├── track_id (INTEGER)                                                    │
│ ├── register_low (INTEGER)                                                │
│ ├── register_high (INTEGER)                                               │
│ ├── enter_bar (INTEGER)                                                   │
│ └── exit_bar (INTEGER)                                                    │
│                                                                             │
│ motifs                                                                     │
│ ├── file_id (TEXT)                                                        │
│ ├── motif_id (TEXT)                                                       │
│ ├── pattern_repr (TEXT)                                                   │
│ └── n_occ (INTEGER)                                                       │
│                                                                             │
│ motif_occ                                                                  │
│ ├── file_id (TEXT)                                                        │
│ ├── motif_id (TEXT)                                                       │
│ ├── track_id (INTEGER)                                                    │
│ ├── start_bar (INTEGER)                                                   │
│ └── end_bar (INTEGER)                                                     │
│                                                                             │
│ layering                                                                   │
│ ├── file_id (TEXT)                                                        │
│ ├── event_id (TEXT)                                                       │
│ ├── type (TEXT) - enter/exit                                              │
│ ├── bar (INTEGER)                                                         │
│ └── track_id (INTEGER)                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Time Series Tables

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              TIME SERIES TABLES                            │
├─────────────────────────────────────────────────────────────────────────────┤
│ files                                                                      │
│ ├── file_id (TEXT) PRIMARY KEY                                            │
│ ├── path (TEXT)                                                           │
│ ├── title (TEXT)                                                          │
│ ├── duration_s (DOUBLE)                                                   │
│ └── ppq (INTEGER) - pulses per quarter                                    │
│                                                                             │
│ tracks                                                                     │
│ ├── file_id (TEXT)                                                        │
│ ├── track_id (INTEGER)                                                    │
│ ├── program (INTEGER) - MIDI program number                               │
│ ├── is_drum (BOOLEAN)                                                     │
│ └── name (TEXT)                                                           │
│                                                                             │
│ notes                                                                      │
│ ├── file_id (TEXT)                                                        │
│ ├── track_id (INTEGER)                                                    │
│ ├── note_id (INTEGER)                                                     │
│ ├── start_s (DOUBLE) - start time in seconds                              │
│ ├── end_s (DOUBLE) - end time in seconds                                  │
│ ├── start_tick (BIGINT)                                                   │
│ ├── end_tick (BIGINT)                                                     │
│ ├── bar (INTEGER) - computed bar number                                   │
│ ├── beat (DOUBLE) - beat within bar                                        │
│ ├── pitch (INTEGER) - MIDI pitch (0-127)                                  │
│ └── velocity (INTEGER) - note velocity (0-127)                            │
│                                                                             │
│ controllers                                                                │
│ ├── file_id (TEXT)                                                        │
│ ├── track_id (INTEGER)                                                    │
│ ├── cc (INTEGER) - controller number                                      │
│ ├── time_s (DOUBLE) - time in seconds                                     │
│ ├── tick (BIGINT)                                                         │
│ └── value (INTEGER) - controller value                                    │
│                                                                             │
│ tempo_ts                                                                   │
│ ├── file_id (TEXT)                                                        │
│ ├── time_s (DOUBLE) - time in seconds                                     │
│ └── bpm (DOUBLE) - tempo in BPM                                           │
│                                                                             │
│ tsigs                                                                      │
│ ├── file_id (TEXT)                                                        │
│ ├── time_s (DOUBLE) - time in seconds                                     │
│ ├── num (INTEGER) - time signature numerator                              │
│ └── den (INTEGER) - time signature denominator                            │
│                                                                             │
│ keys                                                                       │
│ ├── file_id (TEXT)                                                        │
│ ├── time_s (DOUBLE) - time in seconds                                     │
│ └── key (TEXT) - key signature                                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.3 Graph Linkage Tables

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              GRAPH TABLES                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ graph_nodes                                                                │
│ ├── file_id (TEXT)                                                        │
│ ├── node_id (TEXT)                                                        │
│ ├── type (TEXT) - Section/Track/PitchClassSpan/Motif/MotifOcc            │
│ ├── ref_key (TEXT) - reference to source table                            │
│ └── payload (JSON) - node-specific data                                   │
│                                                                             │
│ graph_edges                                                                │
│ ├── file_id (TEXT)                                                        │
│ ├── src (TEXT) - source node ID                                           │
│ ├── rel (TEXT) - relationship type                                        │
│ ├── dst (TEXT) - destination node ID                                      │
│ ├── start_bar (INTEGER) - relationship start                              │
│ ├── end_bar (INTEGER) - relationship end                                  │
│ └── props (JSON) - relationship properties                                │
│                                                                             │
│ facts_text                                                                 │
│ ├── file_id (TEXT)                                                        │
│ ├── fact_id (TEXT)                                                        │
│ ├── start_bar (INTEGER)                                                   │
│ ├── end_bar (INTEGER)                                                     │
│ ├── score_path (TEXT) - path to source data                               │
│ └── text (TEXT) - human-readable fact                                    │
│                                                                             │
│ descriptions                                                               │
│ ├── file_id (TEXT) PRIMARY KEY                                            │
│ ├── description (TEXT) - full piece description                           │
│ └── claims (JSON) - array of claims with evidence                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Data Flow Pipeline

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   MIDI      │───▶│  Time      │───▶│  Symbolic   │───▶│   Graph     │
│   Files     │    │  Series    │    │  Facts      │    │  Creation   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                           │                   │                   │
                           ▼                   ▼                   ▼
                    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
                    │ notes       │    │ sections   │    │ graph_nodes │
                    │ controllers │    │ ensemble   │    │ graph_edges │
                    │ tempo_ts    │    │ motifs     │    │             │
                    │ tracks      │    │ pitch_class│    │ _spans     │
                    └─────────────┘    └─────────────┘    └─────────────┘
```

---

## 3. Graph Relationship Types

### 3.1 Edge Types
```
OCCURS_IN          - Track/Section relationship
PLAYED_BY          - Motif occurrence to Track
DOUBLES            - Track to Track (same notes)
SUPPORTS_HARMONY_OF - Pitch class span to Section
CONTROLS           - Controller to Track
FOLLOWS            - Section to Section
```

### 3.2 Node Types
```
Section            - Musical sections (segments)
Track              - MIDI tracks with metadata
PitchClassSpan     - Harmonic content over time
Motif              - Recurring musical patterns
MotifOcc           - Specific motif occurrences
```

---

## 4. ScoreSpec JSON Output Structure

The ScoreSpec JSON provides a structured, queryable representation of the musical content:

```json
{
  "file_id": "Dancing Queen",
  "global": {
    "meter": "4/4",
    "approx_bpm": 105
  },
  "segments": [
    {
      "id": "seg_0",
      "bars": [0, 8]
    }
    // ... more segments
  ],
  "instruments": [
    {
      "track_id": 0,
      "program": 85,
      "register": {"low": 57, "high": 74},
      "enter_bar": 8,
      "exit_bar": 85
    }
    // ... more instruments
  ],
  "pitch_class_spans": [
    {
      "bars": [0, 1],
      "pcs": [0, 2, 4, 5, 7, 9, 11]
    }
    // ... more harmonic spans
  ],
  "controllers": {
    "1": {"present": true, "count": 16},   // Modulation
    "7": {"present": true, "count": 208},  // Volume
    "10": {"present": true, "count": 27},  // Pan
    "11": {"present": true, "count": 16},  // Expression
    "64": {"present": true, "count": 2}    // Sustain
  },
  "graph": {
    "nodes": [...],
    "edges": [...]
  },
  "provenance": {
    "tables": ["sections", "tracks", "notes", "pitch_class_spans", "controllers"]
  }
}
```

---

## 5. Key Features

### 5.1 Symbolic Analysis
- **Automatic segmentation** into 8-bar sections
- **Pitch class analysis** for harmonic content
- **Motif detection** via repeated pitch patterns
- **Ensemble tracking** with entrance/exit points

### 5.2 Time Series Data
- **Bar-aligned** note and controller data
- **Tempo and time signature** changes
- **Lossless MIDI** event preservation
- **Fast columnar** querying via DuckDB

### 5.3 Graph Relationships
- **Musical structure** as connected nodes
- **Temporal relationships** with bar scoping
- **Provenance tracking** back to source data
- **Queryable relationships** for analysis

### 5.4 ScoreSpec Benefits
- **Structured output** for LLM consumption
- **Musical metadata** in human-readable format
- **Graph integration** for relationship queries
- **Provenance tracking** for fact verification

---

## 6. Example Queries

### 6.1 Symbolic Analysis
```sql
-- Find all sections with specific pitch classes
SELECT s.name, pcs.start_bar, pcs.end_bar, pcs.pcs
FROM sections s
JOIN pitch_class_spans pcs USING (file_id)
WHERE s.file_id = 'Dancing Queen' 
  AND pcs.pcs LIKE '%0,4,7%';  -- C major triad
```

### 6.2 Time Series
```sql
-- Find notes in specific bars
SELECT n.pitch, n.velocity, t.program
FROM notes n
JOIN tracks t USING (file_id, track_id)
WHERE n.file_id = 'Dancing Queen' 
  AND n.bar BETWEEN 24 AND 32;
```

### 6.3 Graph Relationships
```sql
-- Find tracks that double each other
SELECT e.src, e.dst, e.start_bar, e.end_bar
FROM graph_edges e
WHERE e.file_id = 'Dancing Queen' 
  AND e.rel = 'DOUBLES';
```

---

## 7. Use Cases

1. **Music Analysis** - Harmonic, rhythmic, and structural analysis
2. **Arrangement** - Understanding instrument roles and layering
3. **Composition** - Pattern recognition and motif development
4. **Education** - Structured musical content for learning
5. **AI/ML** - Training data for musical understanding models
6. **Research** - Large-scale musical pattern analysis

---

## 8. Technical Benefits

- **DuckDB integration** for fast analytical queries
- **Columnar storage** for efficient data access
- **Graph relationships** for complex musical analysis
- **Provenance tracking** for data lineage
- **JSON export** for external tool integration
- **Scalable architecture** for large MIDI collections
