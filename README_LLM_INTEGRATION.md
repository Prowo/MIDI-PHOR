# MIDIPHOR LLM Integration for Musical Composition

This document explains how to use MIDIPHOR's ScoreSpec data with Large Language Models (LLMs) for musical composition tasks.

## Overview

MIDIPHOR extracts **factual, verifiable musical information** from MIDI files and creates structured data that LLMs can use for:
- Musical composition and arrangement
- Harmonic analysis and development
- Instrumentation and orchestration
- Musical structure analysis
- Interactive composition sessions

## Key Components

### 1. Enhanced Facts Extractor (`enhanced_facts_extractor.py`)

Extracts comprehensive musical facts from ScoreSpec JSON files:

```python
from enhanced_facts_extractor import EnhancedFactsExtractor

# Load and analyze a ScoreSpec file
extractor = EnhancedFactsExtractor("scorespec_json/Dancing Queen.scorespec.json")

# Generate all musical facts
facts = extractor.generate_all_facts()

# Save facts to file
extractor.save_facts_to_file("musical_facts.txt")

# Get LLM prompt context
context = extractor.get_llm_prompt_context()
```

**What it extracts:**
- **Global facts**: Time signature, tempo, meter
- **Structure facts**: Section count, bar lengths, form analysis
- **Instrumentation facts**: Track count, program families, register ranges
- **Harmonic facts**: Chord types, pitch classes, harmonic rhythm
- **Controller facts**: CC usage, expression data
- **Graph facts**: Musical relationships, node/edge counts
- **Compositional facts**: Complexity assessment, ensemble size

### 2. LLM Composition Prompter (`llm_composition_prompter.py`)

Creates specialized prompts for different musical composition tasks:

```python
from llm_composition_prompter import LLMCompositionPrompter

# Initialize prompter
prompter = LLMCompositionPrompter("scorespec_json/Dancing Queen.scorespec.json")

# Create different types of prompts
extend_prompt = prompter.create_composition_prompt("extend", target_bars=16, style="similar")
arrange_prompt = prompter.create_composition_prompt("arrange", section="chorus", focus="dynamics")
harmonize_prompt = prompter.create_composition_prompt("harmonize", melody_notes=[60, 62, 64, 65], style="jazz")
orchestrate_prompt = prompter.create_composition_prompt("orchestrate", target_instruments=["strings", "brass"], style="classical")
analysis_prompt = prompter.create_composition_prompt("analyze", focus_area="harmonic structure")
interactive_prompt = prompter.create_interactive_session()

# Save prompts to files
prompter.save_prompt_to_file(extend_prompt, "extend_prompt.txt")
```

## Available Prompt Types

### 1. **Extend** - Compose new sections
- Maintains musical consistency with existing piece
- Suggests harmonic frameworks and melodic ideas
- Provides arrangement and performance notes

### 2. **Arrange** - Rearrange existing sections
- Focuses on dynamics, texture, or layering
- Uses existing instrumental palette
- Suggests performance directions

### 3. **Harmonize** - Add harmony to melodies
- Uses piece's harmonic language
- Follows established harmonic rhythm
- Considers instrumental ranges and voice leading

### 4. **Orchestrate** - Instrumentation for specific ensembles
- Works within harmonic framework
- Creates balanced textures
- Maintains musical coherence

### 5. **Analyze** - Deep musical analysis
- Identifies patterns and relationships
- Explains musical significance
- Provides theoretical insights

### 6. **Interactive** - Collaborative composition session
- Two-way musical conversation
- Problem-solving for composition challenges
- Suggests alternatives and variations

## Example Usage

### Basic Facts Extraction
```bash
# Extract facts from a ScoreSpec file
python enhanced_facts_extractor.py

# This will:
# 1. Load "Dancing Queen.scorespec.json"
# 2. Generate comprehensive musical facts
# 3. Save facts to "dancing_queen_facts.txt"
# 4. Print LLM prompt context
```

### Generate Composition Prompts
```bash
# Create various composition prompts
python llm_composition_prompter.py

# This will create:
# - dancing_queen_extend_prompt.txt
# - dancing_queen_arrange_prompt.txt
# - dancing_queen_analysis_prompt.txt
# - dancing_queen_interactive_prompt.txt
```

## What the Facts Actually Extract

The system extracts **objective, verifiable information** from the ScoreSpec data:

### From "Dancing Queen" ScoreSpec:
- **Piece is in 4/4 time signature** (from `global.meter`)
- **Tempo is approximately 105 BPM** (from `global.approx_bpm`)
- **Piece has 12 sections spanning 96 bars** (from `segments` array)
- **All sections are 8 bars long** (calculated from section data)
- **Piece uses 11 instrument tracks** (from `instruments` array)
- **Uses 5 unique MIDI programs** (calculated from program numbers)
- **Has 7 piano instrument(s)** (categorized from MIDI program numbers)
- **Total pitch range spans 72 semitones** (calculated from register data)
- **Piece has 89 harmonic sections** (from `pitch_class_spans` array)
- **Uses 12 unique pitch classes** (calculated from pitch class data)
- **Common chord types: major triad, diminished triad, minor triad** (identified from pitch class sets)
- **Average harmonic rhythm: 1.1 bars per chord** (calculated from span lengths)

## LLM Integration Workflow

### 1. **Load ScoreSpec Data**
```python
# The ScoreSpec contains structured musical information
scorespec = {
    "file_id": "Dancing Queen",
    "global": {"meter": "4/4", "approx_bpm": 105},
    "segments": [...],
    "instruments": [...],
    "pitch_class_spans": [...],
    "controllers": {...},
    "graph": {"nodes": [...], "edges": [...]}
}
```

### 2. **Extract Factual Information**
```python
# Convert structured data to human-readable facts
facts = [
    "Piece is in 4/4 time signature",
    "Tempo is approximately 105 BPM",
    "Piece has 12 sections spanning 96 bars",
    # ... more facts
]
```

### 3. **Create Specialized Prompts**
```python
# Generate task-specific prompts with musical context
prompt = f"""You are a musical composition assistant. 
Based on the following analysis of '{piece_name}':

MUSICAL CONTEXT:
{chr(10).join(f"• {fact}" for fact in facts)}

TASK: {specific_task}
REQUIREMENTS: {requirements}
"""
```

### 4. **LLM Response**
The LLM can now provide informed musical suggestions based on:
- **Factual musical data** (not subjective opinions)
- **Specific piece characteristics** (tempo, harmony, instrumentation)
- **Musical relationships** (from graph data)
- **Performance context** (controller usage, register ranges)

## Benefits for Musical Composition

### 1. **Factual Grounding**
- Every musical statement is based on actual MIDI data
- No subjective interpretations or "musical opinions"
- Clear provenance for all musical facts

### 2. **Musical Context**
- LLMs understand the piece's specific characteristics
- Suggestions are tailored to the piece's style and structure
- Harmonic and rhythmic patterns are preserved

### 3. **Technical Precision**
- Bar-level precision for all musical events
- Specific instrumental capabilities and ranges
- Controller usage and expression data

### 4. **Compositional Guidance**
- Harmonic frameworks based on actual chord progressions
- Instrumental suggestions based on available tracks
- Structural extensions that maintain musical flow

## Example LLM Interaction

**User**: "Help me extend this piece with a new 16-bar section"

**LLM** (with ScoreSpec context): "Based on the analysis of 'Dancing Queen', I can help you create a 16-bar extension. The piece is in 4/4 time at 105 BPM, uses 11 instrument tracks, and has a harmonic rhythm of about 1.1 bars per chord change.

For the extension, I suggest:
1. **Harmonic Framework**: Use the common chord types found in the piece (major, minor, and diminished triads)
2. **Melodic Development**: The piece uses 12 unique pitch classes, so you have harmonic flexibility
3. **Instrumental Arrangement**: Follow the pattern of 7 piano tracks, 1 string track, and 2 synth tracks
4. **Structure**: Since all sections are 8 bars, consider dividing the 16 bars into two 8-bar phrases

Would you like me to suggest specific chord progressions or melodic ideas?"

## File Structure

```
MIDIPHOR/
├── enhanced_facts_extractor.py      # Extract musical facts from ScoreSpec
├── llm_composition_prompter.py     # Generate LLM prompts for composition
├── scorespec_json/                  # ScoreSpec JSON files
│   └── Dancing Queen.scorespec.json
├── dancing_queen_facts.txt          # Generated musical facts
├── dancing_queen_extend_prompt.txt  # Extension composition prompt
├── dancing_queen_arrange_prompt.txt # Arrangement prompt
├── dancing_queen_analysis_prompt.txt # Analysis prompt
└── dancing_queen_interactive_prompt.txt # Interactive session prompt
```

## Next Steps

1. **Test with different ScoreSpec files** to ensure robustness
2. **Integrate with specific LLM APIs** (OpenAI, Claude, etc.)
3. **Add more specialized prompt types** for specific musical tasks
4. **Create feedback loops** where LLM suggestions can be validated against musical theory
5. **Build interactive composition tools** that use this factual foundation

## Why This Approach Works

The key insight is that **musical composition requires factual understanding**, not subjective interpretation. By extracting objective facts from MIDI data:

- **LLMs get real musical context** instead of generic advice
- **Suggestions are musically coherent** with the existing piece
- **Composition decisions are grounded** in actual musical data
- **The creative process is informed** by technical understanding

This creates a powerful partnership where human creativity is enhanced by AI's ability to process and analyze large amounts of musical data, while maintaining the artistic integrity and musical coherence of the composition.
