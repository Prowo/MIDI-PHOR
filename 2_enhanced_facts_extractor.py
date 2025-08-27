#!/usr/bin/env python3
"""
Enhanced Facts Extractor for MIDIPHOR
Creates comprehensive, LLM-ready musical facts from ScoreSpec data
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Any, Tuple
import duckdb

from scorespec_db import build_scorespec


class EnhancedFactsExtractor:
    """Extracts comprehensive musical facts from ScoreSpec data for LLM consumption"""
    
    def __init__(self, scorespec_input):
        """Initialize with a ScoreSpec JSON file or dict"""
        if isinstance(scorespec_input, dict):
            self.scorespec = scorespec_input
        else:
            self.scorespec = self._load_scorespec(Path(scorespec_input))
        self.facts = []
        
    def _load_scorespec(self, path: Path) -> Dict[str, Any]:
        with open(path, 'r') as f:
            return json.load(f)
    
    def extract_global_facts(self) -> List[str]:
        facts = []
        global_info = self.scorespec.get('global', {})
        if global_info.get('meter'):
            facts.append(f"Piece is in {global_info['meter']} time signature")
        if global_info.get('approx_bpm'):
            facts.append(f"Tempo is approximately {global_info['approx_bpm']} BPM")
        return facts
    
    def extract_structure_facts(self) -> List[str]:
        facts = []
        segments = self.scorespec.get('segments', [])
        if segments:
            total_bars = segments[-1]['bars'][1]
            facts.append(f"Piece has {len(segments)} sections spanning {total_bars} bars")
            section_lengths = [seg['bars'][1] - seg['bars'][0] for seg in segments]
            if len(set(section_lengths)) == 1:
                facts.append(f"All sections are {section_lengths[0]} bars long")
            else:
                facts.append(f"Section lengths vary: {min(section_lengths)}-{max(section_lengths)} bars")
            if len(segments) >= 4:
                facts.append("Piece appears to have a multi-section form structure")
        return facts
    
    def extract_instrumentation_facts(self) -> List[str]:
        facts = []
        instruments = self.scorespec.get('instruments', [])
        if instruments:
            facts.append(f"Piece uses {len(instruments)} instrument tracks")
            programs = [inst.get('program') for inst in instruments]
            unique_programs = len(set(programs))
            facts.append(f"Uses {unique_programs} unique MIDI programs")
        return facts
    
    def _identify_common_chords(self, pitch_spans: List[Dict[str, Any]]) -> List[str]:
        chord_names = []
        for span in pitch_spans:
            pcs = set(span.get('pcs', []))
            if {0, 4, 7}.issubset(pcs):
                chord_names.append("major triad")
            elif {0, 3, 7}.issubset(pcs):
                chord_names.append("minor triad")
            elif {0, 3, 6}.issubset(pcs):
                chord_names.append("diminished triad")
            elif {0, 4, 7, 10}.issubset(pcs):
                chord_names.append("dominant 7th")
            elif {0, 4, 7, 11}.issubset(pcs):
                chord_names.append("major 7th")
            elif {0, 3, 7, 10}.issubset(pcs):
                chord_names.append("minor 7th")
        return list(set(chord_names))[:3]
    
    def extract_harmonic_facts(self) -> List[str]:
        facts = []
        pitch_spans = self.scorespec.get('pitch_class_spans', [])
        if pitch_spans:
            facts.append(f"Piece has {len(pitch_spans)} harmonic sections")
            all_pcs = []
            for span in pitch_spans:
                all_pcs.extend(span.get('pcs', []))
            unique_pcs = len(set(all_pcs))
            facts.append(f"Uses {unique_pcs} unique pitch classes")
            avg_span_length = sum(span['bars'][1] - span['bars'][0] for span in pitch_spans) / len(pitch_spans)
            facts.append(f"Average harmonic rhythm: {avg_span_length:.1f} bars per chord")
            common_chords = self._identify_common_chords(pitch_spans)
            if common_chords:
                facts.append(f"Common chord types: {', '.join(common_chords)}")
        return facts
    
    def extract_controller_facts(self) -> List[str]:
        return []
    
    def extract_graph_facts(self) -> List[str]:
        facts = []
        graph = self.scorespec.get('graph', {})
        nodes = graph.get('nodes', [])
        edges = graph.get('edges', [])
        if nodes:
            facts.append(f"Musical structure contains {len(nodes)} elements")
        if edges:
            facts.append(f"Contains {len(edges)} musical relationships")
            rel_types = {}
            for edge in edges:
                rel_type = edge.get('rel', 'Unknown')
                rel_types[rel_type] = rel_types.get(rel_type, 0) + 1
            for rel_type, count in rel_types.items():
                facts.append(f"Has {count} {rel_type} relationships")
        return facts
    
    def extract_compositional_facts(self) -> List[str]:
        facts = []
        segments = self.scorespec.get('segments', [])
        instruments = self.scorespec.get('instruments', [])
        pitch_spans = self.scorespec.get('pitch_class_spans', [])
        if segments and instruments:
            total_bars = segments[-1]['bars'][1]
            if total_bars > 64:
                facts.append("Piece is substantial in length (over 64 bars)")
            elif total_bars > 32:
                facts.append("Piece is moderate in length (32-64 bars)")
            else:
                facts.append("Piece is concise (under 32 bars)")
            if len(instruments) > 8:
                facts.append("Piece has a large ensemble")
            elif len(instruments) > 4:
                facts.append("Piece has a medium-sized ensemble")
            else:
                facts.append("Piece has a small ensemble")
            if pitch_spans:
                avg_pcs_per_span = sum(len(span.get('pcs', [])) for span in pitch_spans) / len(pitch_spans)
                if avg_pcs_per_span > 8:
                    facts.append("Piece uses harmonically complex chord structures")
                elif avg_pcs_per_span > 5:
                    facts.append("Piece uses moderately complex harmonies")
                else:
                    facts.append("Piece uses simple harmonic structures")
        return facts
    
    def generate_all_facts(self) -> List[str]:
        all_facts = []
        all_facts.extend(self.extract_global_facts())
        all_facts.extend(self.extract_structure_facts())
        all_facts.extend(self.extract_instrumentation_facts())
        all_facts.extend(self.extract_harmonic_facts())
        all_facts.extend(self.extract_controller_facts())
        all_facts.extend(self.extract_graph_facts())
        all_facts.extend(self.extract_compositional_facts())
        return all_facts
    
    def save_facts_to_file(self, output_path: str):
        facts = self.generate_all_facts()
        with open(output_path, 'w') as f:
            f.write(f"Musical Facts for: {self.scorespec.get('file_id', 'Unknown Piece')}\n")
            f.write("=" * 60 + "\n\n")
            for i, fact in enumerate(facts, 1):
                f.write(f"{i}. {fact}\n")
            f.write(f"\nTotal Facts: {len(facts)}\n")
    
    def get_llm_prompt_context(self) -> str:
        facts = self.generate_all_facts()
        context = f"Based on the following musical analysis of '{self.scorespec.get('file_id', 'Unknown Piece')}':\n\n"
        for fact in facts:
            context += f"• {fact}\n"
        context += "\nPlease use this information to help with musical composition tasks."
        return context


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=None)
    ap.add_argument('--song_id', default=None)
    ap.add_argument('--in_file', default=None)
    ap.add_argument('--out', default=None)
    args = ap.parse_args()

    if args.db and args.song_id:
        con = duckdb.connect(args.db)
        ss = build_scorespec(con, args.song_id)
        extractor = EnhancedFactsExtractor(ss)
    elif args.in_file:
        extractor = EnhancedFactsExtractor(args.in_file)
    else:
        raise SystemExit('Provide --db and --song_id, or --in_file')

    if args.out:
        extractor.save_facts_to_file(args.out)
    else:
        print(extractor.get_llm_prompt_context())


if __name__ == "__main__":
    main()
