#!/usr/bin/env python3
"""
Enhanced Facts Extractor for MIDIPHOR
Creates comprehensive, LLM-ready musical facts from ScoreSpec data
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Tuple
import duckdb


class EnhancedFactsExtractor:
    """Extracts comprehensive musical facts from ScoreSpec data for LLM consumption"""
    
    def __init__(self, scorespec_path: str):
        """Initialize with a ScoreSpec JSON file"""
        self.scorespec_path = Path(scorespec_path)
        self.scorespec = self._load_scorespec()
        self.facts = []
        
    def _load_scorespec(self) -> Dict[str, Any]:
        """Load ScoreSpec JSON file"""
        with open(self.scorespec_path, 'r') as f:
            return json.load(f)
    
    def extract_global_facts(self) -> List[str]:
        """Extract global musical facts"""
        facts = []
        global_info = self.scorespec.get('global', {})
        
        if 'meter' in global_info:
            facts.append(f"Piece is in {global_info['meter']} time signature")
        
        if 'approx_bpm' in global_info:
            facts.append(f"Tempo is approximately {global_info['approx_bpm']} BPM")
            
        return facts
    
    def extract_structure_facts(self) -> List[str]:
        """Extract musical structure facts"""
        facts = []
        segments = self.scorespec.get('segments', [])
        
        if segments:
            total_bars = segments[-1]['bars'][1] if segments else 0
            facts.append(f"Piece has {len(segments)} sections spanning {total_bars} bars")
            
            # Analyze section patterns
            section_lengths = [seg['bars'][1] - seg['bars'][0] for seg in segments]
            if len(set(section_lengths)) == 1:
                facts.append(f"All sections are {section_lengths[0]} bars long")
            else:
                facts.append(f"Section lengths vary: {min(section_lengths)}-{max(section_lengths)} bars")
                
            # Identify potential form
            if len(segments) >= 4:
                facts.append("Piece appears to have a multi-section form structure")
        
        return facts
    
    def extract_instrumentation_facts(self) -> List[str]:
        """Extract instrumentation and ensemble facts"""
        facts = []
        instruments = self.scorespec.get('instruments', [])
        
        if instruments:
            facts.append(f"Piece uses {len(instruments)} instrument tracks")
            
            # Analyze program distribution
            programs = [inst['program'] for inst in instruments]
            unique_programs = len(set(programs))
            facts.append(f"Uses {unique_programs} unique MIDI programs")
            
            # Identify instrument families
            program_families = self._categorize_programs(programs)
            for family, count in program_families.items():
                if count > 0:
                    facts.append(f"Has {count} {family} instrument(s)")
            
            # Analyze register usage
            total_range = self._analyze_register_span(instruments)
            facts.append(f"Total pitch range spans {total_range} semitones")
            
            # Analyze entrance patterns
            entrance_timing = self._analyze_entrance_timing(instruments)
            facts.append(f"Instruments enter over {entrance_timing} bars")
        
        return facts
    
    def _categorize_programs(self, programs: List[int]) -> Dict[str, int]:
        """Categorize MIDI programs into families"""
        families = {
            'piano': 0,
            'strings': 0,
            'brass': 0,
            'woodwinds': 0,
            'percussion': 0,
            'synth': 0
        }
        
        for prog in programs:
            if prog in [0, 1, 2, 3, 4, 5, 6, 7]:  # Piano
                families['piano'] += 1
            elif prog in [48, 49, 50, 51, 52, 53, 54, 55]:  # Strings
                families['strings'] += 1
            elif prog in [56, 57, 58, 59, 60, 61, 62, 63]:  # Brass
                families['brass'] += 1
            elif prog in [64, 65, 66, 67, 68, 69, 70, 71]:  # Woodwinds
                families['woodwinds'] += 1
            elif prog in [112, 113, 114, 115, 116, 117, 118, 119]:  # Percussion
                families['percussion'] += 1
            elif prog >= 80:  # Synth/Electronic
                families['synth'] += 1
        
        return families
    
    def _analyze_register_span(self, instruments: List[Dict]) -> int:
        """Calculate total pitch range across all instruments"""
        if not instruments:
            return 0
        
        all_lows = [inst['register']['low'] for inst in instruments if 'register' in inst]
        all_highs = [inst['register']['high'] for inst in instruments if 'register' in inst]
        
        if all_lows and all_highs:
            return max(all_highs) - min(all_lows)
        return 0
    
    def _analyze_entrance_timing(self, instruments: List[Dict]) -> int:
        """Calculate span of instrument entrances"""
        if not instruments:
            return 0
        
        enter_bars = [inst['enter_bar'] for inst in instruments if 'enter_bar' in inst]
        if enter_bars:
            return max(enter_bars) - min(enter_bars)
        return 0
    
    def extract_harmonic_facts(self) -> List[str]:
        """Extract harmonic analysis facts"""
        facts = []
        pitch_spans = self.scorespec.get('pitch_class_spans', [])
        
        if pitch_spans:
            facts.append(f"Piece has {len(pitch_spans)} harmonic sections")
            
            # Analyze harmonic complexity
            all_pcs = []
            for span in pitch_spans:
                all_pcs.extend(span['pcs'])
            
            unique_pcs = len(set(all_pcs))
            facts.append(f"Uses {unique_pcs} unique pitch classes")
            
            # Identify common harmonic patterns
            common_chords = self._identify_common_chords(pitch_spans)
            if common_chords:
                facts.append(f"Common chord types: {', '.join(common_chords)}")
            
            # Analyze harmonic rhythm
            avg_span_length = sum(span['bars'][1] - span['bars'][0] for span in pitch_spans) / len(pitch_spans)
            facts.append(f"Average harmonic rhythm: {avg_span_length:.1f} bars per chord")
        
        return facts
    
    def _identify_common_chords(self, pitch_spans: List[Dict]) -> List[str]:
        """Identify common chord types from pitch class sets"""
        chord_names = []
        
        for span in pitch_spans:
            pcs = set(span['pcs'])
            
            # Major triad
            if {0, 4, 7}.issubset(pcs):
                chord_names.append("major triad")
            # Minor triad
            elif {0, 3, 7}.issubset(pcs):
                chord_names.append("minor triad")
            # Diminished triad
            elif {0, 3, 6}.issubset(pcs):
                chord_names.append("diminished triad")
            # Dominant 7th
            elif {0, 4, 7, 10}.issubset(pcs):
                chord_names.append("dominant 7th")
            # Major 7th
            elif {0, 4, 7, 11}.issubset(pcs):
                chord_names.append("major 7th")
            # Minor 7th
            elif {0, 3, 7, 10}.issubset(pcs):
                chord_names.append("minor 7th")
        
        # Return unique chord names
        return list(set(chord_names))[:3]  # Top 3 most common
    
    def extract_controller_facts(self) -> List[str]:
        """Extract controller usage facts"""
        facts = []
        controllers = self.scorespec.get('controllers', {})
        
        if controllers:
            active_controllers = [cc for cc, info in controllers.items() if info.get('present', False)]
            facts.append(f"Uses {len(active_controllers)} different controller types")
            
            # Analyze specific controllers
            controller_names = {
                '1': 'modulation wheel',
                '7': 'volume',
                '10': 'pan',
                '11': 'expression',
                '64': 'sustain pedal'
            }
            
            for cc, info in controllers.items():
                if info.get('present', False):
                    name = controller_names.get(cc, f'CC{cc}')
                    count = info.get('count', 0)
                    facts.append(f"{name.capitalize()} controller active with {count} messages")
        
        return facts
    
    def extract_graph_facts(self) -> List[str]:
        """Extract relationship facts from graph data"""
        facts = []
        graph = self.scorespec.get('graph', {})
        
        nodes = graph.get('nodes', [])
        edges = graph.get('edges', [])
        
        if nodes:
            facts.append(f"Musical structure contains {len(nodes)} elements")
            
            # Count node types
            node_types = {}
            for node in nodes:
                node_type = node.get('type', 'Unknown')
                node_types[node_type] = node_types.get(node_type, 0) + 1
            
            for node_type, count in node_types.items():
                facts.append(f"Has {count} {node_type} elements")
        
        if edges:
            facts.append(f"Contains {len(edges)} musical relationships")
            
            # Analyze relationship types
            rel_types = {}
            for edge in edges:
                rel_type = edge.get('rel', 'Unknown')
                rel_types[rel_type] = rel_types.get(rel_type, 0) + 1
            
            for rel_type, count in rel_types.items():
                facts.append(f"Has {count} {rel_type} relationships")
        
        return facts
    
    def extract_compositional_facts(self) -> List[str]:
        """Extract high-level compositional facts"""
        facts = []
        
        # Analyze overall structure
        segments = self.scorespec.get('segments', [])
        instruments = self.scorespec.get('instruments', [])
        pitch_spans = self.scorespec.get('pitch_class_spans', [])
        
        if segments and instruments:
            # Complexity assessment
            total_bars = segments[-1]['bars'][1] if segments else 0
            if total_bars > 64:
                facts.append("Piece is substantial in length (over 64 bars)")
            elif total_bars > 32:
                facts.append("Piece is moderate in length (32-64 bars)")
            else:
                facts.append("Piece is concise (under 32 bars)")
            
            # Ensemble complexity
            if len(instruments) > 8:
                facts.append("Piece has a large ensemble")
            elif len(instruments) > 4:
                facts.append("Piece has a medium-sized ensemble")
            else:
                facts.append("Piece has a small ensemble")
            
            # Harmonic complexity
            if pitch_spans:
                avg_pcs_per_span = sum(len(span['pcs']) for span in pitch_spans) / len(pitch_spans)
                if avg_pcs_per_span > 8:
                    facts.append("Piece uses harmonically complex chord structures")
                elif avg_pcs_per_span > 5:
                    facts.append("Piece uses moderately complex harmonies")
                else:
                    facts.append("Piece uses simple harmonic structures")
        
        return facts
    
    def generate_all_facts(self) -> List[str]:
        """Generate all musical facts"""
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
        """Save extracted facts to a text file"""
        facts = self.generate_all_facts()
        
        with open(output_path, 'w') as f:
            f.write(f"Musical Facts for: {self.scorespec.get('file_id', 'Unknown Piece')}\n")
            f.write("=" * 60 + "\n\n")
            
            for i, fact in enumerate(facts, 1):
                f.write(f"{i}. {fact}\n")
            
            f.write(f"\nTotal Facts: {len(facts)}\n")
    
    def get_llm_prompt_context(self) -> str:
        """Generate LLM prompt context from facts"""
        facts = self.generate_all_facts()
        
        context = f"Based on the following musical analysis of '{self.scorespec.get('file_id', 'Unknown Piece')}':\n\n"
        
        for fact in facts:
            context += f"• {fact}\n"
        
        context += "\nPlease use this information to help with musical composition tasks."
        return context


def main():
    """Example usage"""
    # Example: extract facts from a ScoreSpec file
    extractor = EnhancedFactsExtractor("scorespec_json/Chiquitita.scorespec.json")
    
    # Save facts to file
    extractor.save_facts_to_file("dancing_queen_facts.txt")
    
    # Print LLM prompt context
    print("LLM Prompt Context:")
    print(extractor.get_llm_prompt_context())


if __name__ == "__main__":
    main()
