#!/usr/bin/env python3
"""
Hierarchical Facts Extractor for MIDIPHOR
Organizes musical facts by domain for targeted LLM consumption
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Any, Tuple

from scorespec_db import build_scorespec
import duckdb


class HierarchicalFactsExtractor:
    """Organizes facts by musical domain for targeted querying"""
    
    def __init__(self, scorespec_input):
        """Initialize with a ScoreSpec JSON file path or dict"""
        if isinstance(scorespec_input, dict):
            self.scorespec = scorespec_input
            self.scorespec_path = None
        else:
            self.scorespec_path = Path(scorespec_input)
            self.scorespec = self._load_scorespec()
        
    def _load_scorespec(self) -> Dict[str, Any]:
        """Load ScoreSpec JSON file"""
        with open(self.scorespec_path, 'r') as f:
            return json.load(f)
    
    def get_hierarchical_facts(self) -> Dict[str, List[str]]:
        """Organize facts by musical domain for targeted querying"""
        return {
            "structure": self.extract_structure_facts(),
            "harmony": self.extract_harmonic_facts(), 
            "orchestration": self.extract_instrumentation_facts(),
            "rhythm": self.extract_rhythmic_facts(),
            "relationships": self.extract_graph_facts(),
            "controllers": self.extract_controller_facts(),
            "motifs": self.extract_motif_facts(),
            "form": self.extract_form_facts()
        }
    
    def extract_structure_facts(self) -> List[str]:
        """Extract musical structure facts"""
        facts = []
        segments = self.scorespec.get('segments', [])
        
        if segments:
            total_bars = segments[-1]['bars'][1] if segments else 0
            facts.append(f"Piece has {len(segments)} sections spanning {total_bars} bars")
            
            section_lengths = [seg['bars'][1] - seg['bars'][0] for seg in segments]
            if len(set(section_lengths)) == 1:
                facts.append(f"All sections are {section_lengths[0]} bars long")
            else:
                facts.append(f"Section lengths vary: {min(section_lengths)}-{max(section_lengths)} bars")
                
            if len(segments) >= 4:
                facts.append("Piece appears to have a multi-section form structure")
        
        return facts
    
    def extract_harmonic_facts(self) -> List[str]:
        """Extract harmonic analysis facts"""
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
        
        return facts
    
    def extract_instrumentation_facts(self) -> List[str]:
        """Extract instrumentation and ensemble facts"""
        facts = []
        instruments = self.scorespec.get('instruments', [])
        
        if instruments:
            facts.append(f"Piece uses {len(instruments)} instrument tracks")
            
            programs = [inst.get('program') for inst in instruments]
            unique_programs = len(set(programs))
            facts.append(f"Uses {unique_programs} unique MIDI programs")
        
        return facts
    
    def extract_rhythmic_facts(self) -> List[str]:
        """Extract rhythmic and groove facts"""
        facts = []
        global_info = self.scorespec.get('global', {})
        
        if 'meter' in global_info and global_info.get('meter'):
            facts.append(f"Piece is in {global_info['meter']} time signature")
        
        if 'approx_bpm' in global_info and global_info.get('approx_bpm'):
            facts.append(f"Tempo is approximately {global_info['approx_bpm']} BPM")
        
        facts.append("Rhythmic analysis based on section structure and tempo")
        
        return facts
    
    def extract_graph_facts(self) -> List[str]:
        """Extract relationship facts from graph data"""
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
    
    def extract_controller_facts(self) -> List[str]:
        """Extract controller usage facts"""
        return []
    
    def extract_motif_facts(self) -> List[str]:
        """Extract motif-related facts"""
        facts = []
        motifs = self.scorespec.get('motifs', [])
        if motifs:
            facts.append(f"Piece contains {len(motifs)} musical motifs")
            total_occ = sum(m.get('support', 0) for m in motifs)
            facts.append(f"Total motif occurrences: {total_occ}")
        return facts
    
    def extract_form_facts(self) -> List[str]:
        """Extract musical form and structure facts"""
        facts = []
        segments = self.scorespec.get('segments', [])
        instruments = self.scorespec.get('instruments', [])
        
        if segments and instruments:
            total_bars = segments[-1]['bars'][1] if segments else 0
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
        
        return facts
    
    def get_domain_facts(self, domain: str) -> List[str]:
        """Get facts for a specific musical domain"""
        hierarchical = self.get_hierarchical_facts()
        return hierarchical.get(domain, [])
    
    def get_llm_context_by_domain(self, domains: List[str] = None) -> str:
        """Generate LLM context focused on specific domains"""
        if domains is None:
            domains = ["structure", "harmony", "orchestration"]
        
        context = f"Based on the following musical analysis of '{self.scorespec.get('file_id', 'Unknown Piece')}':\n\n"
        
        for domain in domains:
            facts = self.get_domain_facts(domain)
            if facts:
                context += f"{domain.upper()}:\n"
                for fact in facts:
                    context += f"• {fact}\n"
                context += "\n"
        
        context += "Please use this information to help with musical composition tasks."
        return context
    
    def save_hierarchical_facts(self, output_path: str):
        """Save hierarchical facts to a structured file"""
        hierarchical = self.get_hierarchical_facts()
        
        with open(output_path, 'w') as f:
            f.write(f"Hierarchical Musical Facts for: {self.scorespec.get('file_id', 'Unknown Piece')}\n")
            f.write("=" * 60 + "\n\n")
            
            for domain, facts in hierarchical.items():
                f.write(f"{domain.upper()}:\n")
                for i, fact in enumerate(facts, 1):
                    f.write(f"  {i}. {fact}\n")
                f.write("\n")
            
            f.write(f"Total Domains: {len(hierarchical)}\n")
            f.write(f"Total Facts: {sum(len(facts) for facts in hierarchical.values())}\n")


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
        extractor = HierarchicalFactsExtractor(ss)
    elif args.in_file:
        extractor = HierarchicalFactsExtractor(args.in_file)
    else:
        raise SystemExit('Provide --db and --song_id, or --in_file')

    if args.out:
        extractor.save_hierarchical_facts(args.out)
    else:
        facts = extractor.get_hierarchical_facts()
        print(json.dumps(facts, indent=2))


if __name__ == "__main__":
    main()