#!/usr/bin/env python3
"""
ScoreSpec-Lite Generator for MIDIPHOR
Creates compact, LLM-optimized versions of ScoreSpec data
"""

import json
from typing import Dict, List, Any, Tuple
import duckdb


class ScoreSpecLiteGenerator:
    """Generates compact, hierarchical ScoreSpec-Lite format for LLMs"""
    
    def __init__(self, scorespec_data: Dict[str, Any]):
        """Initialize with full ScoreSpec data"""
        self.scorespec = scorespec_data
        
    def get_total_bars(self) -> int:
        """Calculate total bars from segments"""
        segments = self.scorespec.get('segments', [])
        if segments:
            return segments[-1]['bars'][1]
        return 0
    
    def estimate_duration(self) -> float:
        """Estimate duration from tempo and bars"""
        global_info = self.scorespec.get('global', {})
        bpm = global_info.get('approx_bpm', 120)
        bars = self.get_total_bars()
        # Rough estimate: 4 beats per bar at given BPM
        return (bars * 4 * 60) / bpm
    
    def compress_tempo_changes(self) -> List[Dict[str, Any]]:
        """Create compressed tempo map with bar references"""
        # For now, use the global tempo - can be enhanced later
        global_info = self.scorespec.get('global', {})
        bpm = global_info.get('approx_bpm', 120)
        return [{"bar": 1, "bpm": bpm}]
    
    def calculate_section_density(self, section: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate note density and activity for a section"""
        # This would need database access for detailed calculation
        # For now, return basic structure
        return {
            "bars": section['bars'],
            "estimated_density": "medium"  # Placeholder
        }
    
    def get_active_instruments(self, section: Dict[str, Any]) -> List[int]:
        """Get instruments active in a section"""
        # This would need database access for detailed calculation
        # For now, return all instruments
        instruments = self.scorespec.get('instruments', [])
        return [inst['track_id'] for inst in instruments]
    
    def extract_harmonic_progressions(self) -> List[Dict[str, Any]]:
        """Extract harmonic progressions per section"""
        pitch_spans = self.scorespec.get('pitch_class_spans', [])
        sections = self.scorespec.get('segments', [])
        
        harmony_summary = []
        for section in sections:
            section_bars = section['bars']
            # Find pitch class spans that overlap with this section
            section_pcs = []
            for span in pitch_spans:
                if (span['bars'][0] < section_bars[1] and 
                    span['bars'][1] > section_bars[0]):
                    section_pcs.extend(span['pcs'])
            
            if section_pcs:
                unique_pcs = len(set(section_pcs))
                coverage = len(section_pcs) / max(len(section_pcs), 1)
                harmony_summary.append({
                    "section": section['id'],
                    "pitch_classes": list(set(section_pcs))[:8],  # Limit to 8 most common
                    "complexity": unique_pcs,
                    "coverage": round(coverage, 2)
                })
        
        return harmony_summary
    
    def compress_motifs(self) -> List[Dict[str, Any]]:
        """Compress motif data for compact representation"""
        graph = self.scorespec.get('graph', {})
        nodes = graph.get('nodes', [])
        
        motifs = []
        for node in nodes:
            if node.get('type') == 'Motif':
                payload = node.get('payload', {})
                motif_id = node.get('node_id', '')
                
                # Find occurrences
                occurrences = []
                for edge in graph.get('edges', []):
                    if edge.get('rel') == 'PLAYED_BY':
                        # Extract motif pattern ID from source (e.g., "motocc:pat_0:0:1" -> "pat_0")
                        src = edge.get('src', '')
                        if src.startswith('motocc:'):
                            edge_motif_id = src.split(':')[1]  # Extract "pat_0" from "motocc:pat_0:0:1"
                            # Extract pattern ID from motif node ID (e.g., "mot:pat_0" -> "pat_0")
                            node_pattern_id = motif_id.split(':')[1] if ':' in motif_id else motif_id
                            if edge_motif_id == node_pattern_id:
                                # Extract track and bar info from edge
                                props = edge.get('props', {})
                                if 'track_id' in props:
                                    occurrences.append({
                                        "track": props['track_id'],
                                        "bars": [edge.get('start_bar', 0), edge.get('end_bar', 0)]
                                    })
                
                motifs.append({
                    "id": motif_id,
                    "pattern": payload.get('pattern_repr', ''),
                    "occurrences": len(occurrences),
                    "locations": occurrences[:3]  # Limit to first 3 occurrences
                })
        
        return motifs
    
    def extract_key_relationships(self) -> List[Dict[str, Any]]:
        """Extract key musical relationships from graph"""
        graph = self.scorespec.get('graph', {})
        edges = graph.get('edges', [])
        
        relationships = []
        for edge in edges:
            rel_type = edge.get('rel', '')
            if rel_type in ['DOUBLES', 'SUPPORTS_HARMONY_OF', 'OCCURS_IN']:
                props = edge.get('props', {})
                relationships.append({
                    "type": rel_type,
                    "from": edge.get('src', ''),
                    "to": edge.get('dst', ''),
                    "bars": [edge.get('start_bar', 0), edge.get('end_bar', 0)],
                    "details": props
                })
        
        return relationships[:10]  # Limit to 10 most important relationships
    
    def create_drill_down_pointers(self) -> List[Dict[str, Any]]:
        """Create pointers for detailed data access"""
        return [
            {
                "kind": "notes",
                "bars": [1, self.get_total_bars()],
                "description": "Full note data available via database queries"
            },
            {
                "kind": "controllers", 
                "description": "Controller data available via database queries"
            },
            {
                "kind": "graph",
                "description": "Full graph structure available in original ScoreSpec"
            }
        ]
    
    def generate_scorespec_lite(self) -> Dict[str, Any]:
        """Generate the complete ScoreSpec-Lite format"""
        return {
            "meta": {
                "title": self.scorespec.get('file_id', 'Unknown'),
                "bars": self.get_total_bars(),
                "duration_estimate": round(self.estimate_duration(), 1)
            },
            "tempo_map": self.compress_tempo_changes(),
            "sections": [
                {
                    "id": seg["id"],
                    "bars": seg["bars"],
                    "density": self.calculate_section_density(seg),
                    "instruments_active": self.get_active_instruments(seg)
                }
                for seg in self.scorespec.get('segments', [])
            ],
            "harmony_summary": self.extract_harmonic_progressions(),
            "motif_inventory": self.compress_motifs(),
            "relationships": self.extract_key_relationships(),
            "evidence_refs": self.create_drill_down_pointers()
        }

    def generate_natural_language_summary(self) -> str:
        """Convert ScoreSpec-Lite to natural language text for LLM consumption"""
        lite_spec = self.generate_scorespec_lite()
        
        summary = f"""MUSICAL PIECE ANALYSIS: {lite_spec['meta']['title']}

OVERVIEW:
- Total bars: {lite_spec['meta']['bars']}
- Estimated duration: {lite_spec['meta']['duration_estimate']} seconds

TEMPO:
"""
        
        # Add tempo information
        if lite_spec['tempo_map']:
            if len(lite_spec['tempo_map']) == 1:
                summary += f"- Steady tempo: {lite_spec['tempo_map'][0]['bpm']} BPM throughout\n"
            else:
                summary += f"- {len(lite_spec['tempo_map'])} tempo changes detected:\n"
                for tempo in lite_spec['tempo_map']:
                    summary += f"  Bar {tempo['bar']}: {tempo['bpm']} BPM\n"
        else:
            summary += "- No tempo changes detected\n"
        
        summary += "\nSTRUCTURE:\n"
        # Add section information
        total_sections = len(lite_spec['sections'])
        summary += f"- Total sections: {total_sections}\n"
        
        # Show first 8 sections for better coverage
        for i, section in enumerate(lite_spec['sections'][:8]):
            bars = section.get('bars', [0, 0])
            density = section.get('density', {}).get('estimated_density', 'unknown')
            instruments = section.get('instruments_active', [])
            summary += f"- Section {i+1} ({section['id']}): Bars {bars[0]}-{bars[1]}, Density: {density}, Instruments: {len(instruments)}\n"
        
        if total_sections > 8:
            summary += f"- ... and {total_sections - 8} more sections\n"
        
        summary += "\nHARMONY:\n"
        # Add harmony information
        if lite_spec['harmony_summary']:
            total_harmony_sections = len(lite_spec['harmony_summary'])
            summary += f"- Total harmony sections: {total_harmony_sections}\n"
            
            # Show first 8 harmony sections for better coverage
            for harmony in lite_spec['harmony_summary'][:8]:
                section = harmony.get('section', 'unknown')
                complexity = harmony.get('complexity', 0)
                coverage = harmony.get('coverage', 0.0)
                pitch_classes = harmony.get('pitch_classes', [])
                summary += f"- {section}: Complexity {complexity}, Coverage {coverage:.1f}, Pitch classes: {len(pitch_classes)}\n"
            
            if total_harmony_sections > 8:
                summary += f"- ... and {total_harmony_sections - 8} more harmony sections\n"
        else:
            summary += "- No harmonic analysis available\n"
        
        summary += "\nMOTIFS:\n"
        # Add motif information
        if lite_spec['motif_inventory']:
            # Sort by occurrences to show most important first
            sorted_motifs = sorted(lite_spec['motif_inventory'], key=lambda x: x['occurrences'], reverse=True)
            for motif in sorted_motifs[:15]:  # Show top 15 motifs by frequency
                summary += f"- Pattern '{motif['pattern']}': {motif['occurrences']} occurrences"
                if motif['locations']:
                    # Get unique locations to avoid repetition
                    unique_locations = []
                    seen = set()
                    for loc in motif['locations']:
                        loc_key = (loc['track'], loc['bars'][0], loc['bars'][1])
                        if loc_key not in seen:
                            seen.add(loc_key)
                            unique_locations.append(loc)
                    
                    if unique_locations:
                        # Show first 3 unique locations
                        locations_str = ", ".join([f"Track {loc['track']} (bars {loc['bars'][0]}-{loc['bars'][1]})" for loc in unique_locations[:3]])
                        summary += f" - Locations: {locations_str}"
                        if len(unique_locations) > 3:
                            summary += f" and {len(unique_locations) - 3} more"
                summary += "\n"
        else:
            summary += "- No motifs detected\n"
        
        summary += "\nRELATIONSHIPS:\n"
        # Add relationship information
        if lite_spec['relationships']:
            total_relationships = len(lite_spec['relationships'])
            summary += f"- Total key relationships: {total_relationships}\n"
            
            # Show first 8 relationships for better coverage
            for rel in lite_spec['relationships'][:8]:
                summary += f"- {rel['type']}: {rel['from']} -> {rel['to']} (bars {rel['bars'][0]}-{rel['bars'][1]})\n"
            
            if total_relationships > 8:
                summary += f"- ... and {total_relationships - 8} more relationships\n"
        else:
            summary += "- No key relationships detected\n"
        
        summary += "\nINSTRUMENTS:\n"
        # Add instrument information
        instruments = self.scorespec.get('instruments', [])
        if instruments:
            summary += f"- Total tracks: {len(instruments)}\n"
            programs = [inst['program'] for inst in instruments]
            unique_programs = len(set(programs))
            summary += f"- Unique MIDI programs: {unique_programs}\n"
        else:
            summary += "- No instrument information available\n"
        
        # Add summary statistics
        summary += "\nSUMMARY STATISTICS:\n"
        total_motifs = len(lite_spec.get('motif_inventory', []))
        total_motif_occurrences = sum(motif.get('occurrences', 0) for motif in lite_spec.get('motif_inventory', []))
        summary += f"- Total unique motifs: {total_motifs}\n"
        summary += f"- Total motif occurrences: {total_motif_occurrences}\n"
        if total_motifs > 0:
            summary += f"- Average motif frequency: {total_motif_occurrences/total_motifs:.1f} occurrences per motif\n"
        else:
            summary += "- Average motif frequency: N/A\n"
        
        return summary

    def save_scorespec_lite(self, output_path: str):
        """Save ScoreSpec-Lite to file"""
        lite_spec = self.generate_scorespec_lite()
        
        with open(output_path, 'w') as f:
            json.dump(lite_spec, f, indent=2)
        
    def save_natural_language_summary(self, output_path: str):
        """Save ScoreSpec-Lite as natural language text"""
        summary = self.generate_natural_language_summary()
        
        with open(output_path, 'w') as f:
            f.write(summary)


def create_scorespec_lite_from_file(scorespec_path: str, output_path: str = None) -> Dict[str, Any]:
    """Convenience function to create ScoreSpec-Lite from file"""
    with open(scorespec_path, 'r') as f:
        scorespec_data = json.load(f)
    
    generator = ScoreSpecLiteGenerator(scorespec_data)
    
    if output_path:
        return generator.save_scorespec_lite(output_path)
    else:
        return generator.generate_scorespec_lite()


if __name__ == "__main__":
    # Example usage
    lite_spec = create_scorespec_lite_from_file("scorespec_json/Chiquitita.scorespec.json")
    print("ScoreSpec-Lite generated successfully!")
    print(f"Meta: {lite_spec['meta']}")
    print(f"Sections: {len(lite_spec['sections'])}")
    print(f"Motifs: {len(lite_spec['motif_inventory'])}")