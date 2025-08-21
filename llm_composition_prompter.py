#!/usr/bin/env python3
"""
LLM Composition Prompter for MIDIPHOR
Uses ScoreSpec data to create prompts for musical composition tasks
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from enhanced_facts_extractor import EnhancedFactsExtractor


class LLMCompositionPrompter:
    """Creates LLM prompts for musical composition using ScoreSpec data"""
    
    def __init__(self, scorespec_path: str):
        """Initialize with a ScoreSpec JSON file"""
        self.scorespec_path = Path(scorespec_path)
        self.scorespec = self._load_scorespec()
        self.facts_extractor = EnhancedFactsExtractor(scorespec_path)
        
    def _load_scorespec(self) -> Dict[str, Any]:
        """Load ScoreSpec JSON file"""
        with open(self.scorespec_path, 'r') as f:
            return json.load(f)
    
    def create_composition_prompt(self, task_type: str, **kwargs) -> str:
        """Create a composition prompt based on task type"""
        if task_type == "extend":
            return self._create_extend_prompt(**kwargs)
        elif task_type == "arrange":
            return self._create_arrange_prompt(**kwargs)
        elif task_type == "harmonize":
            return self._create_harmonize_prompt(**kwargs)
        elif task_type == "orchestrate":
            return self._create_orchestrate_prompt(**kwargs)
        elif task_type == "analyze":
            return self._create_analysis_prompt(**kwargs)
        else:
            return self._create_general_prompt(**kwargs)
    
    def _create_extend_prompt(self, target_bars: int = 8, style: str = "similar") -> str:
        """Create prompt for extending the piece"""
        facts = self.facts_extractor.generate_all_facts()
        
        prompt = f"""You are a musical composition assistant. Based on the following analysis of '{self.scorespec.get('file_id', 'Unknown Piece')}', please help extend the piece.

MUSICAL CONTEXT:
"""
        for fact in facts[:15]:  # First 15 facts for context
            prompt += f"• {fact}\n"
        
        prompt += f"""

TASK: Compose a {target_bars}-bar extension that {style} to the existing piece.

REQUIREMENTS:
1. Maintain the same key, meter, and tempo
2. Use similar harmonic progressions and chord types
3. Follow the established instrumental patterns
4. Create a natural musical flow from the existing material
5. Provide specific musical suggestions (chord progressions, melodic ideas, etc.)

Please provide:
- Harmonic framework for the {target_bars} bars
- Melodic suggestions for key instruments
- Arrangement notes (which instruments should play when)
- Any specific musical techniques to use
"""
        return prompt
    
    def _create_arrange_prompt(self, section: str = "chorus", focus: str = "dynamics") -> str:
        """Create prompt for arranging a specific section"""
        facts = self.facts_extractor.generate_all_facts()
        
        prompt = f"""You are a musical arrangement specialist. Based on the following analysis of '{self.scorespec.get('file_id', 'Unknown Piece')}', please help arrange the {section} section.

MUSICAL CONTEXT:
"""
        for fact in facts[:20]:  # More facts for arrangement context
            prompt += f"• {fact}\n"
        
        prompt += f"""

TASK: Create an arrangement for the {section} section focusing on {focus}.

CURRENT ARRANGEMENT ANALYSIS:
"""
        
        # Add specific section information
        if 'segments' in self.scorespec:
            for seg in self.scorespec['segments']:
                if section.lower() in seg['id'].lower():
                    prompt += f"• Section {seg['id']}: bars {seg['bars'][0]}-{seg['bars'][1]}\n"
        
        prompt += f"""

ARRANGEMENT REQUIREMENTS:
1. Focus on {focus} (dynamics, texture, layering, etc.)
2. Use the existing instrumental palette effectively
3. Create clear musical hierarchy and balance
4. Suggest specific performance directions
5. Consider the emotional impact and musical flow

Please provide:
- Detailed arrangement notes for each instrument
- Dynamic markings and expression suggestions
- Textural changes and layering ideas
- Performance techniques and articulations
"""
        return prompt
    
    def _create_harmonize_prompt(self, melody_notes: List[int], style: str = "jazz") -> str:
        """Create prompt for harmonizing a melody"""
        facts = self.facts_extractor.generate_all_facts()
        
        prompt = f"""You are a harmony specialist. Based on the following analysis of '{self.scorespec.get('file_id', 'Unknown Piece')}', please help harmonize a melody.

MUSICAL CONTEXT:
"""
        for fact in facts[:12]:  # Focus on harmonic facts
            prompt += f"• {fact}\n"
        
        prompt += f"""

TASK: Harmonize the following melody in a {style} style that fits the piece's harmonic language.

MELODY NOTES: {melody_notes}

HARMONIC REQUIREMENTS:
1. Use chord types that appear in the original piece
2. Follow the established harmonic rhythm
3. Create voice leading that fits the instrumental ranges
4. Maintain the piece's overall harmonic character
5. Consider the emotional and stylistic context

Please provide:
- Chord symbols for each melody note
- Voice leading suggestions
- Alternative harmonization options
- Performance considerations
"""
        return prompt
    
    def _create_orchestrate_prompt(self, target_instruments: List[str], style: str = "classical") -> str:
        """Create prompt for orchestration"""
        facts = self.facts_extractor.generate_all_facts()
        
        prompt = f"""You are an orchestration expert. Based on the following analysis of '{self.scorespec.get('file_id', 'Unknown Piece')}', please help with orchestration.

MUSICAL CONTEXT:
"""
        for fact in facts[:18]:  # Focus on instrumentation facts
            prompt += f"• {fact}\n"
        
        prompt += f"""

TASK: Create an orchestration for {', '.join(target_instruments)} that works with the existing piece.

ORCHESTRATION REQUIREMENTS:
1. Work within the established harmonic framework
2. Use the target instruments effectively
3. Create balanced textures and dynamics
4. Consider the piece's emotional character
5. Maintain musical coherence with existing material

Please provide:
- Instrumental assignments for different musical elements
- Textural layering suggestions
- Dynamic and articulation markings
- Performance considerations for each instrument
- Balance and blend recommendations
"""
        return prompt
    
    def _create_analysis_prompt(self, focus_area: str = "structure") -> str:
        """Create prompt for musical analysis"""
        facts = self.facts_extractor.generate_all_facts()
        
        prompt = f"""You are a music analyst. Based on the following analysis of '{self.scorespec.get('file_id', 'Unknown Piece')}', please provide deeper insights.

MUSICAL CONTEXT:
"""
        for fact in facts:
            prompt += f"• {fact}\n"
        
        prompt += f"""

TASK: Provide a detailed analysis focusing on {focus_area}.

ANALYSIS REQUIREMENTS:
1. Use the factual information provided above
2. Identify patterns and relationships in the music
3. Explain the musical significance of key elements
4. Suggest connections to music theory and history
5. Provide insights for performers and composers

Please provide:
- Detailed analysis of the {focus_area}
- Musical patterns and relationships
- Theoretical explanations
- Performance insights
- Compositional implications
"""
        return prompt
    
    def _create_general_prompt(self, custom_task: str = "") -> str:
        """Create a general composition prompt"""
        facts = self.facts_extractor.generate_all_facts()
        
        prompt = f"""You are a musical composition assistant. Based on the following analysis of '{self.scorespec.get('file_id', 'Unknown Piece')}', please help with musical tasks.

MUSICAL CONTEXT:
"""
        for fact in facts:
            prompt += f"• {fact}\n"
        
        if custom_task:
            prompt += f"\nTASK: {custom_task}\n"
        else:
            prompt += f"""

AVAILABLE TASKS:
1. Extend the piece with new sections
2. Arrange existing sections for different instruments
3. Harmonize melodies
4. Orchestrate for specific ensembles
5. Analyze musical structure and patterns
6. Suggest compositional improvements

Please specify what musical task you'd like help with, and I'll provide detailed guidance based on the piece's characteristics.
"""
        return prompt
    
    def create_interactive_session(self) -> str:
        """Create a prompt for an interactive composition session"""
        facts = self.facts_extractor.generate_all_facts()
        
        prompt = f"""You are an interactive musical composition partner. Based on the following analysis of '{self.scorespec.get('file_id', 'Unknown Piece')}', let's work together on musical ideas.

MUSICAL CONTEXT:
"""
        for fact in facts[:25]:  # Comprehensive context for interaction
            prompt += f"• {fact}\n"
        
        prompt += f"""

INTERACTIVE SESSION:
I'm working on musical composition and would like your input. You have access to the complete musical analysis above.

HOW TO INTERACT:
1. Ask me specific questions about what I'm working on
2. Suggest musical ideas based on the piece's characteristics
3. Help me solve compositional challenges
4. Provide feedback on my musical decisions
5. Suggest alternatives and variations

I can ask you about:
- Harmonic progressions and chord choices
- Melodic development and motifs
- Instrumental arrangement and orchestration
- Musical form and structure
- Performance and expression
- Any other musical aspect

Let's start! What musical idea or challenge would you like to work on together?
"""
        return prompt
    
    def save_prompt_to_file(self, prompt: str, output_path: str):
        """Save a generated prompt to a text file"""
        with open(output_path, 'w') as f:
            f.write(f"LLM Composition Prompt for: {self.scorespec.get('file_id', 'Unknown Piece')}\n")
            f.write("=" * 70 + "\n\n")
            f.write(prompt)
    
    def get_scorespec_summary(self) -> str:
        """Get a concise summary of the ScoreSpec for quick reference"""
        summary = f"SCORESPEC SUMMARY: {self.scorespec.get('file_id', 'Unknown Piece')}\n"
        summary += "=" * 50 + "\n\n"
        
        # Global info
        global_info = self.scorespec.get('global', {})
        if global_info:
            summary += f"Global: {global_info.get('meter', 'Unknown')} time, {global_info.get('approx_bpm', 'Unknown')} BPM\n"
        
        # Structure
        segments = self.scorespec.get('segments', [])
        if segments:
            summary += f"Structure: {len(segments)} sections, {segments[-1]['bars'][1]} total bars\n"
        
        # Instruments
        instruments = self.scorespec.get('instruments', [])
        if instruments:
            summary += f"Instruments: {len(instruments)} tracks\n"
        
        # Harmony
        pitch_spans = self.scorespec.get('pitch_class_spans', [])
        if pitch_spans:
            summary += f"Harmony: {len(pitch_spans)} harmonic sections\n"
        
        # Controllers
        controllers = self.scorespec.get('controllers', {})
        if controllers:
            active_ccs = [cc for cc, info in controllers.items() if info.get('present', False)]
            summary += f"Controllers: {len(active_ccs)} active types\n"
        
        return summary


def main():
    """Example usage"""
    # Example: create prompts for a ScoreSpec file
    prompter = LLMCompositionPrompter("scorespec_json/Dancing Queen.scorespec.json")
    
    # Create different types of prompts
    extend_prompt = prompter.create_composition_prompt("extend", target_bars=16, style="similar")
    arrange_prompt = prompter.create_composition_prompt("arrange", section="chorus", focus="dynamics")
    analysis_prompt = prompter.create_composition_prompt("analyze", focus_area="harmonic structure")
    interactive_prompt = prompter.create_interactive_session()
    
    # Save prompts to files
    prompter.save_prompt_to_file(extend_prompt, "dancing_queen_extend_prompt.txt")
    prompter.save_prompt_to_file(arrange_prompt, "dancing_queen_arrange_prompt.txt")
    prompter.save_prompt_to_file(analysis_prompt, "dancing_queen_analysis_prompt.txt")
    prompter.save_prompt_to_file(interactive_prompt, "dancing_queen_interactive_prompt.txt")
    
    # Print summary
    print("ScoreSpec Summary:")
    print(prompter.get_scorespec_summary())
    
    print("\nExample Extend Prompt:")
    print(extend_prompt[:500] + "...")


if __name__ == "__main__":
    main()
