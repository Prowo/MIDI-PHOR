#!/usr/bin/env python3
"""
Comprehensive LLM Prompt for MIDIPHOR
Dynamically generates prompts from ScoreSpec data
"""

import json
from pathlib import Path
from typing import Dict, List, Any
from enhanced_facts_extractor import EnhancedFactsExtractor


class ComprehensiveLLMPrompt:
	"""Dynamically generates prompts from ScoreSpec data"""
	
	def __init__(self, scorespec_path: str):
		"""Initialize with a ScoreSpec JSON file"""
		self.scorespec_path = Path(scorespec_path)
		self.scorespec = self._load_scorespec()
		self.facts_extractor = EnhancedFactsExtractor(scorespec_path)
		
	def _load_scorespec(self) -> Dict[str, Any]:
		"""Load ScoreSpec JSON file"""
		with open(self.scorespec_path, 'r') as f:
			return json.load(f)
	
	def create_comprehensive_prompt(self) -> str:
		"""Dynamically create prompt from ScoreSpec data (full UI + analysis)"""
		facts = self.facts_extractor.generate_all_facts()
		
		# Dynamically build prompt sections based on available data
		prompt_sections = []
		
		# 1. Header - dynamically generated from piece info
		prompt_sections.append(self._generate_header())
		
		# 2. Musical Analysis - dynamically generated from facts
		prompt_sections.append(self._generate_musical_analysis(facts))
		
		# 3. Capabilities - dynamically generated from available data
		prompt_sections.append(self._generate_capabilities())
		
		# 4. Usage Examples - dynamically generated from piece characteristics
		prompt_sections.append(self._generate_usage_examples())
		
		# 5. Musical Context - dynamically generated from ScoreSpec
		prompt_sections.append(self._generate_musical_context())
		
		# 6. Ready message - personalized to the piece
		prompt_sections.append(self._generate_ready_message())
		
		# Combine all sections
		return "\n\n".join(prompt_sections)

	def create_analysis_context(self, include_header: bool = False) -> str:
		"""Create a slim, analysis-only context for LLM input.
		Excludes UI sections like 'Available Capabilities', 'How to Use Me', and 'Ready to Help'.
		"""
		facts = self.facts_extractor.generate_all_facts()
		sections: List[str] = []
		if include_header:
			# Minimal header that names the piece only
			piece_name = self.scorespec.get('file_id', 'Unknown Piece')
			sections.append(f"Musical analysis context for '{piece_name}'.")
		# Always include analysis facts and derived context
		sections.append(self._generate_musical_analysis(facts))
		sections.append(self._generate_musical_context())
		return "\n\n".join(sections)
	
	def _generate_header(self) -> str:
		"""Dynamically generate header from piece data"""
		piece_name = self.scorespec.get('file_id', 'Unknown Piece')
		
		# Analyze piece characteristics to determine appropriate role
		global_info = self.scorespec.get('global', {})
		segments = self.scorespec.get('segments', [])
		instruments = self.scorespec.get('instruments', [])
		
		# Determine piece complexity and style
		complexity = self._assess_complexity(segments, instruments)
		style = self._assess_style(global_info)
		
		header = f"""You are an advanced musical composition AI assistant with access to comprehensive musical analysis data. Based on the following detailed analysis of '{piece_name}', you can help with any musical composition task.

This piece is {complexity} and has a {style} character."""
		
		return header
	
	def _assess_complexity(self, segments: List, instruments: List) -> str:
		"""Dynamically assess piece complexity"""
		if not segments or not instruments:
			return "moderate complexity"
		
		total_bars = segments[-1]['bars'][1] if segments else 0
		num_instruments = len(instruments)
		
		if total_bars > 64 and num_instruments > 8:
			return "high complexity"
		elif total_bars > 32 and num_instruments > 4:
			return "moderate complexity"
		else:
			return "accessible complexity"
	
	def _assess_style(self, global_info: Dict) -> str:
		"""Dynamically assess piece style from tempo and meter"""
		meter = global_info.get('meter', '4/4')
		bpm = global_info.get('approx_bpm', 120)
		
		if bpm < 80:
			return "relaxed, contemplative"
		elif bpm < 120:
			return "flowing, moderate"
		elif bpm < 160:
			return "energetic, upbeat"
		else:
			return "fast-paced, driving"
	
	def _generate_musical_analysis(self, facts: List[str]) -> str:
		"""Dynamically generate musical analysis section"""
		# Group facts by category
		categorized_facts = self._categorize_facts(facts)
		
		analysis = "================================================================================\nMUSICAL ANALYSIS DATA\n================================================================================\n\n"
		
		# Add piece overview based on available data
		analysis += self._generate_piece_overview()
		
		# Add categorized facts
		for category, category_facts in categorized_facts.items():
			if category_facts:
				analysis += f"\n{category.upper()}:\n"
				for i, fact in enumerate(category_facts, 1):
					analysis += f"  {i:2d}. {fact}\n"
		
		return analysis
	
	def _categorize_facts(self, facts: List[str]) -> Dict[str, List[str]]:
		"""Dynamically categorize facts by type"""
		categories = {
			'structure': [],
			'instrumentation': [],
			'harmony': [],
			'performance': [],
			'compositional': []
		}
		
		for fact in facts:
			fact_lower = fact.lower()
			if any(word in fact_lower for word in ['section', 'bar', 'form']):
				categories['structure'].append(fact)
			elif any(word in fact_lower for word in ['instrument', 'track', 'program', 'register']):
				categories['instrumentation'].append(fact)
			elif any(word in fact_lower for word in ['chord', 'pitch', 'harmonic', 'triad']):
				categories['harmony'].append(fact)
			elif any(word in fact_lower for word in ['controller', 'expression', 'volume', 'modulation']):
				categories['performance'].append(fact)
			else:
				categories['compositional'].append(fact)
		
		return categories
	
	def _generate_piece_overview(self) -> str:
		"""Dynamically generate piece overview from ScoreSpec data"""
		overview = "PIECE OVERVIEW:\n"
		
		# Add available global information
		global_info = self.scorespec.get('global', {})
		if global_info:
			if 'meter' in global_info:
				overview += f"• Time Signature: {global_info['meter']}\n"
			if 'approx_bpm' in global_info:
				overview += f"• Tempo: {global_info['approx_bpm']} BPM\n"
		
		# Add available structure information
		segments = self.scorespec.get('segments', [])
		if segments:
			total_bars = segments[-1]['bars'][1]
			overview += f"• Structure: {len(segments)} sections, {total_bars} total bars\n"
		
		# Add available instrumentation information
		instruments = self.scorespec.get('instruments', [])
		if instruments:
			overview += f"• Ensemble: {len(instruments)} instrument tracks\n"
		
		# Add available harmonic information
		pitch_spans = self.scorespec.get('pitch_class_spans', [])
		if pitch_spans:
			overview += f"• Harmony: {len(pitch_spans)} harmonic sections\n"
		
		# Add available performance information
		controllers = self.scorespec.get('controllers', {})
		if controllers:
			active_ccs = [cc for cc, info in controllers.items() if info.get('present', False)]
			overview += f"• Expression: {len(active_ccs)} active controller types\n"
		
		return overview
	
	def _generate_capabilities(self) -> str:
		"""Dynamically generate capabilities based on available data"""
		capabilities = "================================================================================\nAVAILABLE CAPABILITIES\n================================================================================\n\n"
		capabilities += "I can help you with the following musical tasks:\n\n"
		
		# Dynamically determine capabilities based on available data
		available_capabilities = []
		
		# Check if we have structure data for composition
		if self.scorespec.get('segments'):
			available_capabilities.append({
				'name': 'EXTEND & COMPOSE',
				'description': 'Create new sections, verses, choruses, bridges',
				'details': [
					'Maintain musical consistency with existing piece',
					'Suggest harmonic frameworks and chord progressions',
					'Provide melodic ideas and instrumental arrangements',
					'Create natural musical flow and transitions'
				]
			})
		
		# Check if we have instrumentation data for arrangement
		if self.scorespec.get('instruments'):
			available_capabilities.append({
				'name': 'ARRANGE & ORCHESTRATE',
				'description': 'Rearrange existing sections for different instruments',
				'details': [
					'Focus on dynamics, texture, or layering',
					'Use existing instrumental palette effectively',
					'Suggest performance directions and articulations',
					'Create balanced textures and musical hierarchy'
				]
			})
		
		# Check if we have harmonic data for analysis
		if self.scorespec.get('pitch_class_spans'):
			available_capabilities.append({
				'name': 'HARMONIZE & ANALYZE',
				'description': 'Add harmony to melodies and analyze musical structure',
				'details': [
					"Use piece's harmonic language and chord types",
					'Follow established harmonic rhythm and patterns',
					'Identify musical patterns and relationships',
					'Provide theoretical explanations and insights'
				]
			})
		
		# Always include interactive composition
		available_capabilities.append({
			'name': 'INTERACTIVE COMPOSITION',
			'description': 'Work together on musical ideas',
			'details': [
				'Solve compositional challenges',
				'Provide feedback on musical decisions',
				'Suggest alternatives and variations',
				'Guide through complex musical concepts'
			]
		})
		
		# Check if we have performance data
		if self.scorespec.get('controllers'):
			available_capabilities.append({
				'name': 'PERFORMANCE & EXPRESSION',
				'description': 'Enhance musical performance',
				'details': [
					'Dynamic markings and expression suggestions',
					'Controller usage and MIDI expression',
					'Articulation and performance techniques',
					'Balance and blend recommendations'
				]
			})
		
		# Generate numbered capabilities
		for i, cap in enumerate(available_capabilities, 1):
			capabilities += f"{i}. **{cap['name']}** - {cap['description']}\n"
			for detail in cap['details']:
				capabilities += f"   - {detail}\n"
			capabilities += "\n"
		
		return capabilities
	
	def _generate_usage_examples(self) -> str:
		"""Dynamically generate usage examples based on piece characteristics"""
		examples = "================================================================================\nHOW TO USE ME\n================================================================================\n\n"
		examples += "Simply tell me what you want to do! For example:\n\n"
		
		# Generate examples based on available data
		example_queries = []
		
		# Structure-based examples
		if self.scorespec.get('segments'):
			total_bars = self.scorespec.get('segments', [{}])[-1].get('bars', [0, 32])[1]
			if total_bars > 32:
				example_queries.append(f"Help me extend this piece with a new {min(16, total_bars//4)}-bar section")
			example_queries.append("Rearrange the chorus section focusing on dynamics")
		
		# Harmony-based examples
		if self.scorespec.get('pitch_class_spans'):
			example_queries.append("Harmonize this melody: [60, 62, 64, 65]")
			example_queries.append("Analyze the harmonic structure of this piece")
		
		# Instrumentation-based examples
		if self.scorespec.get('instruments'):
			num_instruments = len(self.scorespec['instruments'])
			if num_instruments > 4:
				example_queries.append("Help me orchestrate this for a string quartet")
			example_queries.append("What instruments should I use for this section?")
		
		# General examples
		example_queries.extend([
			"I'm stuck on this chord progression, can you help?",
			"How can I improve the arrangement of this section?"
		])
		
		# Add examples to prompt
		for query in example_queries[:7]:  # Limit to 7 examples
			examples += f"• \"{query}\""
			if query != example_queries[-1]:
				examples += "\n"
		
		examples += "\n\nI'll use the musical analysis above to provide informed, musically coherent suggestions that:\n"
		examples += "- Maintain the piece's established style and character\n"
		examples += "- Use the actual harmonic and rhythmic patterns found in the music\n"
		examples += "- Consider the available instruments and their capabilities\n"
		examples += "- Provide specific, actionable musical advice\n"
		examples += "- Explain the reasoning behind my suggestions"
		
		return examples
	
	def _generate_musical_context(self) -> str:
		"""Dynamically generate musical context from ScoreSpec data"""
		context = "================================================================================\nMUSICAL CONTEXT FOR COMPOSITION\n================================================================================\n\n"
		context += "Based on the analysis above, here are key considerations for any composition task:\n\n"
		
		# Generate context insights based on available data
		insights = []
		
		if self.scorespec.get('segments'):
			insights.append(f"**Structure & Form**: {self._get_structure_insights()}")
		
		if self.scorespec.get('pitch_class_spans'):
			insights.append(f"**Harmonic Language**: {self._get_harmonic_insights()}")
		
		if self.scorespec.get('instruments'):
			insights.append(f"**Instrumentation**: {self._get_instrumentation_insights()}")
		
		if self.scorespec.get('global'):
			insights.append(f"**Rhythmic Character**: {self._get_rhythmic_insights()}")
		
		if self.scorespec.get('controllers'):
			insights.append(f"**Performance Style**: {self._get_performance_insights()}")
		
		# Add insights to context
		for insight in insights:
			context += f"{insight}\n"
		
		return context
	
	def _generate_ready_message(self) -> str:
		"""Dynamically generate ready message based on piece characteristics"""
		piece_name = self.scorespec.get('file_id', 'Unknown Piece')
		
		# Analyze piece to personalize message
		global_info = self.scorespec.get('global', {})
		segments = self.scorespec.get('segments', [])
		
		tempo_desc = ""
		if 'approx_bpm' in global_info:
			bpm = global_info['approx_bpm']
			if bpm < 120:
				tempo_desc = " Whether you want to maintain the relaxed pace or add energy"
			else:
				tempo_desc = " Whether you want to build on the existing energy or create contrast"
		
		structure_desc = ""
		if segments:
			total_bars = segments[-1]['bars'][1]
			if total_bars > 64:
				structure_desc = " Given the piece's substantial length, you have room for development"
			else:
				structure_desc = " The piece's concise structure offers opportunities for expansion"
		
		ready_message = f"""================================================================================
READY TO HELP!
================================================================================

What musical task would you like to work on? I'm ready to help you compose, arrange, analyze, or improve your music using the comprehensive musical data I have access to.{tempo_desc}{structure_desc}

Remember: Every suggestion I make is based on actual musical facts from '{piece_name}', ensuring musical coherence and artistic integrity."""
		
		return ready_message
	
	def _get_structure_insights(self) -> str:
		"""Get insights about musical structure"""
		segments = self.scorespec.get('segments', [])
		if not segments:
			return "Structure information not available"
		
		total_bars = segments[-1]['bars'][1]
		section_lengths = [seg['bars'][1] - seg['bars'][0] for seg in segments]
		
		if len(set(section_lengths)) == 1:
			return f"Consistent {section_lengths[0]}-bar sections throughout {total_bars} bars"
		else:
			return f"Variable section lengths ({min(section_lengths)}-{max(section_lengths)} bars) across {total_bars} total bars"
	
	def _get_harmonic_insights(self) -> str:
		"""Get insights about harmonic language"""
		pitch_spans = self.scorespec.get('pitch_class_spans', [])
		if not pitch_spans:
			return "Harmonic information not available"
		
		all_pcs = []
		for span in pitch_spans:
			all_pcs.extend(span['pcs'])
		
		unique_pcs = len(set(all_pcs))
		avg_span_length = sum(span['bars'][1] - span['bars'][0] for span in pitch_spans) / len(pitch_spans)
		
		return f"Uses {unique_pcs} unique pitch classes with harmonic rhythm of {avg_span_length:.1f} bars per chord change"
	
	def _get_instrumentation_insights(self) -> str:
		"""Get insights about instrumentation"""
		instruments = self.scorespec.get('instruments', [])
		if not instruments:
			return "Instrumentation information not available"
		
		programs = [inst['program'] for inst in instruments]
		unique_programs = len(set(programs))
		
		# Categorize by family
		families = self._categorize_programs(programs)
		family_desc = []
		for family, count in families.items():
			if count > 0:
				family_desc.append(f"{count} {family}")
		
		return f"{len(instruments)} tracks using {unique_programs} unique MIDI programs: {', '.join(family_desc)}"
	
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
	
	def _get_rhythmic_insights(self) -> str:
		"""Get insights about rhythmic character"""
		global_info = self.scorespec.get('global', {})
		if 'meter' in global_info and 'approx_bpm' in global_info:
			meter = global_info['meter']
			bpm = global_info['approx_bpm']
			
			if bpm < 80:
				tempo_char = "slow, relaxed"
			elif bpm < 120:
				tempo_char = "moderate, flowing"
			elif bpm < 160:
				tempo_char = "upbeat, energetic"
			else:
				tempo_char = "fast, driving"
			
			return f"{meter} time at {bpm} BPM ({tempo_char} character)"
		
		return "Rhythmic information not available"
	
	def _get_performance_insights(self) -> str:
		"""Get insights about performance style"""
		controllers = self.scorespec.get('controllers', {})
		if not controllers:
			return "Performance information not available"
		
		active_controllers = [cc for cc, info in controllers.items() if info.get('present', False)]
		
		controller_names = {
			'1': 'modulation wheel',
			'7': 'volume control',
			'10': 'panning',
			'11': 'expression',
			'64': 'sustain pedal'
		}
		
		controller_desc = []
		for cc in active_controllers:
			name = controller_names.get(cc, f'CC{cc}')
			count = controllers[cc].get('count', 0)
			controller_desc.append(f"{name} ({count} messages)")
		
		return f"Active expression controls: {', '.join(controller_desc)}"
	
	def save_comprehensive_prompt(self, output_path: str):
		"""Save the comprehensive prompt to a text file"""
		prompt = self.create_comprehensive_prompt()
		
		with open(output_path, 'w') as f:
			f.write(f"COMPREHENSIVE LLM PROMPT for: {self.scorespec.get('file_id', 'Unknown Piece')}\n")
			f.write("=" * 80 + "\n\n")
			f.write(prompt)
	
	def get_prompt_preview(self, max_length: int = 1000) -> str:
		"""Get a preview of the prompt for quick review"""
		prompt = self.create_comprehensive_prompt()
		if len(prompt) <= max_length:
			return prompt
		else:
			return prompt[:max_length] + "...\n\n[Prompt truncated for preview]"


def main():
	"""Example usage"""
	# Create comprehensive prompt for a ScoreSpec file
	prompter = ComprehensiveLLMPrompt("scorespec_json/Dancing Queen.scorespec.json")
	
	# Save the comprehensive prompt
	prompter.save_comprehensive_prompt("dancing_queen_comprehensive_prompt.txt")
	
	# Print a preview
	print("Comprehensive Prompt Preview:")
	print("=" * 60)
	print(prompter.get_prompt_preview(800))
	
	print(f"\nFull prompt saved to: dancing_queen_comprehensive_prompt.txt")
	print(f"Prompt length: {len(prompter.create_comprehensive_prompt())} characters")


if __name__ == "__main__":
	main()
