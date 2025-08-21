#!/usr/bin/env python3
"""
Run OpenAI on a dynamically generated MIDIPHOR prompt
- Builds prompt from ScoreSpec (no hardcoding)
- Sends to OpenAI using environment variable OPENAI_API_KEY
- Validates that output contains a robust ensemble/composition description
- Supports dry-run (no API call) and saves request/response to files
"""

import os
import json
import argparse
from pathlib import Path
from typing import Dict, Any, List

try:
	from openai import OpenAI
	_OPENAI_AVAILABLE = True
except Exception:  # pragma: no cover
	_OPENAI_AVAILABLE = False

from comprehensive_llm_prompt import ComprehensiveLLMPrompt


def build_messages(prompt_text: str) -> List[Dict[str, str]]:
	"""Construct system+user messages for robust ensemble/composition description"""
	system_msg = (
		"You are a senior musicologist, arranger, and orchestrator. "
		"Write precise, well-structured prose grounded ONLY in the provided musical facts. "
		"Prefer concise paragraphs and clear bullet lists where helpful. "
		"Avoid speculation; if something is unknown, say so."
	)

	user_task = (
		"Using the analysis context below, produce a robust text description of the piece's ensemble and composition.\n\n"
		"OUTPUT REQUIREMENTS (all sections required, omit only if truly unknown):\n"
		"1) Ensemble Overview (size, instrument families, notable timbres)\n"
		"2) Instrumentation and Ranges (register spans, total pitch range)\n"
		"3) Roles and Functions (melody carriers, accompaniment, bass/support)\n"
		"4) Texture and Layering (how forces combine or alternate over form)\n"
		"5) Form and Section Flow (high-level structure, section lengths)\n"
		"6) Harmonic Language (common chord types, harmonic rhythm, pitch-class traits)\n"
		"7) Rhythmic Character (meter, tempo character, notable grooves)\n"
		"8) Expression and Controllers (dynamics/CC usage and performance implications)\n\n"
		"Write for musicians. Be specific and factual; cite concrete bars/sections when implied by the data."
	)

	messages = [
		{"role": "system", "content": system_msg},
		{"role": "user", "content": f"MUSICAL CONTEXT (auto-generated):\n\n{prompt_text}\n\n{user_task}"},
	]
	return messages


def validate_response(text: str) -> Dict[str, Any]:
	"""Check that required sections are present in the LLM response."""
	required_sections = [
		"Ensemble Overview",
		"Instrumentation and Ranges",
		"Roles and Functions",
		"Texture and Layering",
		"Form and Section Flow",
		"Harmonic Language",
		"Rhythmic Character",
		"Expression and Controllers",
	]
	present = {name: (name.lower() in text.lower()) for name in required_sections}
	missing = [name for name, ok in present.items() if not ok]
	return {"present": present, "missing": missing}


def run_openai(messages: List[Dict[str, str]], model: str, temperature: float, max_tokens: int) -> str:
	"""Call OpenAI Chat Completions API and return response text."""
	if not _OPENAI_AVAILABLE:
		raise RuntimeError("openai package not installed. Run: pip install openai")

	client = OpenAI()
	resp = client.chat.completions.create(
		model=model,
		messages=messages,
		temperature=temperature,
		max_tokens=max_tokens,
	)
	choice = resp.choices[0]
	return choice.message.content or ""


def main():
	parser = argparse.ArgumentParser(description="Send dynamic MIDIPHOR prompt to OpenAI for ensemble/composition description")
	parser.add_argument("--scorespec", default="scorespec_json/Dancing Queen.scorespec.json", help="Path to ScoreSpec JSON")
	parser.add_argument("--out", default="ensemble_composition_description.txt", help="Where to save the model output")
	parser.add_argument("--request-preview", default="openai_payload_preview.json", help="Where to save the request preview (dry-run)")
	parser.add_argument("--model", default=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"), help="OpenAI model name")
	parser.add_argument("--temperature", type=float, default=0.2)
	parser.add_argument("--max-tokens", type=int, default=1200)
	parser.add_argument("--dry-run", action="store_true", help="Do not call the API; save the request only")
	parser.add_argument("--full-prompt", action="store_true", help="Use full UI prompt (includes capabilities/how-to); default uses analysis-only context")
	parser.add_argument("--context-out", default="openai_prompt_context.txt", help="Where to save the analysis context that will be sent to the model")
	args = parser.parse_args()

	# Generate dynamic prompt from ScoreSpec
	prompter = ComprehensiveLLMPrompt(args.scorespec)
	if args.full_prompt:
		prompt_text = prompter.create_comprehensive_prompt()
	else:
		# Default: slim analysis-only context (excludes UI sections not needed by model)
		prompt_text = prompter.create_analysis_context(include_header=True)

	# Save the prompt/context used for the request
	Path(args.context_out).write_text(prompt_text, encoding="utf-8")
	print(f"Saved analysis context to: {args.context_out}")
	print("Context preview (first 600 chars):")
	print("=" * 60)
	print(prompt_text[:600] + ("..." if len(prompt_text) > 600 else ""))

	# Build messages
	messages = build_messages(prompt_text)

	# If no API key or dry-run requested, save request preview and exit
	api_key = os.environ.get("OPENAI_API_KEY")
	if args.dry_run or not api_key:
		preview = {
			"model": args.model,
			"temperature": args.temperature,
			"max_tokens": args.max_tokens,
			"messages": messages,
			"using_full_prompt": args.full_prompt,
			"context_path": args.context_out,
			"context_char_count": len(prompt_text),
			"note": "Set OPENAI_API_KEY to actually call the API. This is a dry-run preview.",
		}
		Path(args.request_preview).write_text(json.dumps(preview, indent=2), encoding="utf-8")
		print(f"[DRY-RUN] Saved request preview to: {args.request_preview}")
		print("[DRY-RUN] To execute, set OPENAI_API_KEY and remove --dry-run")
		return

	# Call OpenAI
	text = run_openai(messages, args.model, args.temperature, args.max_tokens)

	# Save output
	Path(args.out).write_text(text, encoding="utf-8")
	print(f"Saved model output to: {args.out}")

	# Validate robustness of sections
	report = validate_response(text)
	if report["missing"]:
		print("Validation: Some required sections appear missing:")
		for name in report["missing"]:
			print(f" - {name}")
	else:
		print("Validation: All required sections detected.")

	# Also save validation report
	Path(args.out + ".validation.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
	print(f"Saved validation report to: {args.out}.validation.json")


if __name__ == "__main__":
	main()
