#!/usr/bin/env python3
"""
Pipeline DB → JSON exporter (controller)
Exports full ScoreSpec, ScoreSpec-Lite, hierarchical facts, and enhanced facts
for one or more song_ids already present in the pipeline DuckDB.
"""

import argparse, json, importlib.util
from pathlib import Path
from typing import List, Optional

import duckdb
from scorespec_db import build_scorespec


def _load_module(path: Path, name: str):
	spec = importlib.util.spec_from_file_location(name, str(path))
	mod = importlib.util.module_from_spec(spec)
	spec.loader.exec_module(mod)  # type: ignore
	return mod


def export_from_pipeline_db(db_path: str, out_dir: Path, song_ids: Optional[List[str]] = None) -> None:
	con = duckdb.connect(db_path)
	out_dir.mkdir(parents=True, exist_ok=True)
	if not song_ids:
		song_ids = [r[0] for r in con.execute("SELECT DISTINCT song_id FROM bars ORDER BY song_id").fetchall()]

	base = Path(__file__).resolve().parent
	lite_mod = _load_module(base / "2_scorespec_lite.py", "scorespec_lite_mod")
	hf_mod = _load_module(base / "2_hierarchical_facts.py", "hier_facts_mod")
	ef_mod = _load_module(base / "2_enhanced_facts_extractor.py", "enhanced_facts_mod")

	for sid in song_ids:
		try:
			ss = build_scorespec(con, sid)
			(out_dir / f"{sid}.scorespec.json").write_text(json.dumps(ss, indent=2))
			lite = lite_mod.ScoreSpecLiteGenerator(ss).generate_scorespec_lite()
			(out_dir / f"{sid}.scorespec_lite.json").write_text(json.dumps(lite, indent=2))
			hf = hf_mod.HierarchicalFactsExtractor(ss).get_hierarchical_facts()
			(out_dir / f"{sid}.hierarchical_facts.json").write_text(json.dumps(hf, indent=2))
			ef = ef_mod.EnhancedFactsExtractor(ss)
			(out_dir / f"{sid}.enhanced_facts.txt").write_text(ef.get_llm_prompt_context())
			print(f"✓ Exported JSONs for {sid} → {out_dir}")
		except Exception as e:
			print(f"✗ Export failed for {sid}: {e}")


def main():
	ap = argparse.ArgumentParser(description="Export ScoreSpec + facts from pipeline DuckDB")
	ap.add_argument("--db", required=True, help="Path to pipeline DuckDB file")
	ap.add_argument("--export_scorespec_dir", default="scorespec_json", help="Output directory")
	ap.add_argument("--songs", nargs='*', default=None, help="Specific song_ids to export (default: all)")
	args = ap.parse_args()

	export_from_pipeline_db(args.db, Path(args.export_scorespec_dir), args.songs)


if __name__ == "__main__":
	main()
