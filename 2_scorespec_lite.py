#!/usr/bin/env python3
"""
ScoreSpec-Lite Generator for MIDIPHOR
Creates compact, LLM-optimized versions of ScoreSpec data
"""

import json
import argparse
from typing import Dict, List, Any, Tuple, Optional
import statistics
import duckdb

from scorespec_db import build_scorespec


class ScoreSpecComputer:
    """Computes real metrics for ScoreSpec-Lite from DuckDB, with safe fallbacks."""

    def __init__(self, con: Optional[duckdb.DuckDBPyConnection], file_id: Optional[str], full_spec: Optional[Dict[str, Any]] = None):
        self.con = con
        self.file_id = file_id or (full_spec.get('file_id') if full_spec else None)
        self.full_spec = full_spec or {}

    # --------- Helpers ---------
    def _avg_bpm(self) -> Optional[float]:
        if not self.con or not self.file_id:
            bpm = (self.full_spec.get('global', {}) or {}).get('approx_bpm')
            return float(bpm) if bpm else None
        row = self.con.execute("SELECT ROUND(AVG(qpm),0) FROM bars WHERE song_id=?", [self.file_id]).fetchone()
        return float(row[0]) if row and row[0] is not None else None

    def get_total_bars(self) -> int:
        if self.con and self.file_id:
            row = self.con.execute("SELECT COALESCE(MAX(bar),0) FROM bars WHERE song_id=?", [self.file_id]).fetchone()
            return int(row[0] or 0)
        # fallback from full_spec segments
        segments = self.full_spec.get('segments', [])
        return int(segments[-1]['bars'][1]) if segments else 0

    # --------- Public computations ---------
    def compute_meta(self) -> Dict[str, Any]:
        title = self.file_id or 'Unknown'
        duration = None
        if self.con and self.file_id:
            r = self.con.execute("SELECT COALESCE(title, ?), duration_sec FROM songs WHERE song_id=?", [title, self.file_id]).fetchone()
            if r:
                title = r[0] or title
                duration = r[1]
        bars = self.get_total_bars()
        bpm = self._avg_bpm() or 120.0
        if duration is None:
            # Heuristic: 4 beats/bar
            duration = (bars * 4.0 * 60.0) / max(bpm, 1.0)
        return {"title": title, "bars": bars, "duration_estimate": round(float(duration), 1)}

    def compute_tempo_map(self) -> List[Dict[str, Any]]:
        # Prefer bar-local qpm to derive change points
        if self.con and self.file_id:
            rows = self.con.execute(
                "SELECT bar, qpm FROM bars WHERE song_id=? AND qpm IS NOT NULL ORDER BY bar",
                [self.file_id],
            ).fetchall()
            tempo_map: List[Dict[str, Any]] = []
            last_bpm: Optional[int] = None
            for bar, qpm in rows:
                bpm = int(round(qpm)) if qpm is not None else None
                if bpm is None:
                    continue
                if last_bpm is None or bpm != last_bpm:
                    tempo_map.append({"bar": int(bar), "bpm": bpm})
                    last_bpm = bpm
            if tempo_map:
                return tempo_map
        # Fallback: single BPM
        bpm = int(round(self._avg_bpm() or 120.0))
        return [{"bar": 1, "bpm": bpm}]

    def _song_bar_note_counts(self) -> List[int]:
        if not (self.con and self.file_id):
            return []
        rows = self.con.execute(
            """
            SELECT onset_bar AS bar, COUNT(*) AS n
            FROM notes
            WHERE song_id=?
            GROUP BY onset_bar
            ORDER BY onset_bar
            """,
            [self.file_id],
        ).fetchall()
        return [int(n) for _, n in rows]

    def _density_category(self, mean_notes_per_bar: float) -> str:
        # Use tertiles across the song as thresholds when available
        counts = self._song_bar_note_counts()
        if counts:
            try:
                q = statistics.quantiles(counts, n=3, method='inclusive')
                low_thr, high_thr = q[0], q[1]
            except Exception:
                low_thr, high_thr = 4.0, 10.0
        else:
            low_thr, high_thr = 4.0, 10.0
        if mean_notes_per_bar < low_thr:
            return "low"
        if mean_notes_per_bar > high_thr:
            return "high"
        return "medium"

    def _compute_section_density(self, start_bar: int, end_bar_inclusive: int) -> Dict[str, Any]:
        if self.con and self.file_id:
            row = self.con.execute(
                """
                WITH counts AS (
                  SELECT onset_bar AS bar, COUNT(*) AS n
                  FROM notes
                  WHERE song_id=? AND onset_bar BETWEEN ? AND ?
                  GROUP BY onset_bar
                )
                SELECT COALESCE(AVG(n),0)
                FROM counts
                """,
                [self.file_id, int(start_bar), int(end_bar_inclusive)],
            ).fetchone()
            avg_npbar = float(row[0] or 0.0)
            category = self._density_category(avg_npbar)
            return {"bars": [int(start_bar), int(end_bar_inclusive)], "estimated_density": category}
        # Fallback
        return {"bars": [int(start_bar), int(end_bar_inclusive)], "estimated_density": "unknown"}

    def _active_instruments(self, start_bar: int, end_bar_inclusive: int) -> List[Any]:
        if self.con and self.file_id:
            rows = self.con.execute(
                """
                SELECT DISTINCT track_id
                FROM notes
                WHERE song_id=? AND onset_bar BETWEEN ? AND ?
                ORDER BY track_id
                """,
                [self.file_id, int(start_bar), int(end_bar_inclusive)],
            ).fetchall()
            return [r[0] for r in rows]
        # Fallback to all instruments listed in full_spec
        instruments = (self.full_spec.get('instruments') or [])
        return [inst.get('track_id') for inst in instruments]

    def compute_sections(self) -> List[Dict[str, Any]]:
        sections: List[Tuple[str, int, int]] = []
        if self.con and self.file_id:
            rows = self.con.execute(
                """
                SELECT section_id, start_bar, end_bar
                FROM sections
                WHERE song_id=?
                ORDER BY start_bar
                """,
                [self.file_id],
            ).fetchall()
            sections = [(sid, int(sb), int(eb)) for sid, sb, eb in rows]
        if not sections:
            for seg in (self.full_spec.get('segments') or []):
                sections.append((seg.get('id'), int(seg.get('bars', [1, 1])[0]), int(seg.get('bars', [1, 1])[1])))

        out: List[Dict[str, Any]] = []
        for sid, sb, eb in sections:
            density = self._compute_section_density(sb, eb)
            active = self._active_instruments(sb, eb)
            out.append({"id": sid, "bars": [sb, eb], "density": density, "instruments_active": active})
        return out

    def compute_harmony_summary(self) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        secs = self.compute_sections()
        if self.con and self.file_id:
            for s in secs:
                sb, eb = int(s['bars'][0]), int(s['bars'][1])
                rows = self.con.execute(
                    """
                    SELECT onset_bar, pcset
                    FROM chords
                    WHERE song_id=? AND onset_bar BETWEEN ? AND ?
                    ORDER BY onset_bar
                    """,
                    [self.file_id, sb, eb],
                ).fetchall()
                pc_hist: Dict[int, int] = {}
                bars_with_chord: set[int] = set()
                for ob, pcset in rows:
                    bars_with_chord.add(int(ob))
                    pcs: List[int] = []
                    if pcset:
                        try:
                            sset = str(pcset).strip("{}")
                            if sset:
                                pcs = [int(x) for x in sset.split(",")]
                        except Exception:
                            pcs = []
                    for p in pcs:
                        pc_hist[p] = pc_hist.get(p, 0) + 1
                if sb <= eb:
                    total_bars = (eb - sb + 1)
                else:
                    total_bars = 0
                coverage = (len(bars_with_chord) / total_bars) if total_bars > 0 else 0.0
                pitch_classes_sorted = sorted(pc_hist.items(), key=lambda kv: kv[1], reverse=True)
                pitch_classes = [p for p, _ in pitch_classes_sorted[:8]]
                results.append({
                    "section": s['id'],
                    "pitch_classes": pitch_classes,
                    "complexity": len(set(pc_hist.keys())),
                    "coverage": round(coverage, 2),
                })
        else:
            # Fallback: derive from full_spec pitch_class_spans
            pitch_spans = self.full_spec.get('pitch_class_spans', [])
            for s in secs:
                sb, eb = int(s['bars'][0]), int(s['bars'][1])
                pcs: List[int] = []
                for span in pitch_spans:
                    spb = span.get('bars', [0, 0])
                    if (spb[0] <= eb and spb[1] >= sb):
                        pcs.extend(span.get('pcs', []))
                if pcs:
                    uniq = set(pcs)
                    results.append({
                        "section": s['id'],
                        "pitch_classes": list(uniq)[:8],
                        "complexity": len(uniq),
                        "coverage": 1.0,
                    })
        return results

    def compute_motif_inventory(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        if self.con and self.file_id:
            rows = self.con.execute(
                "SELECT motif_id, pattern, occurrences, support FROM motifs WHERE song_id=?",
                [self.file_id],
            ).fetchall()
            import json as _json
            for mid, pattern, occ_json, support in rows:
                locs: List[Dict[str, Any]] = []
                try:
                    arr = _json.loads(occ_json) if occ_json else []
                    seen = set()
                    for o in arr:
                        b = int(o.get("bar", 0))
                        key = (b,)
                        if key in seen:
                            continue
                        seen.add(key)
                        locs.append({"bars": [b, b]})
                except Exception:
                    pass
                out.append({
                    "id": str(mid),
                    "pattern": str(pattern or ""),
                    "occurrences": int(support or len(locs)),
                    "locations": locs[:3],
                })
            # Keep top-N by occurrences
            out.sort(key=lambda m: m.get("occurrences", 0), reverse=True)
            return out[:20]
        # Fallback from full_spec (if present)
        for m in (self.full_spec.get('motifs') or []):
            occs = m.get('occurrences') or []
            locs = [{"bars": [int(o.get('bar', 0)), int(o.get('bar', 0))]} for o in occs[:3]]
            out.append({
                "id": m.get('id'),
                "pattern": m.get('pattern', ''),
                "occurrences": len(occs),
                "locations": locs,
            })
        return out

    def compute_relationships(self) -> List[Dict[str, Any]]:
        rels: List[Dict[str, Any]] = []
        if self.con and self.file_id:
            rows = self.con.execute(
                """
                SELECT e.src_node_id, e.dst_node_id, LOWER(e.rel_type) AS rel_type,
                       MIN(ev.start_bar) AS sb, MAX(ev.end_bar) AS eb
                FROM graph_edges e
                LEFT JOIN edge_evidence ev USING (song_id, edge_id)
                WHERE e.song_id=?
                GROUP BY e.src_node_id, e.dst_node_id, rel_type
                """,
                [self.file_id],
            ).fetchall()
            def _map_rel(r: str) -> str:
                m = (r or "").lower()
                if m == "doubles":
                    return "DOUBLES"
                if m == "cooccur":
                    return "OCCURS_IN"
                if m == "rhythmic_lock":
                    return "RHYTHMIC_LOCK"
                if m == "supports":
                    return "SUPPORTS_HARMONY_OF"
                return m.upper()

            allowed = {"OCCURS_IN", "PLAYED_BY", "DOUBLES", "SUPPORTS_HARMONY_OF", "RHYTHMIC_LOCK"}
            for src, dst, rel, sb, eb in rows:
                r = _map_rel(rel)
                if r not in allowed:
                    continue
                span = (int(eb) - int(sb)) if (sb is not None and eb is not None) else 0
                rels.append({
                    "type": r,
                    "from": src,
                    "to": dst,
                    "bars": [int(sb) if sb is not None else 0, int(eb) if eb is not None else 0],
                    "details": {"span_bars": span},
                })
            rels.sort(key=lambda e: e.get("details", {}).get("span_bars", 0), reverse=True)
            return rels[:10]
        # Fallback from full_spec graph
        graph = self.full_spec.get('graph', {})
        edges = graph.get('edges', [])
        for edge in edges:
            rel_type = edge.get('rel', '')
            if rel_type in ['DOUBLES', 'SUPPORTS_HARMONY_OF', 'OCCURS_IN', 'RHYTHMIC_LOCK']:
                props = edge.get('props', {})
                rels.append({
                    "type": rel_type,
                    "from": edge.get('src', ''),
                    "to": edge.get('dst', ''),
                    "bars": [edge.get('start_bar', 0), edge.get('end_bar', 0)],
                    "details": props
                })
        return rels[:10]


class ScoreSpecLiteGenerator:
    """Generates compact, hierarchical ScoreSpec-Lite format using a computer."""

    def __init__(self, computer: ScoreSpecComputer):
        self.c = computer

    def create_drill_down_pointers(self) -> List[Dict[str, Any]]:
        return [
            {"kind": "notes", "bars": [1, self.c.get_total_bars()], "description": "Full note data available via database queries"},
            {"kind": "controllers", "description": "Controller data available via database queries"},
            {"kind": "graph", "description": "Full graph structure available in DB"},
        ]

    def generate_scorespec_lite(self) -> Dict[str, Any]:
        return {
            "meta": self.c.compute_meta(),
            "tempo_map": self.c.compute_tempo_map(),
            "sections": self.c.compute_sections(),
            "harmony_summary": self.c.compute_harmony_summary(),
            "motif_inventory": self.c.compute_motif_inventory(),
            "relationships": self.c.compute_relationships(),
            "evidence_refs": self.create_drill_down_pointers(),
        }

    def generate_natural_language_summary(self, lite_spec: Dict[str, Any]) -> str:
        meta = lite_spec.get('meta', {})
        tempo = lite_spec.get('tempo_map', [])
        sections = lite_spec.get('sections', [])
        motifs = lite_spec.get('motif_inventory', [])

        # Track metadata (names/roles) from DB if available, else from full_spec
        track_info: Dict[Any, Dict[str, Any]] = {}
        if self.c.con and self.c.file_id:
            rows = self.c.con.execute(
                "SELECT track_id, COALESCE(name, CAST(track_id AS TEXT)) AS name, COALESCE(role,'other') FROM tracks WHERE song_id=? ORDER BY track_id",
                [self.c.file_id],
            ).fetchall()
            for tid, name, role in rows:
                track_info[tid] = {"name": str(name), "role": str(role)}
        else:
            for inst in (self.c.full_spec.get('instruments') or []):
                track_info[inst.get('track_id')] = {"name": inst.get('name') or str(inst.get('track_id')), "role": inst.get('role') or 'other'}

        # Section types from DB if present
        section_types: Dict[str, str] = {}
        if self.c.con and self.c.file_id:
            try:
                rows = self.c.con.execute(
                    "SELECT section_id, type FROM sections WHERE song_id=?",
                    [self.c.file_id],
                ).fetchall()
                for sid, typ in rows:
                    if sid:
                        section_types[str(sid)] = str(typ) if typ else ''
            except Exception:
                pass

        def midi_to_name(m: int) -> str:
            names = ["C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B"]
            if not isinstance(m, int):
                return str(m)
            octave = (m // 12) - 1
            return f"{names[m % 12]}{octave}"

        def interpret_pattern_to_interval(p: str) -> Optional[str]:
            try:
                if '->' in p:
                    parts = p.split('->')
                    a = int(parts[0].strip())
                    b = int(parts[1].strip())
                    diff = b - a
                    sign = '+' if diff >= 0 else ''
                    return f"{sign}{diff} semitones ({midi_to_name(a)}→{midi_to_name(b)})"
            except Exception:
                return None
            return None

        lines: List[str] = []
        title = meta.get('title', 'Unknown')
        bars = meta.get('bars', 0)
        dur = meta.get('duration_estimate', 0)
        lines.append(f"Title: {title}")
        lines.append(f"Length: ~{dur}s across {bars} bars")
        if len(tempo) <= 1:
            if tempo:
                lines.append(f"Tempo: steady {tempo[0]['bpm']} BPM")
            else:
                lines.append("Tempo: unknown")
        else:
            segs = ", ".join([f"Bar {t['bar']}: {t['bpm']} BPM" for t in tempo[:6]])
            more = "" if len(tempo) <= 6 else f", and {len(tempo)-6} more"
            lines.append(f"Tempo changes: {len(tempo)} segments — {segs}{more}")

        # Sections with instrument names and roles
        for s in sections:
            sid = str(s.get('id', ''))
            sb, eb = s.get('bars', [0, 0])
            dens = (s.get('density') or {}).get('estimated_density', 'unknown')
            active = s.get('instruments_active') or []
            typ = section_types.get(sid, '')
            if typ:
                header = f"Section {sid} ({typ}) [{sb}-{eb}]"
            else:
                header = f"Section {sid} [{sb}-{eb}]"
            # Top 3 active instrument names
            names = []
            roles: Dict[str, int] = {}
            for tid in active:
                info = track_info.get(tid) or {}
                nm = info.get('name') or str(tid)
                rl = info.get('role') or 'other'
                names.append(nm)
                roles[rl] = roles.get(rl, 0) + 1
            names_str = ", ".join(names[:3]) + (" …" if len(names) > 3 else "")
            role_bits = ", ".join([f"{r}:{c}" for r, c in sorted(roles.items(), key=lambda kv: kv[1], reverse=True)[:3]])
            lines.append(f"{header}: density {dens}; active tracks {len(active)} — {names_str} | roles {role_bits}")

        # Harmony details per section (top chord names) if available
        if self.c.con and self.c.file_id and sections:
            try:
                lines.append("Harmony overview:")
                for s in sections[:8]:
                    sb, eb = s.get('bars', [0, 0])
                    sid = s.get('id')
                    rows = self.c.con.execute(
                        """
                        SELECT name, COUNT(*) AS c
                        FROM chords
                        WHERE song_id=? AND onset_bar BETWEEN ? AND ? AND name IS NOT NULL
                        GROUP BY name
                        ORDER BY c DESC
                        LIMIT 3
                        """,
                        [self.c.file_id, int(sb), int(eb)],
                    ).fetchall()
                    if rows:
                        top = ", ".join([f"{n}×{c}" for n, c in rows])
                        lines.append(f"- {sid}: {top}")
            except Exception:
                pass

        # Motifs with interpretation
        if motifs:
            lines.append("Motifs:")
            for m in motifs[:10]:
                patt = m.get('pattern', '')
                occ = m.get('occurrences', 0)
                interp = interpret_pattern_to_interval(patt)
                if interp:
                    lines.append(f"- '{patt}' ({interp}): {occ} occurrences")
                else:
                    lines.append(f"- '{patt}': {occ} occurrences")

        # Relationship highlights
        try:
            rels = self.c.compute_relationships()[:8]
            if rels:
                lines.append("Relationships:")
                for r in rels:
                    sb, eb = r.get('bars', [0, 0])
                    lines.append(f"- {r.get('type')}: {r.get('from')} → {r.get('to')} [{sb}-{eb}]")
        except Exception:
            pass

        # Audio events if present
        if self.c.con and self.c.file_id:
            try:
                ev = self.c.con.execute(
                    """
                    SELECT event_type, bar, strength
                    FROM events WHERE song_id=?
                    ORDER BY 
                      CASE event_type WHEN 'CLIMAX' THEN 0 WHEN 'DROP' THEN 1 WHEN 'SECTION_BOUNDARY' THEN 2 ELSE 3 END,
                      strength DESC
                    LIMIT 10
                    """,
                    [self.c.file_id],
                ).fetchall()
                if ev:
                    lines.append("Audio events:")
                    for et, bar, strength in ev:
                        if strength is None:
                            lines.append(f"- {et} at bar {int(bar)}")
                        else:
                            lines.append(f"- {et} at bar {int(bar)} (score {round(float(strength),2)})")
            except Exception:
                pass
        return "\n".join(lines)


def create_scorespec_lite_from_db(db_path: str, song_id: str, output_path: str = None) -> Dict[str, Any]:
    con = duckdb.connect(db_path)
    full_spec = build_scorespec(con, song_id)
    computer = ScoreSpecComputer(con, song_id, full_spec)
    generator = ScoreSpecLiteGenerator(computer)
    lite = generator.generate_scorespec_lite()
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(lite, f, indent=2, ensure_ascii=False)
        return {}
    return lite


def create_scorespec_lite_from_file(scorespec_path: str, output_path: str = None) -> Dict[str, Any]:
    """Convenience function to create ScoreSpec-Lite from file only (no DB)."""
    with open(scorespec_path, 'r') as f:
        scorespec_data = json.load(f)
    computer = ScoreSpecComputer(con=None, file_id=scorespec_data.get('file_id'), full_spec=scorespec_data)
    generator = ScoreSpecLiteGenerator(computer)
    lite = generator.generate_scorespec_lite()
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(lite, f, indent=2, ensure_ascii=False)
        return {}
    return lite


def create_text_summary_from_db(db_path: str, song_id: str, output_path: Optional[str] = None) -> str:
    con = duckdb.connect(db_path)
    full_spec = build_scorespec(con, song_id)
    generator = ScoreSpecLiteGenerator(ScoreSpecComputer(con, song_id, full_spec))
    lite = generator.generate_scorespec_lite()
    text = generator.generate_natural_language_summary(lite)
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(text)
        return ""
    return text


def create_text_summary_from_file(scorespec_path: str, output_path: Optional[str] = None) -> str:
    with open(scorespec_path, 'r') as f:
        scorespec_data = json.load(f)
    generator = ScoreSpecLiteGenerator(ScoreSpecComputer(None, scorespec_data.get('file_id'), scorespec_data))
    lite = generator.generate_scorespec_lite()
    text = generator.generate_natural_language_summary(lite)
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(text)
        return ""
    return text


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=None)
    ap.add_argument('--song_id', default=None)
    ap.add_argument('--in_file', default=None)
    ap.add_argument('--out', default=None)
    ap.add_argument('--summary_out', default=None, help='Path to write natural-language summary (.txt)')
    args = ap.parse_args()

    if args.db and args.song_id:
        lite = create_scorespec_lite_from_db(args.db, args.song_id, args.out)
        if args.summary_out:
            _ = create_text_summary_from_db(args.db, args.song_id, args.summary_out)
        if lite:
            print(json.dumps(lite, indent=2))
    elif args.in_file:
        lite = create_scorespec_lite_from_file(args.in_file, args.out)
        if args.summary_out:
            _ = create_text_summary_from_file(args.in_file, args.summary_out)
        if lite:
            print(json.dumps(lite, indent=2))
    else:
        raise SystemExit('Provide --db and --song_id, or --in_file')