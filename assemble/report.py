# scripts/report.py
from __future__ import annotations
import argparse, json
from pathlib import Path
import duckdb

def _q(con, sql, *args): return con.execute(sql, list(args)).fetchall()
def _first(con, sql, *args):
    r = con.execute(sql, list(args)).fetchone()
    return r[0] if r and r[0] is not None else None

def render_report(con, song_id: str) -> str:
    lines = []
    title = _first(con, "SELECT title FROM songs WHERE song_id=?", song_id) or song_id
    meter = _first(con, "SELECT CONCAT(num,'/',den) FROM timesig_changes WHERE song_id=? ORDER BY t_sec LIMIT 1", song_id) or "unknown"
    bpm = _first(con, "SELECT ROUND(AVG(qpm),0) FROM bars WHERE song_id=?", song_id)

    lines.append(f"# Report: {title}")
    lines.append("")
    lines.append(f"- Meter: **{meter}**   - Tempo: **{int(bpm) if bpm else '—'} BPM**")
    lines.append("")

    # Sections (merged if present; else any)
    secs = _q(con, """
        SELECT section_id, COALESCE(type,'other'), start_bar, end_bar, source, ROUND(confidence,2)
        FROM sections WHERE song_id=? 
        ORDER BY (source='merged') DESC, start_bar
    """, song_id)
    if secs:
        lines.append("## Sections")
        for sid, typ, sb, eb, src, conf in secs:
            lines.append(f"- {sid} [{typ}] bars {sb}-{eb} (source={src}, conf={conf})")
        lines.append("")

    # Events
    ev = _q(con, """
        SELECT bar, event_type, ROUND(COALESCE(strength,0.0),2) s
        FROM events WHERE song_id=? ORDER BY bar
    """, song_id)
    if ev:
        lines.append("## Events")
        for b, t, s in ev:
            lines.append(f"- bar {b}: {t} (strength {s})")
        lines.append("")

    # Bar features summary (means)
    feats = ["energy_bar","energy_bar_z","energy_bar_delta","brightness_bar",
             "novelty_bar","density","polyphony","harmonic_rhythm",
             "active_tracks","active_drums","active_bass","active_pad","active_melody",
             "repeat_score_bar","cadence_strength"]
    rows = _q(con, f"""
        WITH agg AS (
          SELECT feature, AVG(value) AS mu
          FROM ts_bar WHERE song_id=? AND feature IN ({','.join(['?']*len(feats))})
          GROUP BY feature
        )
        SELECT feature, ROUND(mu,3) FROM agg ORDER BY feature
    """, song_id, *feats)
    if rows:
        lines.append("## Bar-level features (mean)")
        for f, v in rows:
            lines.append(f"- {f}: {v}")
        lines.append("")

    # Chords (first few)
    ch = _q(con, """
        SELECT onset_bar, onset_beat, name, rn FROM chords
        WHERE song_id=? ORDER BY onset_bar, onset_beat LIMIT 16
    """, song_id)
    if ch:
        lines.append("## Chords (first 16)")
        for b, bt, nm, rn in ch:
            lines.append(f"- bar {b:>3} beat {bt:>4.1f}: {nm} ({rn})")
        lines.append("")

    # Tags
    tg = _q(con, """
        SELECT section_id, tag_type, tag, ROUND(confidence,2)
        FROM tags_section WHERE song_id=? ORDER BY section_id, tag_type, confidence DESC
    """, song_id)
    if tg:
        lines.append("## Tags")
        for sid, tt, tag, conf in tg:
            lines.append(f"- {sid}: {tt} = {tag} ({conf})")
        lines.append("")

    # Graph summary
    ge = _q(con, """
        SELECT rel_type, COUNT(*) c FROM graph_edges WHERE song_id=? GROUP BY rel_type ORDER BY c DESC
    """, song_id)
    if ge:
        lines.append("## Graph edges")
        for rel, c in ge:
            lines.append(f"- {rel}: {c}")
        lines.append("")
    return "\n".join(lines)

def main():
    ap = argparse.ArgumentParser(description="Print a per-song extraction report.")
    ap.add_argument("--db", required=True)
    ap.add_argument("--song_id", required=True)
    ap.add_argument("--out_md", default=None, help="Optional path to write Markdown report")
    args = ap.parse_args()

    con = duckdb.connect(args.db)
    md = render_report(con, args.song_id)
    print(md)
    if args.out_md:
        p = Path(args.out_md); p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(md)
        print(f"\nSaved Markdown → {p}")

if __name__ == "__main__":
    main()
