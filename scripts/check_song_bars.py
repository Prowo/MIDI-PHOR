from __future__ import annotations
import argparse
import duckdb


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/musiccap.duckdb")
    ap.add_argument("--song", required=True)
    args = ap.parse_args()

    con = duckdb.connect(args.db)

    n_bars = con.execute("SELECT COUNT(*) FROM bars WHERE song_id=?", [args.song]).fetchone()[0]
    sigs = con.execute("SELECT DISTINCT num, den FROM bars WHERE song_id=? ORDER BY num, den", [args.song]).fetchall()
    qpm_stats = con.execute("SELECT MIN(qpm), MAX(qpm), COUNT(DISTINCT qpm) FROM bars WHERE song_id=?", [args.song]).fetchone()
    frame_counts = con.execute("SELECT feature, COUNT(*) FROM ts_frame WHERE song_id=? GROUP BY feature ORDER BY feature LIMIT 10", [args.song]).fetchall()

    print(f"bars_count={n_bars}")
    print(f"time_signatures={sigs}")
    print(f"tempo_min_max_distinct={qpm_stats}")
    print("frame_counts_sample=")
    for f, c in frame_counts:
        print(f"  {f}: {c}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



