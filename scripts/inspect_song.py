from __future__ import annotations
import argparse
import duckdb


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/musiccap.duckdb")
    ap.add_argument("--song", required=True)
    ap.add_argument("--bars", type=int, default=8)
    args = ap.parse_args()

    con = duckdb.connect(args.db)

    print(f"Song: {args.song}")
    feats = con.execute(
        "SELECT feature, COUNT(*) AS n FROM ts_bar WHERE song_id=? GROUP BY feature ORDER BY feature",
        [args.song],
    ).fetchall()
    print("\nFeatures in ts_bar (name, rows):")
    for f, n in feats:
        print(f"- {f}: {n}")

    keys = (
        "energy_bar",
        "energy_bar_z",
        "brightness_bar_z",
        "novelty_bar",
        "onset_strength_bar",
        "tempo_bar",
        "pulse_strength_bar",
        "repeat_score_bar",
        "recurrence_density_bar",
    )
    rows = con.execute(
        f"""
        SELECT bar, feature, ROUND(value, 4) AS value
        FROM ts_bar
        WHERE song_id=? AND feature IN {keys}
        ORDER BY bar, feature
        LIMIT ?
        """,
        [args.song, args.bars * len(keys)],
    ).fetchall()
    print(f"\nFirst {args.bars} bars (selected features):")
    cur_bar = None
    for bar, feat, val in rows:
        if bar != cur_bar:
            cur_bar = bar
            print(f"\nBar {bar}")
        print(f"  {feat}: {val}")

    tags = con.execute(
        "SELECT tag_type, tag, ROUND(confidence, 4) FROM tags_section WHERE song_id=? ORDER BY tag_type, confidence DESC",
        [args.song],
    ).fetchall()
    print("\nTags:")
    for t in tags:
        print(f"- {t[0]}: {t[1]} ({t[2]})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())



