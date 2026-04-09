"""
MIDIPHOR Web Demo - Gradio App with Pipeline Visualization
Generates captions from MIDI files using symbolic analysis + LLM.

Shows each pipeline step and displays extracted data tables.
"""
from __future__ import annotations
import json
import os
import uuid
from pathlib import Path
from typing import Any, Tuple
import pandas as pd

import gradio as gr

_ROOT = Path(__file__).resolve().parent


def _resolve_soundfont() -> str:
    """Prefer SF2_PATH; else common local paths; else Debian package path."""
    env = os.environ.get("SF2_PATH")
    if env and Path(env).is_file():
        return env
    candidates = [
        _ROOT / "midi_models" / "FluidR3_GM.sf2",
        Path("/usr/share/sounds/sf2/FluidR3_GM.sf2"),
    ]
    for p in candidates:
        if p.is_file():
            return str(p)
    return str(_ROOT / "midi_models" / "FluidR3_GM.sf2")


# ---------- Configuration ----------
SF2_PATH = _resolve_soundfont()
CACHE_DIR = os.environ.get("CACHE_DIR", "cache")

_OPENAI_KEY = bool(os.environ.get("OPENAI_API_KEY", "").strip())
_OPENAI_MAX = os.environ.get("OPENAI_MAX_CALLS", "").strip()


def _llm_quota_path() -> Path:
    custom = os.environ.get("LLM_QUOTA_PATH")
    if custom:
        return Path(custom)
    return Path(CACHE_DIR) / ".openai_llm_calls"


def _read_llm_use_count() -> int:
    p = _llm_quota_path()
    try:
        return max(0, int(p.read_text(encoding="utf-8").strip()))
    except Exception:
        return 0


def _commit_llm_success() -> None:
    """Increment counter after a successful OpenAI caption call."""
    if not _OPENAI_MAX:
        return
    p = _llm_quota_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    n = _read_llm_use_count() + 1
    p.write_text(str(n), encoding="utf-8")


def llm_calls_remaining() -> int | None:
    """None if unlimited; else remaining quota."""
    if not _OPENAI_MAX:
        return None
    limit = int(_OPENAI_MAX)
    return max(0, limit - _read_llm_use_count())


def llm_quota_allows() -> tuple[bool, str]:
    """Whether another LLM call is allowed under OPENAI_MAX_CALLS."""
    if not _OPENAI_MAX:
        return True, ""
    if _read_llm_use_count() >= int(_OPENAI_MAX):
        return (
            False,
            "LLM quota for this public demo is used up. Captions still work via the **template**.",
        )
    return True, ""


# Default LLM checkbox: off if no key; off if capped demo unless USE_LLM=true; else env
if not _OPENAI_KEY:
    _DEFAULT_USE_LLM = False
elif _OPENAI_MAX:
    _DEFAULT_USE_LLM = os.environ.get("USE_LLM", "false").lower() == "true"
else:
    _DEFAULT_USE_LLM = os.environ.get("USE_LLM", "true").lower() == "true"

EXAMPLE_MIDI = _ROOT / "examples" / "lmd_dancing_queen.mid"
_EMPTY_GRAPH_JSON: dict[str, list] = {"nodes": [], "edges": []}


# ---------- Pipeline Step Definitions ----------
PIPELINE_STEPS = [
    {"id": "upload", "name": "Upload", "icon": "📁", "desc": "MIDI file uploaded"},
    {"id": "symbolic", "name": "Symbolic Extraction", "icon": "🎼", "desc": "Extract notes, chords, keys, tempo"},
    {"id": "sections", "name": "Section Analysis", "icon": "📊", "desc": "Identify musical sections"},
    {"id": "graph", "name": "Orchestration Graph", "icon": "🔗", "desc": "Analyze instrument relationships"},
    {"id": "slots", "name": "Feature Aggregation", "icon": "📋", "desc": "Build feature slots"},
    {"id": "caption", "name": "Caption Generation", "icon": "✍️", "desc": "Generate natural language"},
]


def create_pipeline_html(current_step: int, step_status: dict) -> str:
    """Generate HTML for pipeline visualization."""
    html = """
    <style>
        .pipeline-container {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px 10px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 12px;
            margin-bottom: 20px;
        }
        .pipeline-step {
            display: flex;
            flex-direction: column;
            align-items: center;
            flex: 1;
            position: relative;
        }
        .pipeline-step:not(:last-child)::after {
            content: '';
            position: absolute;
            top: 25px;
            right: -50%;
            width: 100%;
            height: 3px;
            background: rgba(255,255,255,0.3);
            z-index: 0;
        }
        .pipeline-step.completed:not(:last-child)::after {
            background: #10b981;
        }
        .pipeline-step.active:not(:last-child)::after {
            background: linear-gradient(90deg, #10b981 50%, rgba(255,255,255,0.3) 50%);
        }
        .step-icon {
            width: 50px;
            height: 50px;
            border-radius: 50%;
            background: rgba(255,255,255,0.2);
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 24px;
            z-index: 1;
            transition: all 0.3s;
        }
        .pipeline-step.completed .step-icon {
            background: #10b981;
            box-shadow: 0 0 15px rgba(16, 185, 129, 0.5);
        }
        .pipeline-step.active .step-icon {
            background: #f59e0b;
        }
        .pipeline-step.pending .step-icon {
            opacity: 0.5;
        }
        .step-name {
            color: white;
            font-size: 11px;
            margin-top: 8px;
            text-align: center;
            font-weight: 500;
        }
        .pipeline-step.pending .step-name {
            opacity: 0.5;
        }
    </style>
    <div class="pipeline-container">
    """
    
    for i, step in enumerate(PIPELINE_STEPS):
        status = step_status.get(step["id"], "pending")
        if i < current_step:
            status = "completed"
        elif i == current_step:
            status = "active"
        
        html += f"""
        <div class="pipeline-step {status}">
            <div class="step-icon">{step['icon']}</div>
            <div class="step-name">{step['name']}</div>
        </div>
        """
    
    html += "</div>"
    return html


def query_table(con, query: str, limit: int = 10) -> pd.DataFrame:
    """Execute query and return DataFrame."""
    try:
        result = con.execute(query).fetchdf()
        if len(result) > limit:
            result = result.head(limit)
        return result
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})


# ---------- Lazy imports for heavy modules ----------

def get_modules():
    """Lazy load heavy modules."""
    from db.duck import connect, ensure_schema
    from extractors import symbolic
    from extractors import graph as graph_ext
    from assemble.section_merge import merge_for_song
    from assemble.slots import build_slots
    from assemble.llm_prompt import generate_caption_openai, build_caption_prompt
    from assemble.caption import caption_for_song
    from scripts.render_midi import render
    
    return {
        'connect': connect,
        'ensure_schema': ensure_schema,
        'symbolic': symbolic,
        'graph_ext': graph_ext,
        'merge_for_song': merge_for_song,
        'build_slots': build_slots,
        'build_caption_prompt': build_caption_prompt,
        'generate_caption_openai': generate_caption_openai,
        'caption_for_song': caption_for_song,
        'render': render,
    }


def _slots_to_json(slots: dict[str, Any]) -> str:
    """Serialize feature slots for copy/paste (e.g. into your own LLM or tooling)."""

    def norm(x: Any) -> Any:
        if isinstance(x, tuple):
            return [norm(i) for i in x]
        if isinstance(x, list):
            return [norm(i) for i in x]
        if isinstance(x, dict):
            return {k: norm(v) for k, v in x.items()}
        if isinstance(x, (str, int, float, bool)) or x is None:
            return x
        return str(x)

    return json.dumps(norm(slots), indent=2, ensure_ascii=False)


def _df_for_gradio(df: pd.DataFrame | None) -> pd.DataFrame:
    """Gradio shows bogus placeholder rows/columns when value is None or empty."""
    if df is None:
        return pd.DataFrame({"—": ["Upload a MIDI file and run the pipeline (or use the Example)."]})
    try:
        if df.empty:
            return pd.DataFrame({"—": ["No rows for this table in this piece."]})
    except Exception:
        return pd.DataFrame({"—": ["(could not read table)"]})
    out = df.copy()
    for c in out.columns:
        if str(out[c].dtype) == "object":
            out[c] = out[c].apply(lambda x: "" if x is None else x)
    return out


def _build_graph_json_and_figures(con, song_id: str, work_dir: str) -> tuple[dict, str | None, str | None, str | None]:
    """Orchestration graph as JSON + three static figures (saved PNG paths)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import networkx as nx

    os.makedirs(work_dir, exist_ok=True)
    graph_json: dict[str, list] = {"nodes": [], "edges": []}
    p_chords: str | None = None
    p_roll: str | None = None
    p_graph: str | None = None

    try:
        ndf = con.execute(
            "SELECT node_id, node_type, role, family FROM graph_nodes WHERE song_id=? ORDER BY node_id",
            [song_id],
        ).fetchdf()
        graph_json["nodes"] = ndf.to_dict(orient="records")
    except Exception:
        pass
    try:
        edf = con.execute(
            "SELECT src_node_id, dst_node_id, rel_type, strength FROM graph_edges WHERE song_id=? ORDER BY strength DESC NULLS LAST",
            [song_id],
        ).fetchdf()
        graph_json["edges"] = edf.to_dict(orient="records")
    except Exception:
        pass

    # Figure 1: chord / RN events over time
    try:
        cdf = con.execute(
            """
            SELECT onset_bar, onset_beat, COALESCE(rn, name, '?') AS label
            FROM chords WHERE song_id=? ORDER BY onset_bar, onset_beat LIMIT 80
            """,
            [song_id],
        ).fetchdf()
        if cdf is not None and not cdf.empty:
            x = cdf["onset_bar"].astype(float) + cdf["onset_beat"].astype(float) / 4.0
            fig, ax = plt.subplots(figsize=(10, 2.8))
            ax.scatter(x, range(len(x)), c="#6366f1", s=42, alpha=0.85)
            for i, (_, row) in enumerate(cdf.iterrows()):
                ax.text(float(x.iloc[i]), i, str(row["label"])[:10], fontsize=7, va="center", ha="left")
            ax.set_xlabel("Approx. position (bar + beat/4)")
            ax.set_ylabel("Chord index")
            ax.set_title("Chord / Roman-numeral timeline (sample)")
            ax.grid(True, alpha=0.25)
            fig.tight_layout()
            p_chords = os.path.join(work_dir, f"{song_id}_chords.png")
            fig.savefig(p_chords, dpi=120)
            plt.close(fig)
    except Exception:
        pass

    # Figure 2: piano roll (sample of notes)
    try:
        ndf = con.execute(
            """
            SELECT onset_bar, onset_beat, pitch, velocity
            FROM notes WHERE song_id=? ORDER BY onset_bar, onset_beat LIMIT 400
            """,
            [song_id],
        ).fetchdf()
        if ndf is not None and not ndf.empty:
            xb = ndf["onset_bar"].astype(float) + ndf["onset_beat"].astype(float) / 4.0
            fig, ax = plt.subplots(figsize=(10, 4))
            sc = ax.scatter(xb, ndf["pitch"], c=ndf["velocity"], cmap="viridis", s=8, alpha=0.65)
            plt.colorbar(sc, ax=ax, label="velocity")
            ax.set_xlabel("Approx. position (bar + beat/4)")
            ax.set_ylabel("MIDI pitch")
            ax.set_title("Piano roll (sample of notes)")
            ax.grid(True, alpha=0.2)
            fig.tight_layout()
            p_roll = os.path.join(work_dir, f"{song_id}_roll.png")
            fig.savefig(p_roll, dpi=120)
            plt.close(fig)
    except Exception:
        pass

    # Figure 3: graph layout
    try:
        if graph_json["nodes"] and graph_json["edges"]:
            g = nx.DiGraph()
            for n in graph_json["nodes"]:
                nid = n.get("node_id")
                if nid:
                    g.add_node(str(nid), label=n.get("role") or n.get("node_type") or "")
            for e in graph_json["edges"]:
                s, d = e.get("src_node_id"), e.get("dst_node_id")
                if s and d:
                    g.add_edge(str(s), str(d), w=float(e.get("strength") or 0.1))
            if len(g.nodes) > 0:
                fig, ax = plt.subplots(figsize=(8, 6))
                pos = nx.spring_layout(g, seed=42, k=0.35)
                nx.draw_networkx_nodes(g, pos, ax=ax, node_color="#a78bfa", node_size=520, alpha=0.9)
                nx.draw_networkx_edges(g, pos, ax=ax, edge_color="#94a3b8", arrows=True, arrowsize=12)
                labels = {n: (g.nodes[n].get("label") or n)[:12] for n in g.nodes}
                nx.draw_networkx_labels(g, pos, labels=labels, ax=ax, font_size=7)
                ax.set_title("Orchestration graph (nodes = parts, edges = relations)")
                ax.axis("off")
                fig.tight_layout()
                p_graph = os.path.join(work_dir, f"{song_id}_graph.png")
                fig.savefig(p_graph, dpi=120)
                plt.close(fig)
    except Exception:
        pass

    return graph_json, p_chords, p_roll, p_graph


def _write_demo_exports(
    export_dir: str,
    caption: str,
    prompt_export: str,
    slots_json: str,
    graph_json: dict,
) -> list[str]:
    """Write JSON/text artifacts for copy-paste and download (CC-BY / research demo)."""
    Path(export_dir).mkdir(parents=True, exist_ok=True)
    paths: list[str] = []
    graph_txt = json.dumps(graph_json, indent=2, ensure_ascii=False)

    for name, body in (
        ("feature_slots.json", slots_json),
        ("orchestration_graph.json", graph_txt),
        ("caption.txt", caption or ""),
        ("caption_prompt.txt", prompt_export or ""),
    ):
        p = os.path.join(export_dir, name)
        Path(p).write_text(body, encoding="utf-8")
        paths.append(p)

    try:
        bundle_obj: dict[str, Any] = {
            "midiphor_export_version": 1,
            "caption": caption,
            "caption_prompt": prompt_export,
            "feature_slots": json.loads(slots_json) if slots_json.strip() else {},
            "orchestration_graph": graph_json,
        }
    except json.JSONDecodeError:
        bundle_obj = {
            "midiphor_export_version": 1,
            "caption": caption,
            "caption_prompt": prompt_export,
            "feature_slots_raw": slots_json,
            "orchestration_graph": graph_json,
        }
    pb = os.path.join(export_dir, "midiphor_export.json")
    Path(pb).write_text(json.dumps(bundle_obj, indent=2, ensure_ascii=False), encoding="utf-8")
    paths.append(pb)
    return paths


# ---------- Core Pipeline with Progress ----------

def process_midi(midi_file: str, use_llm: bool = True) -> Tuple:
    """
    Process a MIDI file through the MIDIPHOR pipeline.

    Returns:
        pipeline_html, status, tables, caption, audio_path,
        export_prompt, export_slots_json,
        orchestration_graph_json, paths to three figure PNGs (or None),
        list of written export file paths (JSON/txt) for download.
    """
    if not midi_file:
        return (
            create_pipeline_html(0, {}),
            "⚠️ No file uploaded",
            {}, "", None, "", "",
            _EMPTY_GRAPH_JSON, None, None, None,
            [],
        )
    
    # Lazy load modules
    modules = get_modules()
    
    # Create temporary database for this request
    song_id = f"demo_{uuid.uuid4().hex[:8]}"
    db_path = os.path.join(CACHE_DIR, f"{song_id}.duckdb")
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    tables = {}
    step_status = {}
    
    try:
        # Connect and initialize schema
        con = modules['connect'](db_path)
        modules['ensure_schema'](con)
        
        # Step 0: Upload complete
        step_status["upload"] = "completed"
        
        # Step 1: Symbolic extraction
        try:
            modules['symbolic'].run(song_id, midi_file, con)
            step_status["symbolic"] = "completed"
            
            # Query extracted tables
            tables["songs"] = query_table(con, f"SELECT * FROM songs WHERE song_id='{song_id}'")
            tables["tracks"] = query_table(con, f"SELECT track_id, name, gm_program, role FROM tracks WHERE song_id='{song_id}'")
            tables["notes"] = query_table(con, f"SELECT note_id, track_id, pitch, velocity, onset_bar, dur_beats FROM notes WHERE song_id='{song_id}' ORDER BY onset_bar LIMIT 15")
            tables["chords"] = query_table(con, f"SELECT chord_id, onset_bar, name, rn, quality FROM chords WHERE song_id='{song_id}' ORDER BY onset_bar LIMIT 12")
            tables["bars"] = query_table(con, f"SELECT bar, start_sec, end_sec, num, den, qpm FROM bars WHERE song_id='{song_id}' LIMIT 10")
            tables["key_changes"] = query_table(con, f"SELECT at_bar, key, confidence FROM key_changes WHERE song_id='{song_id}'")
            
        except Exception as e:
            return (
                create_pipeline_html(1, step_status),
                f"❌ Symbolic extraction failed: {e}",
                tables, "", None, "", "",
                _EMPTY_GRAPH_JSON, None, None, None,
                [],
            )
        
        # Step 2: Section merging
        try:
            modules['merge_for_song'](con, song_id)
            step_status["sections"] = "completed"
            
            tables["sections"] = query_table(con, f"SELECT section_id, type, start_bar, end_bar, source FROM sections WHERE song_id='{song_id}'")
            tables["bar_metrics"] = query_table(con, f"SELECT bar, density, polyphony, backbeat_strength, syncopation FROM bar_metrics WHERE song_id='{song_id}' LIMIT 10")
        except Exception as e:
            step_status["sections"] = "skipped"
            tables["sections"] = pd.DataFrame({"note": ["Section analysis skipped"]})
            tables["bar_metrics"] = pd.DataFrame()
        
        # Step 3: Graph analysis
        try:
            modules['graph_ext'].run(song_id, con)
            step_status["graph"] = "completed"
            
            tables["graph_nodes"] = query_table(con, f"SELECT node_id, node_type, role, family FROM graph_nodes WHERE song_id='{song_id}'")
            tables["graph_edges"] = query_table(con, f"SELECT src_node_id, dst_node_id, rel_type, strength FROM graph_edges WHERE song_id='{song_id}' LIMIT 10")
        except Exception as e:
            step_status["graph"] = "skipped"
            tables["graph_nodes"] = pd.DataFrame({"note": ["Graph analysis skipped"]})
            tables["graph_edges"] = pd.DataFrame()
        
        # Step 4: Build feature slots
        slots = modules['build_slots'](con, song_id, section_id=None)
        step_status["slots"] = "completed"
        
        # Format slots as a table
        slots_data = {
            "Feature": ["Meter", "Tempo", "Key", "Bars", "Progression", "Rhythm", "Energy (z)", "Density", "Polyphony", "Instruments", "Tags"],
            "Value": [
                slots.get("meter", "N/A"),
                f"{slots.get('tempo_bpm', 'N/A')} BPM",
                slots.get("key", "N/A"),
                f"{slots.get('bars', (0,0))[0]} - {slots.get('bars', (0,0))[1]}",
                slots.get("progression", "N/A"),
                slots.get("rhythm_trait") or "N/A",
                f"{slots.get('energy_z', 0):.2f}" if slots.get('energy_z') else "N/A",
                f"{slots.get('density', 0):.2f}" if slots.get('density') else "N/A",
                f"{slots.get('polyphony', 0):.2f}" if slots.get('polyphony') else "N/A",
                ", ".join(slots.get("instruments_summary", [])[:4]) or "N/A",
                ", ".join(slots.get("tags", [])) or "N/A",
            ]
        }
        tables["feature_slots"] = pd.DataFrame(slots_data)

        # Export pack for "bring your own LLM" (no server API key required to use this text)
        prompt_export = modules["build_caption_prompt"](con, song_id, section_id=None, style="medium")
        slots_json = _slots_to_json(slots)

        fig_dir = os.path.join(CACHE_DIR, song_id, "figs")
        orch_json, p_chords, p_roll, p_graph = _build_graph_json_and_figures(con, song_id, fig_dir)

        # Step 5: Generate caption
        quota_note = ""
        if use_llm and _OPENAI_KEY:
            ok, quota_msg = llm_quota_allows()
            if not ok:
                use_llm = False
                quota_note = quota_msg
        if use_llm and _OPENAI_KEY:
            try:
                caption = modules["generate_caption_openai"](con, song_id, section_id=None, style="medium")
                _commit_llm_success()
            except Exception as e:
                caption = modules["caption_for_song"](con, song_id)
                caption += f"\n\n(LLM error: {e})"
        else:
            caption = modules["caption_for_song"](con, song_id)
            if quota_note:
                caption += f"\n\n{quota_note}"
            elif use_llm and not _OPENAI_KEY:
                caption += "\n\n(This deployment has no LLM API key; using template caption.)"
        
        step_status["caption"] = "completed"

        export_dir = os.path.join(CACHE_DIR, song_id, "export")
        try:
            export_paths = _write_demo_exports(
                export_dir, caption, prompt_export, slots_json, orch_json
            )
        except Exception:
            export_paths = []

        # Step 6: Render audio
        audio_path = os.path.join(CACHE_DIR, f"{song_id}.wav")
        sf2 = SF2_PATH if Path(SF2_PATH).is_file() else None
        try:
            modules['render'](midi_file, audio_path, sf2=sf2)
        except Exception as e:
            audio_path = None
        
        con.close()
        
        # Clean up temp database
        try:
            os.remove(db_path)
        except Exception:
            pass
        
        status_msg = "✅ Pipeline complete!"
        rem = llm_calls_remaining()
        if _OPENAI_KEY and rem is not None:
            status_msg += f" (LLM calls remaining: {rem})"

        return (
            create_pipeline_html(6, step_status),
            status_msg,
            tables,
            caption,
            audio_path if audio_path and os.path.exists(audio_path) else None,
            prompt_export,
            slots_json,
            orch_json,
            p_chords,
            p_roll,
            p_graph,
            export_paths,
        )

    except Exception as e:
        return (
            create_pipeline_html(0, step_status),
            f"❌ Error: {str(e)}",
            tables, "", None, "", "",
            _EMPTY_GRAPH_JSON, None, None, None,
            [],
        )


# ---------- Gradio Interface ----------

def run_pipeline(midi_file, use_llm_checkbox: bool):
    """Main Gradio handler."""
    empty_tables = [_df_for_gradio(None)] * 11
    if midi_file is None:
        empty_graph_txt = json.dumps(_EMPTY_GRAPH_JSON, indent=2, ensure_ascii=False)
        return (
            create_pipeline_html(0, {}),
            "Upload a MIDI file to begin",
            *empty_tables,
            "",
            None,
            "",
            "",
            empty_graph_txt,
            None,
            _EMPTY_GRAPH_JSON,
            None,
            None,
            None,
        )

    use_llm = bool(use_llm_checkbox) and _OPENAI_KEY
    (
        pipeline_html,
        status,
        tables,
        caption,
        audio,
        prompt_export,
        slots_json,
        graph_json,
        p_chords,
        p_roll,
        p_graph,
        export_paths,
    ) = process_midi(midi_file, use_llm=use_llm)

    graph_json_text = json.dumps(graph_json, indent=2, ensure_ascii=False)

    return (
        pipeline_html,
        status,
        _df_for_gradio(tables.get("songs")),
        _df_for_gradio(tables.get("tracks")),
        _df_for_gradio(tables.get("notes")),
        _df_for_gradio(tables.get("chords")),
        _df_for_gradio(tables.get("bars")),
        _df_for_gradio(tables.get("key_changes")),
        _df_for_gradio(tables.get("sections")),
        _df_for_gradio(tables.get("bar_metrics")),
        _df_for_gradio(tables.get("graph_nodes")),
        _df_for_gradio(tables.get("graph_edges")),
        _df_for_gradio(tables.get("feature_slots")),
        caption,
        audio,
        prompt_export,
        slots_json,
        graph_json_text,
        export_paths if export_paths else None,
        graph_json,
        p_chords,
        p_roll,
        p_graph,
    )


# ---------- Build the Gradio app ----------

custom_css = """
.gradio-container { max-width: 1200px !important; }
.caption-box { font-size: 1.1em; line-height: 1.6; }
"""

with gr.Blocks(title="MIDIPHOR Demo") as demo:
    
    gr.Markdown("""
    # 🎹 MIDIPHOR: MIDI Caption Generator
    
    Upload a MIDI file to see the full analysis pipeline in action. Watch as each step extracts 
    musical features and builds toward a natural language caption.
    """)

    if not _OPENAI_KEY:
        gr.Markdown(
            "*This deployment has **no OpenAI API key**: captions use the built-in **template** only. "
            "Symbolic extraction, tables, audio preview, and **JSON/text exports** (copy or download) still run. "
            "Open **Exports** below to copy the prompt and feature JSON into ChatGPT / Claude / a local model—**your keys stay on your machine**.*"
        )
    elif _OPENAI_MAX:
        gr.Markdown(
            f"*Optional LLM captions use a server-side key and are limited to **{int(_OPENAI_MAX)}** successful API calls "
            f"total (`OPENAI_MAX_CALLS`). Remaining count is shown after each run; template captions are always available.*"
        )

    if EXAMPLE_MIDI.is_file():
        gr.Markdown(
            f"*Quick try: use the **Examples** section at the bottom with `{EXAMPLE_MIDI.name}`, or upload any `.mid` file.*"
        )
        if EXAMPLE_MIDI.name == "lmd_dancing_queen.mid":
            gr.Markdown(
                "*Bundled example: **Lakh MIDI Dataset** (Clean MIDI subset), [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/). "
                "Citation: [dataset page](https://colinraffel.com/projects/lmd/) and Colin Raffel, PhD thesis, 2016 — see `examples/README.md`.*"
            )
    
    # Pipeline visualization
    pipeline_display = gr.HTML(create_pipeline_html(0, {}))
    status_display = gr.Markdown("Upload a MIDI file to begin")
    
    _llm_info = None
    if _OPENAI_KEY and _OPENAI_MAX:
        _llm_info = (
            f"Uses server key; capped at {int(_OPENAI_MAX)} successful calls (shared). "
            "Template is used when off or when quota is exhausted."
        )
    elif _OPENAI_KEY:
        _llm_info = "Uses the server's API key. Turn off for template-only captions."

    with gr.Row():
        with gr.Column(scale=1):
            midi_input = gr.File(
                label="Upload MIDI File",
                file_types=[".mid", ".midi"],
                type="filepath"
            )
            use_llm = gr.Checkbox(
                label="Use LLM for caption",
                value=_DEFAULT_USE_LLM,
                visible=_OPENAI_KEY,
                interactive=_OPENAI_KEY,
                info=_llm_info,
            )
            submit_btn = gr.Button("🚀 Run Pipeline", variant="primary", size="lg")
        
        with gr.Column(scale=1):
            audio_output = gr.Audio(
                label="🔊 Rendered Audio",
                type="filepath",
                interactive=False
            )
    
    # Caption output (readable); select text to copy, or use caption.txt in downloads
    caption_output = gr.Textbox(
        label="✍️ Generated Caption",
        lines=4,
        max_lines=8,
        elem_classes=["caption-box"],
        placeholder="Run the pipeline to generate a template or LLM caption.",
    )

    with gr.Accordion("📋 Exports — copy, download, or bring your own LLM", open=False):
        gr.Markdown(
            "Use **Code** blocks to select all and copy. **Download** gives the same files on disk: "
            "`feature_slots.json`, `orchestration_graph.json`, `caption.txt`, `caption_prompt.txt`, "
            "and **`midiphor_export.json`** (single bundle: caption + prompt + slots + graph). "
            "Nothing here is sent to OpenAI unless you enabled the server-side LLM checkbox above."
        )
        export_prompt = gr.Code(
            label="caption_prompt.txt — copy for ChatGPT / Claude / local LLM",
            language="markdown",
            lines=14,
            max_lines=28,
            interactive=False,
            wrap_lines=True,
        )
        export_slots = gr.Code(
            label="feature_slots.json — copy",
            language="json",
            lines=12,
            max_lines=28,
            interactive=False,
            wrap_lines=True,
        )
        export_graph_code = gr.Code(
            label="orchestration_graph.json — copy",
            language="json",
            lines=14,
            max_lines=32,
            interactive=False,
            wrap_lines=True,
        )
        export_files = gr.File(
            label="Download JSON & text exports",
            file_count="multiple",
            type="filepath",
            interactive=False,
        )
    
    gr.Markdown("---")
    gr.Markdown("## 📊 Extracted Data Tables")
    gr.Markdown("*Explore the data extracted at each pipeline step*")
    
    # Data tables in accordion sections
    with gr.Accordion("🎼 Step 1: Symbolic Extraction", open=True):
        with gr.Row():
            with gr.Column():
                gr.Markdown("**Song Metadata**")
                tbl_songs = gr.Dataframe(label="songs", interactive=False, wrap=True)
            with gr.Column():
                gr.Markdown("**Key Detection**")
                tbl_keys = gr.Dataframe(label="key_changes", interactive=False, wrap=True)
        with gr.Row():
            with gr.Column():
                gr.Markdown("**Tracks (Instruments)**")
                tbl_tracks = gr.Dataframe(label="tracks", interactive=False, wrap=True)
            with gr.Column():
                gr.Markdown("**Bars (Time Grid)**")
                tbl_bars = gr.Dataframe(label="bars", interactive=False, wrap=True)
        with gr.Row():
            with gr.Column():
                gr.Markdown("**Notes (Sample)**")
                tbl_notes = gr.Dataframe(label="notes", interactive=False, wrap=True)
            with gr.Column():
                gr.Markdown("**Chords**")
                tbl_chords = gr.Dataframe(label="chords", interactive=False, wrap=True)
    
    with gr.Accordion("📊 Step 2: Section Analysis", open=False):
        with gr.Row():
            with gr.Column():
                gr.Markdown("**Sections**")
                tbl_sections = gr.Dataframe(label="sections", interactive=False, wrap=True)
            with gr.Column():
                gr.Markdown("**Bar Metrics**")
                tbl_bar_metrics = gr.Dataframe(label="bar_metrics", interactive=False, wrap=True)
    
    with gr.Accordion("🔗 Step 3: Orchestration Graph", open=False):
        gr.Markdown(
            "**JSON** is the same structure you can pass to downstream tools (`nodes` / `edges`). "
            "**Figures** are quick matplotlib previews from this run."
        )
        graph_json_out = gr.JSON(label="Orchestration graph (JSON)")
        with gr.Row():
            with gr.Column():
                fig_chords = gr.Image(label="Chord / RN timeline", type="filepath", interactive=False)
            with gr.Column():
                fig_roll = gr.Image(label="Piano roll (sample)", type="filepath", interactive=False)
        fig_graph = gr.Image(label="Graph layout", type="filepath", interactive=False)
        with gr.Row():
            with gr.Column():
                gr.Markdown("**Graph Nodes (Instruments)**")
                tbl_graph_nodes = gr.Dataframe(label="graph_nodes", interactive=False, wrap=True)
            with gr.Column():
                gr.Markdown("**Graph Edges (Relationships)**")
                tbl_graph_edges = gr.Dataframe(label="graph_edges", interactive=False, wrap=True)
    
    with gr.Accordion("📋 Step 4: Feature Slots", open=False):
        gr.Markdown("**Aggregated features used for caption generation**")
        tbl_slots = gr.Dataframe(label="feature_slots", interactive=False, wrap=True)
    
    # Event handlers
    outputs = [
        pipeline_display,
        status_display,
        tbl_songs,
        tbl_tracks,
        tbl_notes,
        tbl_chords,
        tbl_bars,
        tbl_keys,
        tbl_sections,
        tbl_bar_metrics,
        tbl_graph_nodes,
        tbl_graph_edges,
        tbl_slots,
        caption_output,
        audio_output,
        export_prompt,
        export_slots,
        export_graph_code,
        export_files,
        graph_json_out,
        fig_chords,
        fig_roll,
        fig_graph,
    ]

    if EXAMPLE_MIDI.is_file():
        gr.Markdown("**Try the bundled example:**")
        gr.Examples(
            examples=[[str(EXAMPLE_MIDI), False]],
            inputs=[midi_input, use_llm],
            outputs=outputs,
            fn=run_pipeline,
            cache_examples=False,
        )
    
    submit_btn.click(
        fn=run_pipeline,
        inputs=[midi_input, use_llm],
        outputs=outputs
    )
    
    midi_input.change(
        fn=run_pipeline,
        inputs=[midi_input, use_llm],
        outputs=outputs
    )
    
    gr.Markdown("""
    ---
    ### How It Works
    
    1. **Symbolic Extraction** — Parses MIDI for notes, tracks, tempo, key, and chord progressions
    2. **Section Analysis** — Identifies musical sections (intro, verse, chorus) using novelty detection
    3. **Orchestration Graph** — Maps relationships between instruments (supports, doubles, rhythmic lock)
    4. **Feature Aggregation** — Combines all features into a structured "slots" representation
    5. **Caption Generation** — Uses templates or LLM to produce natural language description
    
    [GitHub](https://github.com/Prowo/MIDI-PHOR) | [Paper (ACL)](https://aclanthology.org/2026.nlp4musa-1.6/)
    """)


# ---------- Launch ----------

if __name__ == "__main__":
    print("Starting MIDIPHOR demo server...")
    host = os.environ.get("GRADIO_SERVER_NAME", "127.0.0.1")
    port_env = os.environ.get("GRADIO_SERVER_PORT")
    server_port = int(port_env) if port_env else None
    demo.launch(
        server_name=host,
        server_port=server_port,
        share=False,
        show_error=True,
        css=custom_css,
    )
