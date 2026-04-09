# Model and asset files

Large binary assets are **not** committed to Git (GitHub file limit ~100 MB; keeps clones small).

## SoundFont (recommended for WAV rendering)

The web demo and `scripts/render_midi.py` use FluidSynth when a `.sf2` path is available.

**Linux (Debian/Ubuntu)**

```bash
sudo apt install fluid-soundfont-gm
export SF2_PATH=/usr/share/sounds/sf2/FluidR3_GM.sf2
```

**Windows / macOS**

1. Download a General MIDI SoundFont (e.g. [MuseScore General](https://musescore.org/en/handbook/3/soundfonts-and-virtual-instruments) or another GM `.sf2` you are licensed to use).
2. Save it as `midi_models/FluidR3_GM.sf2`, **or** set `SF2_PATH` to the full path of your file.

If no SoundFont is present, PrettyMIDI falls back to a simple built-in synthesizer (lower quality).

## Essentia TensorFlow weights (optional)

Files such as `discogs-effnet-bs64-1.pb` and the MTG Jamendo genre/mood models are used only by the **optional** audio tagging path in `extractors/audio.py` when those paths are configured. They are not required for the Gradio demo, which runs primarily on **symbolic** MIDI features.

To obtain compatible models, see the [Essentia models documentation](https://essentia.upf.edu/models.html) and MTG’s published weights; place `.pb` and `.json` files here and wire paths via `AudioConfig` if you enable that pipeline.

## Other files in this folder

- `config.cfg`, `instruments.csv`, `*.json` — small metadata used by optional tooling.
- `pipeline.py` — legacy / reference script for an older Essentia-based workflow; not imported by `app.py` or `cli.py`.
