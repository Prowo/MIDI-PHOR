# Contributing

Thanks for your interest in improving MIDIPHOR.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Install a General MIDI SoundFont for best audio (see `midi_models/README.md`).

## Running checks

- Start the demo: `python app.py` (see `docs/DEMO.md` for Windows output tips).
- CLI batch flow: `python cli.py --help`

## Pull requests

- Keep changes focused; match existing style and imports.
- Do not commit large binaries (`.sf2`, `.pb`); document acquisition in `midi_models/README.md` instead.
- Update `README.md` or `docs/` if behavior or deployment steps change.

## License

By contributing, you agree your contributions are licensed under the MIT License (`LICENSE`).
