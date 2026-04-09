# Hugging Face Spaces: how it works (and your API key)

## What runs where

- The **Space** is a small **Linux container** that runs your Gradio app (`app.py`).
- When someone opens the Space URL, their **browser** loads the Gradio **frontend** (HTML/JS). That UI talks to **your container over HTTPS** (Gradio’s API routes like `/gradio_api/*`).
- **MIDI uploads** go to **your container**: the file is processed in memory/disk there, not in the visitor’s browser Python.

## If you do **not** set `OPENAI_API_KEY`

- The secret is simply **absent** from the container environment.
- The app **cannot** call OpenAI on the server—there is no key to use.
- Visitors still get **template captions**, **tables**, **audio preview**, and the **“Bring your own LLM”** export (prompt + JSON) to paste into **their** model locally.

So: **nobody can “use your key”** if you never add one.

## If you **do** set `OPENAI_API_KEY` (Space secret)

- The key exists **only inside the container** as an environment variable.
- It is **not** sent to the browser and **not** shown in the UI.
- It **is** used when a visitor checks **“Use LLM for caption”** and runs the pipeline—each such run triggers a **server-side** request to OpenAI **using your billing**.
- To limit cost, use `OPENAI_MAX_CALLS` (Docker default **5**), optionally plus `OPENAI_MAX_CALLS_PER_HOUR` for a rolling-window cap—**either** limit can block the next call. See [DEMO.md](DEMO.md).

## “Bring your own LLM” (no shared key)

The Space can **output text** (prompt + JSON). The visitor copies that into ChatGPT, Claude, a local Ollama model, etc. **Their** API keys stay on **their** side. Your Space never sees those keys.

This is the recommended pattern for a **public** demo if you do not want to fund everyone’s LLM usage.
