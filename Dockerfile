# MIDIPHOR — Hugging Face Spaces (Docker SDK)
# https://huggingface.co/docs/hub/spaces-sdks-gradio

FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    fluidsynth \
    libfluidsynth-dev \
    fluid-soundfont-gm \
    libsndfile1 \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p cache

EXPOSE 7860

ENV GRADIO_SERVER_NAME="0.0.0.0"
ENV GRADIO_SERVER_PORT="7860"
ENV SF2_PATH="/usr/share/sounds/sf2/FluidR3_GM.sf2"
ENV CACHE_DIR="cache"

CMD ["python", "app.py"]
