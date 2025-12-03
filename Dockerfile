FROM python:3.12-slim-bookworm AS dev

RUN apt-get update && \
    apt-get install --no-install-recommends -y build-essential curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod -R 655 /install.sh && /install.sh && rm /install.sh

ENV PATH="/root/.local/bin:$PATH"

COPY . /flowders
COPY pyproject.toml uv.lock /flowders/

WORKDIR /flowders

RUN uv sync --frozen --no-cache

ENV PATH="/flowders/.venv/bin:$PATH"

#CMD ["uv", "run", "-m", "flowders"]
#CMD ["tail", "-f", "/dev/null"]