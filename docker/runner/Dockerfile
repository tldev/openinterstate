FROM rust:1.88-bookworm

COPY tooling/requirements.txt /tmp/openinterstate-tooling-requirements.txt

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        gh \
        jq \
        osm2pgsql \
        osmium-tool \
        postgresql-client \
        python3 \
        python3-pip \
        python3-venv \
    && python3 -m venv /opt/openinterstate-venv \
    && /opt/openinterstate-venv/bin/pip install --no-cache-dir -r /tmp/openinterstate-tooling-requirements.txt \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/openinterstate-venv/bin:${PATH}"

WORKDIR /workspace
