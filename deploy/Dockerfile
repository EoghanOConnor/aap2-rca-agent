FROM python:3.12-slim

# git/openssh-client/rsync support bastion fetches; jq is used by Claude
# settings hooks and the packaged root-cause-analysis instructions.
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    openssh-client \
    rsync \
    jq \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/app

COPY . /opt/app/

# The Agent SDK bundles a native Claude CLI binary; expose it for Agent SDK
# queries and legacy compatibility without installing Node or npm.
RUN pip install --no-cache-dir /opt/app
RUN CLI_PATH="$(python -c 'from importlib.resources import files; print(files("claude_agent_sdk").joinpath("_bundled", "claude"))')" \
    && ln -s "$CLI_PATH" /usr/local/bin/claude

WORKDIR /workspace

RUN mkdir -p /workspace/.config/gcloud /workspace/.rca \
    && touch /workspace/.config/gcloud/application_default_credentials.json \
    && chmod -R g+rwX /workspace \
    && chgrp -R 0 /workspace

ENV HOME=/workspace \
    RCA_STATE_DIR=/workspace/.rca

ENTRYPOINT ["/bin/bash"]
