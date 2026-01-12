FROM python:3.12-slim

ARG FDB_VERSION=7.3.69
ARG CODEX_VERSION=1.8.19
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates git runit nodejs npm procps jq && \
    curl -fsSL "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb" -o /tmp/fdb-clients.deb && \
    curl -fsSL "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-server_${FDB_VERSION}-1_amd64.deb" -o /tmp/fdb-server.deb && \
    apt-get install -y --no-install-recommends /tmp/fdb-clients.deb /tmp/fdb-server.deb && \
    rm /tmp/fdb-clients.deb /tmp/fdb-server.deb && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN npm install -g @openai/codex
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN mkdir -p /etc/sv/fdbserver/log /etc/sv/fdb-config/log /etc/sv/api/log /etc/sv/collector/log

COPY docker/fdbserver-run.sh /etc/sv/fdbserver/run
COPY docker/fdbserver-log-run.sh /etc/sv/fdbserver/log/run
COPY docker/fdb-config-run.sh /etc/sv/fdb-config/run
COPY docker/fdb-config-log-run.sh /etc/sv/fdb-config/log/run
COPY docker/api-run.sh /etc/sv/api/run
COPY docker/api-log-run.sh /etc/sv/api/log/run
COPY docker/collector-run.sh /etc/sv/collector/run
COPY docker/collector-log-run.sh /etc/sv/collector/log/run
RUN chmod +x /etc/sv/fdbserver/run /etc/sv/fdbserver/log/run /etc/sv/fdb-config/run /etc/sv/fdb-config/log/run /etc/sv/api/run /etc/sv/api/log/run /etc/sv/collector/run /etc/sv/collector/log/run && \
    mkdir -p /etc/service && \
    ln -s /etc/sv/fdbserver /etc/service/fdbserver && \
    ln -s /etc/sv/fdb-config /etc/service/fdb-config && \
    ln -s /etc/sv/api /etc/service/api && \
    ln -s /etc/sv/collector /etc/service/collector

WORKDIR /workspace
COPY . /workspace

CMD ["runsvdir", "-P", "/etc/service"]
