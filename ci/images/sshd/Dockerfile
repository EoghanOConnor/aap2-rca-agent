# CI jumpbox stand-in: an sshd+rsync server that runs as an arbitrary UID
# under OpenShift's restricted SCC. Built by the batch-cronjob workflow via
# a binary build; no real jumpbox or bastion is involved.
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      openssh-server \
      rsync \
      libnss-wrapper \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /usr/local/mlflow \
    && chmod 777 /usr/local/mlflow

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 2222
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
