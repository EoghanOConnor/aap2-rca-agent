#!/bin/bash
# Unprivileged sshd for OpenShift: the container may run as an arbitrary UID,
# so everything writable lives in /tmp, the listener is on a high port, and
# nss_wrapper makes that UID resolvable as a named user (a non-root sshd can
# only authenticate its own UID).
set -euo pipefail

PORT="${SSHD_PORT:-2222}"
USER_NAME="${SSHD_USER:-testuser}"
RUN_UID="$(id -u)"
RUN_GID="$(id -g)"
SCRATCH=/tmp/sshd
mkdir -p "$SCRATCH"

ssh-keygen -t ed25519 -N "" -f "$SCRATCH/host_key" -q

cat > "$SCRATCH/sshd_config" <<EOF
Port $PORT
ListenAddress 0.0.0.0
HostKey $SCRATCH/host_key
PidFile $SCRATCH/sshd.pid
UsePAM no
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
AuthorizedKeysFile /keys/id_ed25519.pub
StrictModes no
AllowTcpForwarding yes
X11Forwarding no
PrintMotd no
Subsystem sftp internal-sftp
EOF

export LD_PRELOAD=libnss_wrapper.so
export NSS_WRAPPER_PASSWD="$SCRATCH/passwd"
export NSS_WRAPPER_GROUP="$SCRATCH/group"
{
  echo "$USER_NAME:x:$RUN_UID:$RUN_GID::/tmp:/bin/bash"
  echo "root:x:0:0::/tmp:/bin/bash"
} > "$NSS_WRAPPER_PASSWD"
{
  echo "$USER_NAME:x:$RUN_GID:"
  echo "root:x:0:"
} > "$NSS_WRAPPER_GROUP"

echo "[SSHD] listening on $PORT as $USER_NAME (uid $RUN_UID)"
exec /usr/sbin/sshd -D -e -f "$SCRATCH/sshd_config"
