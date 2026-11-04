#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# setup-block-storage.sh — prepare durable state storage for hetzner-deadman.
#
# Two modes:
#
#   builtin (default)  Create a host directory that will hold state.json,
#                      state.db and the 4 MiB A/B checksummed state.block.
#                      Works unprivileged, no loop device, no CAP_SYS_ADMIN.
#                      This is what the Docker image uses.
#
#   loop               Create /var/lib/hetzner-deadman/state.img (ext4), mount
#                      it on /var/lib/hetzner-deadman/state, and optionally add
#                      an /etc/fstab entry.  Requires root and is only useful
#                      when you want the whole state directory to be a single
#                      image file (e.g. to copy it off-box atomically).
#
# The program NEVER requires loop mode.  Its /data/state.block is already a
# preallocated block file with two redundant, CRC32+SHA256 protected slots.
#
# Usage:
#   sudo ./setup-block-storage.sh                 # builtin, /var/lib/hetzner-deadman
#   sudo ./setup-block-storage.sh --loop          # ext4 loop image + mount
#   sudo ./setup-block-storage.sh --loop --fstab  # ... and persist in /etc/fstab
#   ./setup-block-storage.sh --dir ./data --owner "$USER"
# ---------------------------------------------------------------------------
set -euo pipefail

MODE="builtin"
BASE_DIR="/var/lib/hetzner-deadman"
MOUNT_DIR=""
SIZE_MB=16
OWNER="${SUDO_USER:-$(id -un)}"
ADD_FSTAB=0
VERIFY=0

usage() {
  sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --loop) MODE="loop"; shift ;;
    --builtin) MODE="builtin"; shift ;;
    --dir) BASE_DIR="$2"; shift 2 ;;
    --size) SIZE_MB="$2"; shift 2 ;;
    --owner) OWNER="$2"; shift 2 ;;
    --fstab) ADD_FSTAB=1; shift ;;
    --verify) VERIFY=1; shift ;;
    -h|--help) usage 0 ;;
    *) echo "unknown argument: $1" >&2; usage 1 ;;
  esac
done

MOUNT_DIR="${BASE_DIR}/state"
IMG="${BASE_DIR}/state.img"

log() { printf '[setup] %s\n' "$*"; }
die() { printf '[setup] ERROR: %s\n' "$*" >&2; exit 1; }

need_root() {
  [[ "${EUID}" -eq 0 ]] || die "this mode needs root (try: sudo $0 $*)"
}

install -d -m 0750 "${BASE_DIR}"

if [[ "${MODE}" == "builtin" ]]; then
  # Pre-create the block file so permissions are right before the container
  # starts.  The daemon will initialise the A/B headers on first write.
  if [[ ! -f "${BASE_DIR}/state.block" ]]; then
    log "creating ${BASE_DIR}/state.block (4 MiB, zeroed)"
    dd if=/dev/zero of="${BASE_DIR}/state.block" bs=1M count=4 status=none
  else
    log "keeping existing ${BASE_DIR}/state.block"
  fi
  chmod 0600 "${BASE_DIR}/state.block" || true
  chown "${OWNER}:${OWNER}" "${BASE_DIR}/state.block" 2>/dev/null || true
  log "state directory: ${BASE_DIR}"
  log "set STATE_DIR=${BASE_DIR} (or bind-mount it to /data in Docker)"
  if [[ "${VERIFY}" -eq 1 ]] && command -v python3 >/dev/null 2>&1; then
    ( cd "$(dirname "$0")" && python3 -m deadman verify-storage --state-dir "${BASE_DIR}" ) || true
  fi
  exit 0
fi

# ------------------------------------------------------------------ loop mode
need_root "$@"
command -v mkfs.ext4 >/dev/null 2>&1 || die "mkfs.ext4 not found (apt-get install e2fsprogs)"
command -v mount >/dev/null 2>&1 || die "mount not found"

if [[ ! -f "${IMG}" ]]; then
  log "creating ${IMG} (${SIZE_MB} MiB)"
  dd if=/dev/zero of="${IMG}" bs=1M count="${SIZE_MB}" status=none
  mkfs.ext4 -q -F -L hetzner-deadman "${IMG}"
else
  log "keeping existing ${IMG} (not reformatting)"
fi
chmod 0600 "${IMG}"

install -d -m 0750 "${MOUNT_DIR}"
if mountpoint -q "${MOUNT_DIR}"; then
  log "${MOUNT_DIR} is already mounted"
else
  log "mounting ${IMG} on ${MOUNT_DIR}"
  mount -o loop "${IMG}" "${MOUNT_DIR}"
fi

chown "${OWNER}:${OWNER}" "${MOUNT_DIR}" 2>/dev/null || true
install -d -m 0700 "${MOUNT_DIR}"

if [[ "${ADD_FSTAB}" -eq 1 ]]; then
  LINE="${IMG} ${MOUNT_DIR} ext4 loop,defaults,nofail 0 0"
  if grep -qF "${MOUNT_DIR}" /etc/fstab 2>/dev/null; then
    log "/etc/fstab already mentions ${MOUNT_DIR}; leaving it alone"
  else
    log "adding fstab entry"
    printf '\n# hetzner-deadman state image\n%s\n' "${LINE}" >> /etc/fstab
  fi
fi

log "state directory: ${MOUNT_DIR}"
log "set STATE_DIR=${MOUNT_DIR} (bind-mount it to /data in Docker)"
if [[ "${VERIFY}" -eq 1 ]] && command -v python3 >/dev/null 2>&1; then
  ( cd "$(dirname "$0")" && python3 -m deadman verify-storage --state-dir "${MOUNT_DIR}" ) || true
fi
