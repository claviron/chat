#!/usr/bin/env bash
set -euo pipefail

CHAT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# Use an absolute destination: the client builder resolves relative paths from client/.
export CLAVIRON_CHAT_PUBLIC_DIR="${CLAVIRON_CHAT_PUBLIC_DIR:-$CHAT_ROOT/../revolution/public/ops/chat}"

exec npm --prefix "$CHAT_ROOT/client" run build
