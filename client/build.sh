#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

# Optional:
#   CLAVIRON_CHAT_PUBLIC_DIR=/path/to/public/chat ./build.sh
# Default output is ../public/chat relative to this client directory.
if [ ! -d node_modules ]; then
  npm install
fi

npm run build:min

#npm run build
