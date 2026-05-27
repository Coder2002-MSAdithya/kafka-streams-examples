#!/usr/bin/env bash
# Deprecated: single combined broker+controller. Use start-controller.sh + start-broker-*.sh instead.
set -euo pipefail
echo "This script started the old single-node broker." >&2
echo "For the 2-broker + 1-controller layout, run:" >&2
echo "  ./start-controller.sh   # terminal 1" >&2
echo "  ./start-broker-1.sh     # terminal 2" >&2
echo "  ./start-broker-2.sh     # terminal 3" >&2
exit 1
