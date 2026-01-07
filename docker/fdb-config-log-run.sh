#!/usr/bin/env bash
set -euo pipefail

exec 2>&1

exec svlogd -tt .
