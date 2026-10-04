#!/usr/bin/env bash
# 운영 환경의 조회/검토/계정/ACL 진입점. 자동 서비스 재시작 없음.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")" && pwd)
export KAFKA_HOME="$HOME/kafka/current"
: "${JAVA_HOME:?JDK17 JAVA_HOME 지정 필요}"
exec python3 "$ROOT/scripts/security.py" "$@"
