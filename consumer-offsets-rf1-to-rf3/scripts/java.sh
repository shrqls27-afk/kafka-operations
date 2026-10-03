#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_HOME:?Kafka 3.9.1 설치 경로 필요}"
: "${JAVA_HOME:?JDK 17 경로 필요}"
ROOT=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$ROOT/build"
"$JAVA_HOME/bin/javac" -cp "$KAFKA_HOME/libs/*" -d "$ROOT/build" "$ROOT/tests/OffsetsLab.java"
exec "$JAVA_HOME/bin/java" -Xms64m -Xmx192m -cp "$ROOT/build:$KAFKA_HOME/libs/*" OffsetsLab "$@"
