#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_HOME:?Kafka 3.9.1 설치 경로 필요}"
: "${JAVA_HOME:?JDK 17 경로 필요}"
ROOT=$(cd "$(dirname "$0")/.." && pwd)
KEY=$(python3 -c 'import hashlib, pathlib, sys; h=hashlib.sha256(); h.update(pathlib.Path(sys.argv[1]).read_bytes()); h.update(str(pathlib.Path(sys.argv[2]).resolve()).encode()); h.update(pathlib.Path(sys.argv[3],"release").read_bytes()); print(h.hexdigest())' "$ROOT/tests/OffsetsLab.java" "$KAFKA_HOME" "$JAVA_HOME")
CACHE="$ROOT/build/$KEY"
mkdir -p "$CACHE"
exec 9>"$ROOT/build/compile.lock"
flock 9
if [[ ! -f "$CACHE/compiled" ]]; then
    "$JAVA_HOME/bin/javac" -encoding UTF-8 -cp "$KAFKA_HOME/libs/*" -d "$CACHE" "$ROOT/tests/OffsetsLab.java"
    touch "$CACHE/compiled"
fi
flock -u 9
exec 9>&-
exec "$JAVA_HOME/bin/java" -Dfile.encoding=UTF-8 -Xms64m -Xmx192m -cp "$CACHE:$KAFKA_HOME/libs/*" OffsetsLab "$@"
