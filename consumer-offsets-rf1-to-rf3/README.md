# __consumer_offsets RF=1 → RF=3

Apache Kafka **3.9.1**, Linux 직접 설치, KRaft broker 3개를 대상으로 합니다. 운영 producer/consumer를 계획적으로 중지하지 않고 내부 토픽의 기존 replica를 보존한 채 replica 2개를 추가합니다. 회사 접근/적용은 수행하지 않습니다.

회사 적용은 **[단계별 실행 절차](docs/COMPANY-PROCEDURE.md)**부터 읽으세요. 상세 판단 기준은 [RUNBOOK](docs/RUNBOOK.md)에 있습니다. 실제 측정 결과는 [RESULTS](docs/RESULTS.md), 근거는 [SOURCES](docs/SOURCES.md)에 있습니다.

## 디렉터리

- `scripts/reassign.py`: 기본 조회/계획/dry-run, 별도 실행 옵션과 계획 해시 필요
- `scripts/lab.py`: 새 전용 경로/loopback 포트에서만 격리 실험
- `scripts/java.sh`, `tests/OffsetsLab.java`: Kafka 3.9.1 Java Admin/producer/consumer 검증
- `tests/test_plan.py`: 잘못된 계획/변경된 클러스터 거부 테스트
- `scripts/summarize.py`: 원본 이벤트에서 익명화된 수치와 파티션별 결과 추출
- `evidence/`: 작은 JSON 요약, assignment, 파일 해시. `runtime/`, `build/`는 Git 제외

## 전제와 재현

Python 3.8+, Bash, JDK 17 전체(JRE만으로는 컴파일 불가), Apache Kafka 3.9.1 배포본이 필요합니다. Red Hat은 승인된 경로에 직접 설치하고 아래 환경변수에 실제 경로를 지정합니다. 패키지 설치/다운로드나 서비스 변경은 스크립트가 자동 수행하지 않습니다.

```bash
export JAVA_HOME=/opt/jdk-17
export KAFKA_HOME=/opt/kafka_2.13-3.9.1
export KAFKA_HEAP_OPTS='-Xms64m -Xmx256m'
python3 -m unittest discover -s tests -v
bash -n scripts/java.sh
python3 -m py_compile scripts/*.py tests/test_plan.py
# 기본은 사전점검만. 경로는 새 전용 경로여야 함.
python3 scripts/lab.py --directory "$PWD/runtime/new-run"
python3 scripts/lab.py --run --directory "$PWD/runtime/new-run"
python3 scripts/summarize.py "$PWD/runtime/new-run" "$PWD/evidence/new-run"
```

실험은 노드 ID 101~103, 기본 broker 포트 41001~41003와 controller 포트 41004~41006를 사용합니다. `--port-base`로 변경할 수 있습니다. 실제 offsets 토픽은 stable group의 첫 coordinator 요청/commit 과정에서 broker 설정 RF=1로 자동 생성합니다. 파티션 수는 기본과 동일한 50입니다. 일반 데이터 토픽은 RF=3, min ISR=2입니다. producer는 `acks=all`, idempotence 사용, 약 20건/초 단일 파티션 부하입니다.

baseline 45초 → 조회/계획/dry-run → 온라인 재할당(1MiB/s throttle) → 모든 파티션 RF/ISR 확인 및 throttle 해제 → after 45초 → 전송 종료/drain → consumer 재접속과 20개 추가 marker 검증 순서입니다. 초기 생성은 준비 과정이며 측정 구간에서 애플리케이션을 계획적으로 중지하지 않습니다. 재접속 검증은 재할당 완료와 drain 뒤 별도 단계에서 consumer를 의도적으로 닫고 다시 엽니다.

도구는 기본적으로 2.5GB 가용 메모리, 5GB 디스크 여유를 요구합니다. 테스트 서버는 각각 256MB 힙, 검증 JVM은 192MB 힙입니다. 가용 자원과 시스템 부하는 실행 중에도 별도로 관찰해야 합니다. 실패 시 원본 증거를 보존하고 소유 PID/명령을 확인한 후 SIGTERM만 보냅니다. 강제 종료와 자동 데이터 삭제는 하지 않습니다. 정상 종료 지연 시 `shutdown.json`의 미종료 PID를 확인하세요.

## 측정 해석

`events.jsonl`은 메시지 ID별 전송 시도/성공/실패, 소비 offset/간격, 커밋 결과/위치/시간, 2초 간격 lag 및 offsets 전체 snapshot, rebalance와 재접속 위치를 기록합니다. 실제 작업 시각은 ISO UTC, 시간 간격은 monotonic clock입니다. lag는 최신 offset과 group committed offset의 차이이며 호출 시점이 다르므로 순간값입니다.

전송 중 lag를 누락으로 판정하지 않습니다. 전송 스레드 종료 뒤 ack된 ID 집합이 소비 집합에 모두 포함되는지 drain에서 판단하며, ack 없는 소비는 별도 표시합니다. 재시도 후 producer 오류는 전송 여부가 불확실할 수 있습니다. drain은 성공 ack 집합을 기준으로 하므로 전송 오류가 있으면 그 ID를 별도로 조사해야 합니다. 실제 비즈니스 처리/DB 트랜잭션/회사의 client 설정을 대체하는 검증이 아닙니다.
