# 운영 환경 작업 절차: __consumer_offsets RF 1 → 3

**간편 실행:** `~/kafka/current` 설치 환경은 [단일 .sh 실행 안내](ONE-SCRIPT.md)를 사용하세요. 아래는 세부 수동 절차입니다.

대상: **Red Hat/Linux, Apache Kafka 3.9.1, KRaft, 물리 서버 3대에 broker 각 1개**.
목적: 실행 중인 producer/consumer와 broker를 계획적으로 중지하지 않고 기존 offsets replica에 2개를 추가합니다.
정확한 토픽명은 `__consumer_offsets`입니다. 이 문서는 운영 환경 적용용이며 운영 서버에서 직접 실행·검증한 기록은 아닙니다.

> 아래 번호 순서로 **한 단계씩** 실행하고 통과 조건을 확인합니다. 전체 문서를 한 번에 붙여 실행하지 마세요.
> NAS 검증은 성공했지만 운영의 오류·지연 0건을 보장하지 않습니다. coordinator 이동이나 복제 부하로 commit 지연/재시도가 생길 수 있습니다.

## 0. 작업 범위와 준비물

- 명령은 Kafka 관리자 권한이 있는 **서버 1대의 Bash 터미널**에서만 실행합니다. 3대에서 같은 재할당을 반복 실행하지 않습니다.
- 운영 환경 bootstrap 주소, 실제 broker ID 3개, Kafka/JDK 경로, 필요 시 기존 TLS/SASL client.properties를 준비합니다.
- JDK 17 전체(`javac` 포함), Python 3.8 이상, Kafka 3.9.1 배포본이 필요합니다. Python 추가 패키지나 인터넷 접속은 필요 없습니다.
- GitHub 저장소 전체를 ZIP으로 내려받아 운영 환경 반입 절차에 따라 복사합니다. `scripts/`와 `tests/`도 함께 옮깁니다. helper가 Kafka 배포본의 Java 라이브러리로 컴파일합니다.
- **운영 서버에서는 `scripts/lab.py`를 실행하지 않습니다.** 운영 offsets 토픽 삭제/재생성, 파티션 수 변경, offset reset, 강제 leader election, broker 재시작을 이 작업에 포함하지 않습니다.
- 작업 담당자·변경 시간·관측 시간과 중단 기준(허용 lag, commit 지연/오류, disk/network 부하)을 사전에 정합니다. 이 값은 운영 환경의 평소 지표와 SLA로 정합니다.

## 1. 운영 환경 입력 — 클러스터 변경 없음

압축을 푼 `consumer-offsets-rf1-to-rf3` 폴더로 이동합니다. 아래 경로 예시는 반드시 실제 경로로 바꿉니다.
`KAFKA_HOME`은 **bin과 libs가 들어 있는 설치 최상위 폴더**입니다. server.properties가 들어 있는 폴더가 아닙니다.

```bash
cd /반입경로/kafka-operations/consumer-offsets-rf1-to-rf3
export JAVA_HOME=/실제/JDK17/경로
export KAFKA_HOME="$HOME/kafka/current"
export PATH="$JAVA_HOME/bin:$PATH"
# 이 터미널에서 실행하는 관리 CLI의 힙이며 기존 broker JVM에는 영향을 주지 않습니다.
export KAFKA_HEAP_OPTS='-Xms64m -Xmx256m'
umask 077
read -r -p 'bootstrap 주소 3개 (쉼표 구분, broker 포트): ' BOOTSTRAP
read -r -p 'client.properties 절대경로 (인증 없으면 -): ' AUTH_CONFIG
read -r -p '증거 저장 상위 디렉터리 절대경로: ' WORK_ROOT
mkdir -p "$WORK_ROOT"
WORK_DIR=$(mktemp -d "$WORK_ROOT/offsets-rf3.XXXXXXXX")
PLAN_DIR="$WORK_DIR/plan"
# PLAN_DIR 자체는 미리 만들지 않습니다. 계획 도구가 새로 생성합니다.
AUTH_ARGS=()
if [[ "$AUTH_CONFIG" != '-' ]]; then
  test -r "$AUTH_CONFIG" || { echo '인증 파일을 읽을 수 없음'; exit 1; }
  AUTH_ARGS=(--command-config "$AUTH_CONFIG")
fi
set -o pipefail
"$JAVA_HOME/bin/java" -version
"$JAVA_HOME/bin/javac" -version
python3 --version
"$KAFKA_HOME/bin/kafka-topics.sh" --version
printf '작업 증거 경로: %s\n' "$WORK_DIR"
```

**통과 조건:** Java/javac 17, Python 3.8+, Kafka 3.9.1. bootstrap에는 controller 전용 포트를 사용하지 않습니다.
같은 터미널에서 이후 단계를 수행합니다. 터미널이 끊기면 실제 변수값과 기존 WORK_DIR/PLAN_DIR를 복원하고 현재 진행 상태부터 확인합니다.
인증 파일과 운영 환경 주소가 포함된 원본 출력은 사내 보관하며 GitHub에 올리지 않습니다.

## 2. 사전 점검 — 하나라도 비정상이면 실행 보류

```bash
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" | tee "$WORK_DIR/brokers-before.txt"
"$KAFKA_HOME/bin/kafka-metadata-quorum.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" describe --status | tee "$WORK_DIR/quorum-before.txt"
"$KAFKA_HOME/bin/kafka-metadata-quorum.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" describe --replication | tee "$WORK_DIR/quorum-replication-before.txt"
"$KAFKA_HOME/bin/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" --list | tee "$WORK_DIR/reassignments-before.txt"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" --topic __consumer_offsets --describe | tee "$WORK_DIR/offsets-before.txt"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" --describe --unavailable-partitions | tee "$WORK_DIR/unavailable-before.txt"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" --describe --under-replicated-partitions | tee "$WORK_DIR/under-replicated-before.txt"
read -r -p '관측할 실제 운영 consumer group.id: ' GROUP_ID
"$KAFKA_HOME/bin/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" --describe --group "$GROUP_ID" | tee "$WORK_DIR/group-before.txt"
```

**통과 조건(모두 확인):**

- broker 3개가 정상 응답하며 실제 물리 서버와 ID 대응을 확인했습니다. ID를 `1,2,3`으로 가정하지 않습니다.
- controller leader가 있고 quorum 복제가 정상입니다. 장애·lag 증가 중이 아닙니다.
- 다른 재할당이 없습니다. unavailable/under-replicated 파티션이 없습니다.
- offsets의 **모든 파티션**에서 `Replicas` 1개, `Isr`에 같은 ID 1개, `Leader`도 그 ID입니다. 50개라고 가정하지 않고 실제 개수를 확인합니다.
- 관측할 중요 그룹들의 lag/commit 오류·지연/rebalance 기준값을 확보했습니다. 그룹 1개의 정상 여부만으로 전체 정상이라 판단하지 않습니다.
- broker별 디스크 여유, I/O 대기, 네트워크, JVM/GC를 확인했습니다. RF 증가 후 각 broker에 offsets 전체 데이터가 배치되므로 추가 복제량과 compaction 여유 공간을 계산했습니다.
- 기존 throttle/quota 및 동시 운영 작업이 없습니다. 아래 계획 도구도 기존 throttle을 검사하지만 OS 자원과 업무 영향은 자동 판정하지 않습니다.

`__consumer_offsets`는 원래 RF=1이므로 복제가 끝나기 전 유일한 원본 broker를 중지하지 않습니다.

## 3. 원본 저장과 RF=3 계획 생성 — 클러스터 변경 없음

```bash
python3 scripts/reassign.py plan --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$PLAN_DIR" | tee "$WORK_DIR/plan.log"
python3 -m json.tool "$PLAN_DIR/original.json"
python3 -m json.tool "$PLAN_DIR/target.json"
sha256sum "$PLAN_DIR/before.json" "$PLAN_DIR/original.json" "$PLAN_DIR/target.json" | tee "$WORK_DIR/plan.sha256"
```

생성 파일:

| 파일 | 내용 |
| --- | --- |
| before.json | cluster ID, broker ID, 원래 replica/leader/ISR, 설정 |
| original.json | 변경 전 assignment. 자동 복구용으로 실행하지 않음 |
| target.json | 모든 offsets 파티션의 RF=3 재할당 계획 |

예를 들어 broker ID가 11,22,33이고 partition 0의 원래 replica가 22이면 `[22,11,33]`처럼 원본을 첫 위치에 보존합니다. 예시 ID를 그대로 입력하지 않습니다.

**통과 조건:** 원본/목표 파티션 수가 같고 offsets만 포함, 각 목표 replica는 실제 broker ID 3개이며 중복 없음, 첫 ID는 원본과 같음. cluster ID와 서버 매핑도 확인합니다.
원본 파일은 별도 사내 백업에 보존합니다. 현재 helper는 전체 파티션 RF=1→3 전용입니다. 혼합 RF나 부분 batch 계획이면 중지하고 별도 검토합니다.

## 4. dry-run — 클러스터 변경 없음

```bash
python3 scripts/reassign.py execute --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$PLAN_DIR" | tee "$WORK_DIR/dry-run.log"
```

`execute`라는 이름이지만 **`--apply`가 없으면 조회·검증만 합니다.** `DRY RUN`과 계획 SHA256이 출력돼야 합니다.
계획 생성 이후 broker/cluster ID/assignment/원본 ISR 변경이나 다른 재할당이 발견되면 중단됩니다. 오류를 무시하거나 Python `-O`로 우회하지 않습니다.

## 5. 실제 RF 변경 — 이 단계부터 클러스터를 변경

작업 담당자가 2~4단계를 확인한 후 실행합니다. 기존 producer/consumer는 계속 실행합니다.

```bash
read -r -p '검토 완료한 target.json SHA256: ' PLAN_SHA
read -r -p '승인한 broker 복제 한도 bytes/sec (양의 정수): ' THROTTLE
python3 scripts/reassign.py execute --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$PLAN_DIR" --throttle "$THROTTLE" --apply --reviewed-sha256 "$PLAN_SHA" | tee "$WORK_DIR/execute.log"
```

이 명령은 Kafka 배포본의 `kafka-reassign-partitions.sh --execute`를 호출합니다. 일반 토픽/offset 값 자체를 수정하는 작업이 아닙니다.
**명령 접수 성공은 복제 완료가 아닙니다.** CLI timeout/실패도 서버 측 작업 미실행을 뜻하지 않습니다. 다음 단계로 현재 상태를 조회하고, 새 계획으로 무작정 재실행하지 않습니다.

throttle 단위는 bytes/sec입니다. NAS의 `1048576`(1MiB/s)은 실험값이며 운영 추천값이 아닙니다.
너무 작으면 새 replica가 유입되는 offsets 데이터를 따라잡지 못하고, 너무 크면 운영 I/O에 영향을 줄 수 있습니다. 부하와 여유를 보고 결정합니다.

## 6. 진행 확인 — 완료 전까지 관측

아래 조회를 적절한 간격으로 반복합니다. 예를 들어 30초 간격으로 시작하되 운영 부하에 맞춰 조정합니다.

```bash
python3 scripts/reassign.py verify --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$PLAN_DIR"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" --topic __consumer_offsets --describe
"$KAFKA_HOME/bin/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" --describe --group "$GROUP_ID"
```

이 단계의 verify는 **throttle을 유지**합니다. 재할당 중 새 replica가 ISR에 아직 없을 수 있으나 기존 원본 replica는 정상이어야 합니다.
애플리케이션 send/commit 오류·지연, 처리량, rebalance, group lag와 broker 자원을 함께 확인합니다.
원본 ISR 이탈, leader 부재, SLA 초과, 디스크 부족, 복제 정체 시 추가 변경을 보류하고 9단계를 따릅니다.

## 7. 완료 검증 후 throttle 해제 — 설정 변경

```bash
python3 scripts/reassign.py check --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$PLAN_DIR"
```

모든 파티션 RF=3·ISR=3이고 진행 중 재할당이 없어야 합니다. 다음 명령은 목표 assignment까지 재검증한 뒤 throttle을 해제합니다.
**다른 담당자가 새로운 throttle 작업을 시작하지 않았는지 먼저 확인합니다.** 도구는 동시 작업 간 설정 소유권을 보장하지 않습니다.

```bash
python3 scripts/reassign.py verify --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$PLAN_DIR" --apply | tee "$WORK_DIR/verify-clear.log"
python3 scripts/reassign.py snapshot --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$WORK_DIR/after.json"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" --topic __consumer_offsets --describe | tee "$WORK_DIR/offsets-after.txt"
"$KAFKA_HOME/bin/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" --list | tee "$WORK_DIR/reassignments-after.txt"
```

**통과 조건:** 모든 파티션의 replica 3개와 ISR 3개가 일치, leader가 ISR 안에 존재, target replica 순서와 일치, 재할당 없음.
`after.json`의 topic/broker throttle 설정을 `before.json`과 비교하여 원래 값으로 복귀했는지 확인합니다.

## 8. 업무 정상 확인 및 작업 종료

- 합의한 관측 시간 동안 중요 consumer group lag가 평소 범위로 유지/회복되는지 확인합니다.
- producer 전송, consumer 실제 업무 처리, offset commit, 오류·지연·rebalance를 확인합니다. 토픽 RF 표시만 보고 완료하지 않습니다.
- 운영 consumer 재시작은 요구하지 않습니다. NAS에서 한 재접속 검증은 운영 변경의 필수 단계가 아닙니다.
- 작업 시각, 명령 로그, 원본/목표/완료 assignment와 지표를 사내 작업 기록에 첨부합니다.

`server.properties`의 `offsets.topic.replication.factor=3` 설정만 바꾸면 **이미 만들어진 토픽 RF는 바뀌지 않습니다.** 이번 재할당에는 broker 재시작이 필요 없습니다.
향후 생성 기본값을 맞추기 위한 설정 파일 정리는 별도 작업입니다. 해당 broker 설정은 read-only이므로 런타임 반영을 위한 재시작은 추후 승인된 rolling maintenance에서 진행합니다.
`min.insync.replicas`, retention/cleanup, controller quorum 구성 변경은 이번 작업에 섞지 않습니다.

## 9. 실패·지연·취소 판단

1. 먼저 6단계 조회와 `--list`로 실제 상태를 확인합니다. 로그와 원본/목표 JSON을 보존합니다.
2. 복제 정체면 target broker 상태, 디스크, 네트워크, 인증/ACL, replication lag를 조사합니다. 유일한 원본 replica를 종료하지 않습니다.
3. throttle 조정이 필요한 경우에만 담당자 판단으로 **동일 target.json**에 새 한도를 적용합니다. 다른 재할당이 없는지 재확인합니다.

```bash
read -r -p '검토한 새 복제 한도 bytes/sec: ' NEW_THROTTLE
"$KAFKA_HOME/bin/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" --reassignment-json-file "$PLAN_DIR/target.json" --execute --additional --throttle "$NEW_THROTTLE"
```

취소가 필요하다고 판단한 경우에만 아래 명령을 사용합니다. 일반 성공 절차에서는 실행하지 않습니다.

```bash
"$KAFKA_HOME/bin/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" "${AUTH_ARGS[@]}" --reassignment-json-file "$PLAN_DIR/target.json" --cancel --preserve-throttles
```

취소는 **진행 중인 해당 파티션의 재할당**을 대상으로 하며 이미 완료된 RF=3을 RF=1로 되돌리지 않습니다.
이후 assignment/ISR/진행 상태와 남은 throttle을 조회하고 변경 소유자와 정리합니다. 취소 후에는 전체 RF=3을 요구하는 helper `verify --apply`가 실패할 수 있습니다.
원본 `original.json`을 재실행하는 RF=1 rollback은 내구성을 낮춥니다. 자동 실행하지 말고 원본 broker가 최신 ISR인지, 부분 완료 상태와 장애 위험을 확인한 별도 복구 계획을 세웁니다.
토픽 삭제, offset reset, unclean election은 이 절차의 복구 수단이 아닙니다.

## 완료 체크리스트

- [ ] 3개 broker와 KRaft quorum 정상
- [ ] offsets 전체 파티션이 목표 RF=3·ISR=3
- [ ] 진행 중 재할당 없음
- [ ] topic/broker throttle 원복 확인
- [ ] 중요 그룹의 commit·lag·업무 처리 정상
- [ ] producer/consumer와 broker를 계획적으로 중지하지 않음
- [ ] 원본/목표/완료 증거 및 지연·오류 여부 기록

근거: [Kafka 3.9 공식 재할당·RF 증가·throttle 안내](https://kafka.apache.org/39/operations/basic-kafka-operations/),
[공식 broker 설정](https://kafka.apache.org/39/configuration/broker-configs/#offsets.topic.replication.factor),
[상세 운영 판단](RUNBOOK.md), [NAS 실제 결과와 한계](RESULTS.md).
