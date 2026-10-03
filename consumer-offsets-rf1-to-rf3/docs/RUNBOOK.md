# 회사 적용 시작 문서: Linux / Red Hat, Kafka 3.9.1 KRaft

범위: 물리 서버 3대의 실제 broker ID를 조회하여 **기존 `__consumer_offsets`의 모든 파티션 RF=1을 RF=3으로 확대**합니다. producer/consumer는 재할당 구간에 계획적으로 중지하지 않습니다. 원격 회사 적용은 수행하지 않았습니다. 변경 창과 담당자, 보류 기준은 회사에서 승인한 값을 사용하세요.

## 사전점검과 보류 조건

- 3개 broker, KRaft quorum 및 controller가 정상이고 모두 같은 대상 클러스터인지 확인합니다. broker ID를 1,2,3으로 가정하지 않습니다. 각 물리 서버/rack에 replica가 하나씩 배치되도록 실제 ID/장애영역을 검토합니다.
- 정확한 내부 토픽 이름은 `__consumer_offsets`입니다. 파티션 수를 바꾸거나 토픽을 삭제/재생성하지 않습니다. 원래 RF=1인 파티션의 유일한 replica와 leader, ISR가 정상이어야 합니다.
- 이미 진행 중인 모든 재할당, 기존 throttle/quota, offline/under-replicated partitions가 있으면 보류합니다. 이 도구는 기존 throttle이 있으면 별도 조정을 요구하며 실행을 거부합니다.
- broker별 offsets log 크기(승인된 OS 경로 또는 kafka-log-dirs), disk 사용량/IO latency, 네트워크, heap/GC, replication fetcher lag를 측정합니다. 합계 데이터가 RF=1 때의 대략 3배가 되므로 각 broker별 추가 복제와 임시 공간/compaction 여유를 계산합니다. 단순 토픽 합계만으로 공간을 판단하지 않습니다.
- 그룹 목록과 committed offset, lag, commit 오류/지연, rebalance 빈도, 애플리케이션 처리 간격 및 producer retry/error 기준을 수집합니다. 시작 전 baseline과 비교할 허용값/관측 기간을 확정합니다. 회사 부하/데이터 크기를 모르는 상태에서 NAS의 수치를 그대로 허용값으로 쓰지 않습니다.
- TLS/SASL/ACL 인증 파일은 별도 보호 경로에 두고 최소 권한을 부여합니다. Admin 조회와 토픽 ALTER, throttle 변경을 위한 cluster/broker config 권한을 확인합니다. 비밀 값이나 인증 파일을 Git 또는 공유 evidence로 복사하지 않습니다. 출력에 포함된 host/client-id도 회사 외 공유 전에 익명화합니다.
- resource 여유 부족, 원본 ISR 이탈/leader 부재, 클러스터 ID/assignment 변화, 동시 변경, lag 증가/commit SLA 초과/복제 진전 없음이면 다음 단계를 보류합니다. 자동 반복 재실행/원본 RF=1 복구는 하지 않습니다.

## 환경과 조회 명령

JDK 17, Python 3.8+, Bash, Kafka 3.9.1 배포본을 준비합니다. 인증은 운영용 `--command-config` 경로를 입력받습니다. PLAINTEXT 격리 실험은 `-`를 사용하며 회사에 강제하지 않습니다.

```bash
cd consumer-offsets-rf1-to-rf3
export JAVA_HOME=/opt/jdk-17
export KAFKA_HOME=/opt/kafka_2.13-3.9.1
export KAFKA_HEAP_OPTS='-Xms64m -Xmx256m'
read -r -p '대상 bootstrap servers: ' BOOTSTRAP
read -r -p '인증 properties 절대경로 (-는 인증 없음): ' AUTH_CONFIG
read -r -p '새 계획/증거 디렉터리: ' PLAN_DIR
# 예시 변수는 실제 회사 주소/비밀 값이 아니다.
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" --command-config "$AUTH_CONFIG"
"$KAFKA_HOME/bin/kafka-metadata-quorum.sh" --bootstrap-server "$BOOTSTRAP" --command-config "$AUTH_CONFIG" describe --status
"$KAFKA_HOME/bin/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --list
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --topic __consumer_offsets --describe
"$KAFKA_HOME/bin/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --all-groups --describe
```

인증 없는 경우 위 직접 CLI 예시에서는 `--command-config "$AUTH_CONFIG"` 두 인자를 생략합니다. Python wrapper는 `-`를 자동 처리합니다. systemd 서비스명과 log 경로는 회사 설치에 맞게 확인하며 스크립트는 서비스를 자동 재시작하지 않습니다.

## 조회 → 계획 검토 → 실행

```bash
python3 scripts/reassign.py plan --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$PLAN_DIR"
python3 scripts/reassign.py execute --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$PLAN_DIR"
```

위 두 번째 명령은 **dry-run**입니다. `before.json`에는 cluster ID, 실제 broker ID, 모든 파티션 replica/leader/ISR와 throttle 설정이, `original.json`에는 복구 검토용 assignment가 저장됩니다. `target.json`은 원래 replica를 첫 위치에 두고 나머지 실제 broker 2개를 추가합니다. 파일은 신규 생성만 하므로 기존 증거를 덮어쓰지 않습니다. 세 파일과 SHA256을 별도 보존하고 모든 파티션, 중복 ID 부재, 원래 replica 보존, RF=3과 3개 물리 서버 배치를 검토합니다. rack-aware 자동 생성에 맡기는 절차가 아닙니다.

실행 직전 다시 조회하여 cluster ID/broker/원본 replica/ISR가 바뀌지 않았는지 검사합니다. 기존 throttle이 없다 해도 실행 직전 동시 운영 작업이 없는지 확인해야 합니다. 검토 후 출력된 해시를 직접 입력합니다.

```bash
read -r -p '검토한 target.json SHA256: ' PLAN_SHA
read -r -p 'broker 복제 throttle bytes/sec: ' THROTTLE
python3 scripts/reassign.py execute --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$PLAN_DIR" --throttle "$THROTTLE" --apply --reviewed-sha256 "$PLAN_SHA"
```

throttle은 broker 단위 복제 트래픽 한도입니다. 다른 토픽에도 관련 설정이 영향을 줄 수 있습니다. 유입 commit 데이터량과 disk/network 여유보다 충분한 값을 정하고 관찰하며 조정합니다. 실험의 1MiB/s는 운영 권장값이 아닙니다. 운영 규모가 크면 승인된 파티션 batch별 별도 JSON을 작성하고 batch마다 검증해야 하며 현재 도구는 전체 파티션 계획을 실행하므로 임의 편집한 부분 계획은 거부합니다.

## 진행과 완료 확인, throttle 해제

```bash
# 조회 전용: throttle 유지
python3 scripts/reassign.py verify --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$PLAN_DIR"
# 모든 파티션 RF=3 ISR=3, leader가 replica 안에 존재, 진행 중 재할당 없음
python3 scripts/reassign.py check --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$PLAN_DIR"
# 완료 확인 후 throttle 해제 (이 명령은 config 변경을 수행)
python3 scripts/reassign.py verify --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$PLAN_DIR" --apply
python3 scripts/reassign.py snapshot --bootstrap "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --directory "$PLAN_DIR/after.json"
```

`check`의 성공과 별도로 target.json과 after.json의 **모든 partition replica 순서/leader/ISR**를 비교하세요. `--verify`는 계획 assignment 완료를 확인하지만 ISR 안정성 관찰을 대체하지 않습니다. after.json에서 topic throttle 목록과 broker rates가 원래 상태로 돌아왔는지 확인하고 kafka-configs로도 조회합니다. 기존 다른 작업의 throttle이 생겼다면 단순 clear하지 말고 소유 담당자와 조정합니다. 정상 lag/commit/error/latency가 합의한 관찰 기간 동안 유지되는지 보세요. 운영 consumer 재접속을 변경 절차에 강제하지 않습니다. NAS의 재접속은 별도 검증입니다.

## 설정 변경과 재시작 구분

현재 RF 변경은 **재할당만으로 완료**됩니다. 실행 broker 설정이 `offsets.topic.replication.factor=1`인 상태에서도 기존 토픽은 RF=3이 됩니다. 별도로 설정 파일을 3으로 정리하면 향후 토픽 생성 정책이 일치합니다. 이 값은 read-only이므로 실행 broker가 새 값을 읽으려면 재시작이 필요합니다. 이 작업에 불필요한 재시작을 묶지 말고 향후 승인된 rolling maintenance에서 quorum/ISR 및 애플리케이션 영향을 확인해 적용하세요. 토픽 삭제/재생성으로 설정을 반영하지 마세요. min.insync.replicas 조정은 가용성/내구성 trade-off가 별도이므로 이번 작업에 자동 포함하지 않습니다.

## 실패/정체 시 대응

1. CLI timeout은 서버 측 미실행을 뜻하지 않습니다. 먼저 `--list`, 동일 JSON `--verify --preserve-throttles`, topic describe와 group/commit 지표를 조회합니다. 새 계획으로 덮어쓰거나 무작정 execute를 반복하지 않습니다.
2. 복제 lag가 줄지 않으면 target broker health, disk/network/ACL, FetcherLagMetrics와 controller 로그를 조사합니다. 원본 유일 replica를 중지하지 않습니다. throttle이 너무 작으면 승인 후 아래 공식 명령으로 현재 계획의 한도를 조정할 수 있습니다.
3. 보류/취소가 필요하면 **진행 중인 이 계획의 파티션만** 대상으로 `--cancel --preserve-throttles`를 검토합니다. 취소는 이미 완료된 RF 증가를 되돌리는 기능이 아닙니다. 현재 assignment/ISR/진행 상태를 재조회하고 다른 작업과 throttle 소유권을 확인한 뒤 잔여 throttle을 정리합니다.

```bash
"$KAFKA_HOME/bin/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --reassignment-json-file "$PLAN_DIR/target.json" --execute --additional --throttle "$NEW_THROTTLE"
# 별도 운영 판단 후에만 실행하는 취소 예시
"$KAFKA_HOME/bin/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" --command-config "$AUTH_CONFIG" --reassignment-json-file "$PLAN_DIR/target.json" --cancel --preserve-throttles
```

원본 assignment를 다시 execute하면 **RF=1로 감소하여 추가 내구성을 버립니다**. 자동 rollback하지 않습니다. 토픽을 RF=1로 되돌려야 할 명백한 원인과 승인, 유지할 원본 broker가 살아 있고 최신 committed offsets를 포함해 ISR에 있는 조건, 정체/부분 완료 상태, 다른 재할당 충돌 여부를 모두 확인해야 합니다. 원본 replica가 offline/stale이면 원본 JSON을 blindly 실행하지 않습니다. RF=3인 채 원인 해결이 가능한지 먼저 검토합니다. RF 감소 뒤 broker 장애는 offsets 접근 불가/소비 재처리 위험을 확대합니다. unclean election, offsets reset, 토픽 삭제는 이 절차의 복구 수단이 아닙니다.

## 예상 영향과 한계

추가 복제로 disk/network/heap와 controller 부하가 늘 수 있습니다. 기존 preferred replica를 보존해도 leader/coordinator 이동, metadata/offset 로딩 및 client 재탐색/재시도로 일시 commit 지연, timeout과 rebalance가 생길 수 있습니다. 계획적 애플리케이션 중지 없음과 오류/지연 한 번도 없음은 다른 기준입니다. 회사의 client 버전, retry/timeout/max.poll.interval, 부하, 장애영역과 토픽 크기를 확인한 변경 창이 필요합니다. 물리 3대의 장애 내성은 NAS 한 대 안의 3개 JVM으로 증명할 수 없습니다.
