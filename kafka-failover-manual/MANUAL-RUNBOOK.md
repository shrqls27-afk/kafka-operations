# Kafka 3.9.1 한 노드씩 종료·복구: 사람이 직접 실행하는 절차

Linux/Red Hat 직접 tar 설치, KRaft, JDK17 기준이다. Docker는 필요 없다. 승인된 시험창에서만 실행한다. 장애 명령을 한꺼번에 붙여 넣지 말고 각 단계의 성공 기준을 확인한다. 기존 운영 토픽을 삭제하거나 상시 offsets RF를 낮추지 않는다. **node1 종료→검증→기동→완전 복구→node2→완전 복구→node3→완전 복구** 순서다. 누적 종료와 두 노드 이상 동시 종료는 금지한다.

Kafka3.9.1/JDK17, Bash, Python3.8+, Linux `/proc`, GNU coreutils와 필요한 systemd 조회 권한을 준비한다. Python은 입력 ID 생성·로그 대조·관측에만 사용하며 Kafka producer/consumer는 기본 CLI다.

## 0. 실행 위치·준비물

- 터미널 A: 클라이언트/관리 호스트, Kafka CLI 설치 계정. 모든 조회·진행 판정.
- 터미널 B: 같은 클라이언트 호스트·계정, producer. 시험 내내 같은 프로세스 유지.
- 터미널 C: 같은 클라이언트 호스트·계정, consumer. 시험 내내 같은 프로세스 유지.
- 서버별 SSH 터미널 S1/S2/S3: 승인받은 시험 노드의 설치 계정. 해당 노드만 종료/기동. DNS·서비스명·PID를 작업 전에 실제 대조한다.

아래 `.example` 주소와 JDK 경로는 placeholder다. 실주소는 각 터미널에서 환경변수로만 입력한다. 인증 환경에서는 topics/consumer-groups/metadata-quorum/broker-api-versions 조회에는 `--command-config "$ADMIN_CONFIG"`, **leader-election에는 `--admin.config "$ADMIN_CONFIG"`**, producer에 `--producer.config "$PRODUCER_CONFIG"`, consumer에 `--consumer.config "$CONSUMER_CONFIG"`를 추가하고 승인된 기존 파일을 사용한다. 인증 파일을 출력하거나 Git에 넣지 않는다. 실측 NAS는 loopback PLAINTEXT 격리망이며 인증을 시험하지 않았다.

A/B/C 각각 설정(같은 RUN_ID와 RUN 경로 사용):

```bash
export JAVA_HOME="$HOME/java/current"  # 실제 JDK17 설치 경로
export PATH="$JAVA_HOME/bin:$PATH"
export KAFKA_HOME="$HOME/kafka/current"
export BIN="$KAFKA_HOME/bin"
export BOOTSTRAP='broker1.example:9092,broker2.example:9092,broker3.example:9092'
export RUN_ID='failover-YYYYMMDDTHHMMSSZ'  # 새 시험마다 고유값
export TOPIC="$RUN_ID"
export GROUP="${RUN_ID}-group"
export RUN="$HOME/kafka-failover-runtime/$RUN_ID"
export KIT="$HOME/kafka-operations/kafka-failover-manual" # 문서/보조스크립트 반입 위치
```

A에서 `test ! -e "$RUN"`이 성공하는지 확인하고 `mkdir -p "$RUN"`을 실행한다. 같은 시험 디렉터리를 덮어쓰지 않는다. B/C에서는 A가 생성한 디렉터리를 사용한다.

A에서 client 전용 INFO 로그 설정을 생성한다. 시험 producer/consumer에만 적용하고 서버 설정은 변경하지 않는다.

```bash
cat > "$RUN/client-log4j.properties" <<'EOF'
log4j.rootLogger=INFO, stderr
log4j.appender.stderr=org.apache.log4j.ConsoleAppender
log4j.appender.stderr.Target=System.err
log4j.appender.stderr.layout=org.apache.log4j.PatternLayout
log4j.appender.stderr.layout.ConversionPattern=[%d] %p %m (%c)%n
EOF
```

B/C 각각 client 시작 전에:

```bash
export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$RUN/client-log4j.properties"
```

**성공 기준:** 실행 계정·DNS·승인 대상·새 RUN_ID·Kafka/JDK 버전 일치. 실패하면 실행 보류하고 경로부터 수정한다.

먼저 실행 경로를 하나 선택한다.

- **기존 승인 시험 클러스터:** 단계1 전체 gate → 단계3부터. 단계2 format/새 nohup 예시는 실행하지 않는다. 기존 운영을 대상으로 한다면 해당 장애 시험의 운영 승인과 기존 복구 절차가 필요하다.
- **신규 격리 클러스터:** 단계1의 호스트 자원·상시 서비스 보존 점검만 → 단계2 전체 format/start → broker3/quorum3 안정 gate → 단계3/4/5 → offsets 생성 후 전체 RF3/ISR3 gate → 최초 종료. 기동 전 broker 조회와 아직 미생성인 offsets를 실패로 판정하지 않는다. 같은 NAS의 상시 cluster는 별도 bootstrap으로 단계1 전체 건강 gate를 먼저 통과해야 한다.

S1/S2/S3 각각 먼저 RUN_ID를 A/B/C와 동일하게 설정하고, 서버의 실제 기존 JDK17 경로를 JAVA_HOME에 지정한다. java 버전과 JVM `/proc/PID/exe` 경로를 대조한다. 재기동 때 기존 JDK/heap/env를 유지한다.

```bash
export RUN_ID='failover-YYYYMMDDTHHMMSSZ' # A/B/C와 같은 값
export JAVA_HOME="$HOME/java/current" # 각 서버 실제 기존 JDK17 경로
export PATH="$JAVA_HOME/bin:$PATH"
"$JAVA_HOME/bin/java" -version
umask 077
```

기존 tar 방식이면 S1/S2/S3에서 각각 아래 준비를 완료한다. CFG는 실제 JVM이 실행하는 설정 **절대경로**, NODE_RUN은 이번 PID/시각 보관용 새 디렉터리다. 기존 heap/JAVA_HOME/KAFKA_OPTS/LOG_DIR/인증·기동 명령과 관리 주체를 비공개 기록으로 확보하고 승인된 기존 start 절차를 사용한다.

```bash
export CFG='/absolute/existing/config/node1.properties' # 실제 확인값
export NODE_RUN="$HOME/kafka-failover-runtime/$RUN_ID/node1-control"
test ! -e "$NODE_RUN" # 이미 있으면 새 RUN_ID로 보류·재준비
mkdir -p "$NODE_RUN"
python3 - "$CFG" <<'PY'
import sys
from pathlib import Path
cfg=Path(sys.argv[1]).resolve()
for f in Path('/proc').glob('[0-9]*/cmdline'):
    try:
        raw=f.read_bytes(); args=raw.split(b'\0')
        if b'kafka.Kafka' not in args or b'java' not in args[0]: continue
        arg=Path(args[args.index(b'kafka.Kafka')+1].decode())
        actual=(arg if arg.is_absolute() else (f.parent/'cwd').resolve()/arg).resolve()
        if actual==cfg: print(f.parent.name,raw.replace(b'\0',b' ').decode())
    except (OSError,IndexError): continue
PY
```

정확히 한 JVM이 나와야 한다. 출력된 숫자를 `PID`로 설정하고 대상 호스트·node.id·CFG·관리 방식과 대조한 뒤 기록한다.

```bash
PID='실제로확인한숫자PID'
tr '\0' ' ' < "/proc/$PID/cmdline"
ps -p "$PID" -o pid,lstart,args
cp "/proc/$PID/cmdline" "$NODE_RUN/expected.cmdline"
python3 - "$PID" "$NODE_RUN/expected.startticks" <<'PY'
import sys
from pathlib import Path
raw=Path('/proc/'+sys.argv[1]+'/stat').read_text()
Path(sys.argv[2]).write_text(raw.rsplit(')',1)[1].split()[19]+'\n')
PY
printf '%s\n' "$PID" > "$NODE_RUN/server.pid"
date -Is > "$NODE_RUN/identified.time"
```

pidfile가 없어도 이 탐색으로 준비할 수 있다. 0개/2개 이상이면 종료 보류. systemd는 실제 unit/ExecStart/MainPID를 함께 확인하고 중복 tar JVM을 기동하지 않는다. 새 격리 tar JVM도 기동 후 같은 cmdline/startticks 보관을 수행한다. 이 기록은 비공개로 보존한다.

## 1. 장애 전 gate — A 및 S1/S2/S3

각 서버에서:

```bash
hostname
date -Is
uptime
free -m
df -h
cat /proc/mdstat   # 해당 파일이 있는 Linux에서
dmesg | tail -100 # 권한이 없으면 관리자의 커널/OOM 점검 기록 확보
ps -eo pid,ppid,comm
java -version
"$HOME/kafka/current/bin/kafka-topics.sh" --version
```

**성공 기준:** Kafka3.9.1/JDK17, 시험용 JVM과 client 추가 실행에 충분한 RAM/디스크, OOM·scrub·재동기화·디스크 포화 없음. 단일 NAS 하네스는 MemAvailable 1.5GB 이상과 디스크 2GB 이상을 최소 차단선으로 사용했지만 운영 용량 권장치가 아니다. 운영 부하는 별도 산정한다. 중복 watchdog·자동 재기동·다른 관리자의 재할당/설정 변경이 시험 노드를 관리하고 있으면 시험 보류한다. 상시 관리 도구를 임의로 중지하지 말고 별도 비관리 시험 노드를 사용한다.

A(기존 시험 또는 상시 클러스터의 전체 gate):

```bash
"$BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" > "$RUN/before-api.out" 2> "$RUN/before-api.err"
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe > "$RUN/before-topics.out" 2> "$RUN/before-topics.err"
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic __consumer_offsets
"$BIN/kafka-metadata-quorum.sh" --bootstrap-server "$BOOTSTRAP" describe --status
"$BIN/kafka-metadata-quorum.sh" --bootstrap-server "$BOOTSTRAP" describe --replication
```

**성공 기준:** broker3개 모두 조회, 전체 topic에 유효 leader·replica 전부 ISR, offsets 모든 partition RF3/ISR3, quorum voter3개·leader 유효·살아있는 세 voter `Lag` 열이0. `LogEndOffset`을 Lag로 읽지 않는다. 3초 이상 간격으로 quorum status/replication을 다시 조회하고 ClusterId·LeaderId·LeaderEpoch가 동일하며 lag0인지 확인한다. metadata가 계속 기록되므로 HWM은 증가할 수 있다. 순간 lag1/선거는 최대180초까지 다시 조회하되 연속2회 정상 조건을 충족하기 전 장애를 주입하지 않는다. 시간 초과·지속 lag면 보류한다.

기존 상시 서비스도 함께 있는 호스트는 시작 전 서비스별 PID와 `/proc/PID/cmdline`을 비공개 파일로 기록하고 마지막에 동일성을 대조한다. 상시 service/설정/토픽을 이 시험용 값으로 바꾸지 않는다.

## 2. 새 격리 클러스터가 필요한 경우만 — S1/S2/S3

이미 승인된 시험 클러스터가 있고 단계1을 통과했다면 format을 건너뛴다. **기존 운영 디렉터리에 format하지 않는다.** 새 클러스터의 별도 설정·데이터·로그 경로, 기존과 다른 clusterUUID/nodeID, 미사용 listener 포트를 확보한다. 실제 NAS는 한 호스트의 loopback 포트6개와 nodeID101/102/103을 사용했다. 아래는 물리3대 예시이며 loopback을 다른 서버 간 주소로 사용하면 안 된다.

S1에서 새 UUID를 생성하고 세 서버에 동일하게 전달:

```bash
"$HOME/kafka/current/bin/kafka-storage.sh" random-uuid
```

각 서버에서 실제값으로 설정 파일을 작성한다. 아래 예시는 **node1**이며 node2는 node.id102/자신의 host, node3는103/자신의 host로 바꾼다. 세 서버에서 voters 목록과 clusterUUID는 같다. 경로는 실제 절대경로로 작성한다(properties에서 `~` 확장은 안 된다).

신규 격리 S1 예시(각 서버의 실제 절대경로와 node 이름으로 교체):

```bash
export NODE_RUN="$HOME/kafka-failover-runtime/$RUN_ID/node1"
test ! -e "$NODE_RUN" # 존재하면 이 단계 보류
mkdir -p "$NODE_RUN"
export CFG="$NODE_RUN/server.properties"
export CLUSTER_ID='앞서생성한동일한UUID'
```

설정 파일은 아래 값을 실제 주소·경로로 바꿔 `$CFG`에 저장한다. 새 test data 경로도 기존 경로와 중복되지 않아야 한다.

```properties
process.roles=broker,controller
node.id=101
controller.quorum.voters=101@broker1.example:9093,102@broker2.example:9093,103@broker3.example:9093
listeners=PLAINTEXT://broker1.example:9092,CONTROLLER://broker1.example:9093
advertised.listeners=PLAINTEXT://broker1.example:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs=/absolute/test-only/data/node1
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
offsets.topic.num.partitions=3
unclean.leader.election.enable=false
auto.create.topics.enable=false
```

offsets 3partition은 작은 새 격리랩 실측값이다. 운영의 기존 offsets partition 수와 RF3 설정을 유지한다. combined는 broker와 controller가 같은 JVM이므로 종료가 두 역할에 동시에 영향을 준다. controller를 분리한 운영 topology에서는 broker 종료가 controller 종료를 의미하지 않는다. 세 전용 controller의 active leader 시험은 별도 대상·승인창에서 1대만 종료하고 별도로 기록한다. 본 실측은 분리 topology를 검증하지 않았다.

각 서버에서 `CLUSTER_ID`, `CFG`, `NODE_RUN`을 실제 새 경로로 설정하고 확인한 뒤:

```bash
"$HOME/kafka/current/bin/kafka-storage.sh" format --cluster-id "$CLUSTER_ID" --config "$CFG"
```

**성공 기준:** 세 format 모두 exit0, meta.properties의 cluster.id 동일·node.id 서로 다름, 기존 데이터와 경로 중복 없음. 실패 시 시작 보류; 재format으로 덮어쓰기 금지.

세 서버 모두 format 완료 후 각 서버에서 **새 격리 tar 클러스터에만** 기동:

```bash
export JAVA_HOME="$HOME/java/current"
export PATH="$JAVA_HOME/bin:$PATH"
export KAFKA_HEAP_OPTS='-Xms256m -Xmx256m' # 새 소형 시험값, 운영 heap은 승인 용량 사용
export LOG_DIR="$NODE_RUN/logs"
mkdir -p "$LOG_DIR"
nohup "$HOME/kafka/current/bin/kafka-server-start.sh" "$CFG" > "$NODE_RUN/server.out" 2> "$NODE_RUN/server.err" < /dev/null &
echo $! > "$NODE_RUN/server.pid"
```

기본 start 스크립트의 최종 exec Java PID를 `/proc`로 확인하고 단계0 cmdline/startticks 기록을 수행한다. systemd 배포라면 새 중복 JVM을 기동하지 말고 실제 service와 ExecStart/설정 경로를 확인한 뒤 해당 service만 start한다(단계6). A에서 단계1의 broker-api-versions와 quorum status/replication 조회만 먼저 수행하여 broker3·세 voter lag0·연속2회 leader/epoch 동일을 확인한다. **새 클러스터의 offsets는 아직 없을 수 있으며 이 시점에는 요구하지 않는다.** 단계3~5에서 group을 만든 뒤 offsets 포함 전체 gate를 통과해야 최초 종료가 허용된다.

## 3. 데이터 topic 준비 — A

아래 node ID는 시험 대상 실제 ID로 교체한다. 승인된 새 테스트 topic에만 적용한다.

```bash
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --topic "$TOPIC" \
  --replica-assignment '101:102:103,102:103:101,103:101:102' \
  --config min.insync.replicas=2 --config unclean.leader.election.enable=false
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
```

**예상:** partition0 leader101, partition1 leader102, partition2 leader103, 각각 RF3/ISR3. 실제 출력이 다르면 선호 리더 복원 후 재조회한다. 전환될 leader가 대상 노드에 있는지 확인하지 못하면 다음 단계 보류.

A에서 데이터 토픽만 선호 리더 복원용 파일 작성(실제 `$TOPIC` 값이 들어간 JSON):

```bash
printf '{"partitions":[{"topic":"%s","partition":0},{"topic":"%s","partition":1},{"topic":"%s","partition":2}]}\n' \
  "$TOPIC" "$TOPIC" "$TOPIC" > "$RUN/preferred.json"
"$BIN/kafka-leader-election.sh" --bootstrap-server "$BOOTSTRAP" \
  --election-type PREFERRED --path-to-json-file "$RUN/preferred.json"
```

인증 환경의 leader-election 예외(다른 admin CLI와 옵션 이름이 다름):

```bash
"$BIN/kafka-leader-election.sh" --bootstrap-server "$BOOTSTRAP" \
  --admin.config "$ADMIN_CONFIG" --election-type PREFERRED \
  --path-to-json-file "$RUN/preferred.json"
```

## 4. consumer 시작 — C

```bash
touch "$RUN/consumer.out"
"$BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --group "$GROUP" --from-beginning \
  --consumer-property enable.auto.commit=true \
  --consumer-property auto.commit.interval.ms=1000 \
  --property print.partition=true --property print.offset=true --property print.timestamp=true \
  > "$RUN/consumer.out" 2> "$RUN/consumer.err" &
echo $! > "$RUN/consumer.pid"
python3 "$KIT/scripts/observe.py" "$RUN" > "$RUN/observer.out" 2> "$RUN/observer.err" &
echo $! > "$RUN/observer.pid"
```

**성공 기준:** `/proc/$(cat "$RUN/consumer.pid")/cmdline`에 ConsoleConsumer와 정확한 GROUP/TOPIC 확인. 출력은 `CreateTime:... Partition:0 Offset:0 RUN_ID|00000001|...` 형식. producer 전에는 consumer가 기다리는 것이 정상이다. observer는 stdout 파일 관측 시각을 보조 기록하며 Kafka 클라이언트를 대체하지 않는다. 실측 하네스는 PIPE를 직접 읽었고 수동 observer는 최대 약20ms polling 오차와 스케줄링 지연이 추가된다.

## 5. producer 시작·정상 기준 기록 — B, 이어서 A

B:

```bash
mkfifo "$RUN/producer-input.fifo"
python3 "$KIT/scripts/feed.py" "$RUN" "$RUN_ID" > "$RUN/producer-input.fifo" 2> "$RUN/feed.err" &
echo $! > "$RUN/feed.pid"
"$BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --sync \
  --producer-property acks=all --producer-property enable.idempotence=true \
  --producer-property retries=2147483647 --producer-property delivery.timeout.ms=120000 \
  --producer-property request.timeout.ms=30000 --producer-property linger.ms=0 \
  --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner \
  < "$RUN/producer-input.fifo" > "$RUN/producer.out" 2> "$RUN/producer.err" &
PRODUCER_PID=$!
echo "$PRODUCER_PID" > "$RUN/producer.pid"
wait "$PRODUCER_PID"
printf '%s\n' "$?" > "$RUN/producer.exit"
```

B는마지막 wait에서 계속 대기한다. A가 마지막에 stop-input을 생성하면 feeder가 EOF를 전달하고 producer가 정상 종료한다. pipeline 전체 Ctrl-C로 시험을 끝내지 않는다.

producer는 key 없는 데이터다. 기본 sticky partitioner의 소형·저속 송신은 충분한 분산을 보장하지 않으므로 **실측과 절차 모두 기본 제공 RoundRobinPartitioner를 명시**한다. 입력은 약10건/초이며 ID는 전역 증가하나 순서는 partition별로만 검증한다. `--sync`는 send future를 기다리지만 메시지별 ACK 성공 로그를 출력하지 않는다. 입력 수는 ACK 수가 아니다. 큰 retries와 delivery timeout은 재시도 무제한/무오류 보장이 아니다. source merge 근거는 [SOURCES](SOURCES.md); Kafka CLI 전용 `--message-send-max-retries` 등을 추가하여 property를 덮어쓰지 않는다.

A에서는 producer.err의 ProducerConfig 블록에서 `acks = -1`, `enable.idempotence = true`, `retries = 2147483647`, `request.timeout.ms = 30000`, `linger.ms = 0`, `delivery.timeout.ms = 120000`과 RoundRobinPartitioner가 실제 적용됐는지 확인한다. 다르면 최초 종료를 보류한다.

A:

```bash
tail -5 "$RUN/consumer.out"
tail -20 "$RUN/producer.err"
tail -20 "$RUN/consumer.err"
"$BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --group "$GROUP" --describe
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe
"$BIN/kafka-metadata-quorum.sh" --bootstrap-server "$BOOTSTRAP" describe --status
"$BIN/kafka-metadata-quorum.sh" --bootstrap-server "$BOOTSTRAP" describe --replication
```

**성공 기준:** consumer에 partition0/1/2 모두 실제 데이터, 각 partition CURRENT-OFFSET 증가·lag 회복, offsets topic 생성 후 모든 RF3/ISR3, 세 voter Lag0. 간격 있는 연속2회 controller leader/epoch 동일. producer·consumer PID/명령을 저장하고 시험 중 동일성 확인. ERROR/종료가 있으면 중지 단계로 진행하지 않는다. 단계0의 필수 client 전용 INFO 설정으로 ProducerConfig와 coordinator/재시도 로그를 기록한다. B/C에 KAFKA_LOG4J_OPTS가 적용되지 않았다면 최초 종료를 보류하고 클라이언트 시작 전 설정부터 다시 준비한다. 서버 전역 logging 설정은 변경하지 않는다.

## 6. node1만 정상 종료 — A 관측, S1 조작

A에서 종료 직전 topic describe, group describe, quorum status/replication 결과와 시각을 각각 파일에 저장한다. 예:

```bash
date -Is > "$RUN/node1-before.time"
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe > "$RUN/node1-before-topics.out"
"$BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --group "$GROUP" --describe > "$RUN/node1-before-group.out"
"$BIN/kafka-metadata-quorum.sh" --bootstrap-server "$BOOTSTRAP" describe --status > "$RUN/node1-before-status.out"
"$BIN/kafka-metadata-quorum.sh" --bootstrap-server "$BOOTSTRAP" describe --replication > "$RUN/node1-before-replication.out"
```

S1 tar 방식:

```bash
PID=$(cat "$NODE_RUN/server.pid")
test "$PID" -gt 1
tr '\0' ' ' < "/proc/$PID/cmdline"
readlink "/proc/$PID/exe"
date -Is
```

**조작 전 성공 기준:** 이 PID가 승인된 시험의 Java `kafka.Kafka`, config 인자를 `/proc/PID/cwd` 기준으로 resolve한 경로가 `$CFG`와 같고, node1이며 보호 대상 상시 서비스 PID와 다름. 단계0의 expected.cmdline과 현재 `/proc`를 `cmp`로 비교하고 startticks도 현재 `/proc/PID/stat`의 시작시각과 비교하여 PID 재사용을 배제한다. 일치하지 않으면 kill하지 않는다. 확인된 경우에만:

```bash
kill -TERM "$PID"
```

60초까지 해당 PID 종료와 로그를 확인한다. 종료되지 않으면 보류하고 정상 종료 원인을 조사한다. `kafka-server-stop.sh`, pkill, killall, 기존 강제 종료 도구는 사용하지 않는다. 임의 SIGKILL을 정상 종료 결과에 섞지 않는다.

systemd 방식은 먼저 실제 unit을 확인한다:

```bash
systemctl list-units --type=service
systemctl show "$SERVICE" -p MainPID -p ExecStart -p Restart
systemctl cat "$SERVICE"
```

확인한 `$SERVICE`가 이 시험 node1 설정을 실행하고 중복 관리가 없을 때만 권한 있는 계정에서 `systemctl stop "$SERVICE"` 실행. MainPID와 `/proc`를 함께 대조한다. 실제 service명 미확인 상태에서 `kafka.service` 같은 추정 이름을 사용하지 않는다.

A에서 종료 직후부터 조회를 반복한다(기존 명령의 저장파일명을 node1-down-start/node1-down-end로 구분):

```bash
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe
"$BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --group "$GROUP" --describe
"$BIN/kafka-metadata-quorum.sh" --bootstrap-server "$BOOTSTRAP" describe --status
"$BIN/kafka-metadata-quorum.sh" --bootstrap-server "$BOOTSTRAP" describe --replication
tail -6 "$RUN/consumer.out"
```

**장애 상태 성공 기준:** node1이 leader였던 partition의 leader가 다른 살아있는 broker로 바뀜, 모든 테스트 데이터/offsets partition ISR2이며 node1 제외, 살아있는 controller leader와 두 voter Lag0. voter membership은 여전히3이며 죽은 voter의 Lag는 증가할 수 있다. CURRENT-OFFSET·실제 소비 ID는 partition별로 증가하고 lag가 회복한다. producer/consumer PID 동일·프로세스 생존. 정상 종료 연결 경고·일시 재시도·지연은 기록한다.

이 상태에서 **최소45초**, 가능하면60초 계속 송수신한다. 종료상태 시작과 끝 모두 ISR2/두 voter 복제/각 partition commit·소비 증가를 확인한다. `uptime/free/df`, scrub/OOM도 다시 확인한다. 두 번째 노드를 종료하지 않는다. 기대 조건이 안 되면 다음 단계 보류하고 node1 복구부터 수행한다.

종료 끝점도 단일 조회로 판단하지 않는다. 최대180초 안에 3초 이상 간격의 연속2회 ISR2·살아있는 두 voter Lag0·동일한 Controller LeaderId/LeaderEpoch를 요구한다. 끝점 재관측으로 정지시간이 늘어나면 실제 down-start/down-end 시각과 확장된 정지시간을 기록한다. 순간 lag1을 무시하거나 기준을 낮추지 않는다.

## 7. node1 기동·완전 복구 — S1, A

S1에서는 **기존 CFG/기존 data**로 다시 기동한다. 신규 격리 tar는 단계2의 동일 env/heap/기동 명령을 사용한다. 기존 tar는 단계0에서 확보한 기존 start 절차·heap·env·로그 경로를 그대로 사용하며 신규 격리 nohup 예시로 대체하지 않는다. 새 PID를 단계0의 정확한 CFG 기반 탐색으로 식별하고 cmdline/startticks/시각을 새 기동 기록으로 갱신한다. format을 재실행하지 않는다. systemd면 확인한 `systemctl start "$SERVICE"`를 사용한다.

A에서는 broker-api-versions/topic/group/quorum CLI를 재조회한다.

**복구 성공 기준:** node1 broker ID가 다시 조회됨, 모든 데이터/offsets partition RF3/ISR3, voter3개 Lag0, 연속2회(3초 이상 간격) ClusterId·LeaderId·LeaderEpoch 동일, client PID 동일, 각 partition commit 증가·lag 회복. 180초 내 안 되면 보류하며 다음 노드 종료 금지. 정상 복구 시각을 기록한다.

## 8. node2, node3 — 반복하되 복구 gate마다 멈춰 확인

A에서 데이터 토픽 PREFERRED election(단계3)을 수행하고 데이터 leader101/102/103 분산과 quorum 안정성을 다시 확인한다. S2에서 단계6·7을 node2/PID/CFG/service명으로 바꿔 **node2 종료→최소45초 검증→기동→완전 복구**한다. 파일명도 node2로 구분한다.

node2 복구 성공 후에만 A에서 선호 리더 복원·안정 gate를 통과하고 S3에서 **node3 종료→최소45초 검증→기동→완전 복구**한다. node1/node2가 정상이라고 추정하지 말고 전체 ISR/quorum으로 확인한다.

각 stop 직전 Controller LeaderId가 종료 대상인지 표시한다. 대상이면 종료 후 실제 새로운 LeaderId/epoch를 기록한다. 순차3회에서 active controller 종료를 관측하지 못했다면 세 노드 모두 정상 복구 후 **그 시점의 active controller 1대만** 동일 절차로 추가 시험한다. 분리 controller topology에서는 별도 controller 대상 절차로 분리하고 이번 combined 결과와 섞지 않는다.

## 9. 정상 EOF·drain·전수 대조 — A/B/C

A:

```bash
touch "$RUN/stop-input"
```

B가 producer EOF/close 후 돌아오는지 확인하고 `cat "$RUN/producer.exit"`가0인지 확인한다. feeder stderr도 확인한다. C는 계속 소비한다. A에서 최소8초 이후 group describe를 반복하여 **3partition 모두 CURRENT-OFFSET=LOG-END-OFFSET, LAG0**를 확인하고 저장한다. producer exit1/timeout은 성공으로 처리하지 않는다.

C에서는 `/proc`의 정확한 consumer PID와 GROUP/TOPIC 확인 후 그 PID만 `kill -TERM`한다. 종료 후 `touch "$RUN/stop-observer"`로 observer가 남은 출력을 drain하고 정상 종료하도록 한다. 입력·출력·오류·CLI 상태·시각은 보존한다.

다음 직접 전수 대조 명령은 간단한 정상 판정용이다. 지연/구간별 상세 분석은 하네스 `analyze.py`가 생성하는 결과 형태를 참고한다(수동 실행에는 하네스 run.json/snapshots가 없으므로 그대로 실행할 수 없다).

```bash
python3 - "$RUN" "$RUN_ID" <<'PY'
import collections,json,re,sys
from pathlib import Path
d=Path(sys.argv[1]); run=sys.argv[2]
expected={x['value'] for x in map(json.loads,(d/'input.jsonl').read_text().splitlines())}
seen=[]; prev={}; counts=collections.Counter(); bad=0
for line in (d/'consumer.out').read_text().splitlines():
    m=re.search(r'Partition:(\d+)\s+Offset:(\d+)\s+('+re.escape(run)+r'\|(\d+)\|\d+)$',line)
    if not m: raise SystemExit('해석 불가 출력: 보류')
    part,offset,ident=int(m[1]),int(m[2]),int(m[4])
    if part in prev and (offset<=prev[part][0] or ident<=prev[part][1]): bad+=1
    prev[part]=(offset,ident); counts[part]+=1; seen.append(m[3])
missing=expected-set(seen); extra=set(seen)-expected; dup=len(seen)-len(set(seen))
print(dict(input=len(expected),consumed=len(seen),missing=len(missing),unexpected=len(extra),duplicates=dup,partition_counts=dict(counts),partition_order_errors=bad))
if missing or extra or dup or bad or set(counts)!={0,1,2}: raise SystemExit(1)
PY
```

**완료 기준:** producer sync EOF/exit0, 전수 값/ID 대조일치, 누락/예상외/중복0, partition별 순서 정상, 최종lag0, 모든 노드 RF3/ISR3·quorum 안정, 시험 중 client PID 동일. 이는 입력·소비와 sync 구현에 근거한 완료 추론이며 메시지별 ACK 시각·broker ACK 로그를 전수 수집한 결과가 아니다. 자동 commit은 소비 출력과 처리 완료의 원자적 보장을 제공하지 않으며 재시작 중복이 가능하다. 실패 시 원본을 보존하고 결과에 실패를 명시한다.

## 10. 정리·상시 보존 — 각 서버와 A

새 격리 클러스터만 정리하는 경우 모든 시험 끝난 뒤 node별 `/proc`·CFG·최초/재기동 PID를 다시 대조하고 **그 PID만 SIGTERM**하여 정상 종료한다. data/config/log는 보존한다. 기존 상시 cluster를 시험했다면 승인된 세 노드를 전부 정상 기동 상태로 유지한다. 삭제 명령은 이 절차에 없다.

시험 포트 폐쇄(새 격리 클러스터 종료 시), 상시 서비스 PID/cmdline 일치, 기존 clusterUUID/broker3/전체ISR/offsetsRF3/quorum 건강을 단계1과 같은 독립 CLI로 사후 확인한다. controller epoch는 자연스럽게 변할 수 있으므로 이전 값 강제 복원 대신 현재 연속 안정성을 확인한다. 공개 전에 raw 로그의 주소·개인 경로·인증정보를 제거한 작은 요약만 공유하고 원본은 비공개 보존한다.
