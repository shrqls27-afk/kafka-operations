> 후속 검증 완료: 이 문서는 첫 수정 인계 시점의 기록입니다. 연속 quorum gate·UTF-8 수정이 추가된 최종 SHA와 NAS/WSL 실측은 [최종 결과](LIVE-VALIDATION-20261006.md)를 확인하세요.

# RF 확대 안전수정 및 시험 인계 (2026-10-06)

새 배포본은 **단위/모의시험 36개, Python 문법, Bash 문법, JDK17 + Kafka3.9.1 Java 컴파일**을 확인했습니다. 실제 Kafka 클러스터 접속/기동/RF 변경 시험은 이번 수정 담당자가 수행하지 않았습니다. 이전 NAS 성공 기록을 새 배포본의 실제 검증으로 사용하지 않습니다. NAS 시험은 금지 상태를 유지하며 WSL Linux 격리 시험은 root 담당자가 조건 확인 후 별도로 수행합니다.

배포 파일: `offsets-rf3.sh`, SHA256: `2149f04e471f2a0d6c6cc2db0960442898291fea551cfc2f72e0afbfb9992a91`.
`build_company_launcher.py`로 재생성했으며 내장 소스 4개와 원본의 바이트 일치를 확인했습니다. 실제 시험 담당자는 동일 배포 SHA와 전체 소스 manifest를 확인하세요.

- company.py: quorum status leader/high watermark/3voter/cluster ID 및 replication offset/lag를 값으로 검사합니다. subprocess 제한시간, done 재개 설정 재확인, lock close를 추가했습니다. 배포는 ~/kafka/current를 유지하며 RF 확대만 허용합니다.
- reassign.py: assert를 명시적 예외 검사로 바꿔 최적화 import에서도 검사합니다. execute 직전에 expected-throttle.json을 보존하고 verify --apply 직전에 완료 상태·예상 설정·실제 값/source를 비교합니다. replica 목록 순서는 집합으로 비교하며 중복/와일드카드를 거부합니다. uncertain 재개는 예상값 전체 일치, clearing 재개는 각 설정이 예상값 또는 원본값일 때만 진행합니다. 이전 버전의 예상 설정 증거가 없으면 자동 해제를 보류합니다.
- Kafka 3.9.1 [공식 ReassignPartitionsCommand.java](https://github.com/apache/kafka/blob/3.9.1/tools/src/main/java/org/apache/kafka/tools/reassign/ReassignPartitionsCommand.java)의 calculateLeaderThrottles/calculateFollowerThrottles/modifyInterBrokerThrottle을 대조했습니다. leader replica는 원본, follower replica는 추가 목적지이며 관련 broker 3개 모두 leader/follower rate 쌍을 설정합니다. 값 일치 검사는 소유권 보장이 아닙니다. 검사 후 해제까지 TOCTOU가 있으므로 다른 관리자 재할당/throttle 작업을 금지해야 합니다.
- java.sh: Java 소스 SHA256·Kafka 경로·JDK release를 기준으로 컴파일 결과를 캐시하고 flock으로 동시 컴파일을 막습니다. snapshot마다 javac를 반복하지 않습니다. Linux util-linux flock이 필요합니다.
- verify_standalone.py: 전체 format 성공 후 전체 launch, leader/high watermark≥0·broker 3개 등록·topic ISR readiness를 제한시간 내 확인합니다. nas 기본 환경은 실험 전 기존 6서비스 PID와 상시 클러스터 전체 partition/quorum health gate를 통과해야 합니다. local은 기존 Kafka/UI 프로세스 snapshot과 사후 보존을 검사하고 없으면 evidence에 명시합니다. 기존 클러스터를 시작하지 않습니다. 모든 evidence에 environment를 기록합니다.
- OffsetsLab.java: Admin 관측을 별도 단일 스레드로 분리해 느린 snapshot이 consumer poll을 직접 막지 않습니다. 첫 commit 이전 warmup은 관측하지 않으며 의도적 interrupt 종료는 sample_cancelled로 구분합니다. send/commit/consume 오류, producer retry 누계, ACK/소비 ID 전수 집합, 중복, phase별 send/commit/소비 지연 p95/max와 실제 in-flight 관측수·그 사이 client 진행을 기록합니다. sample_error 등 필수 측정 오류는 measurement_error_free=false로 보고합니다.
- 구간은 첫 commit 준비 뒤 baseline, 실제 execute YES 전송 직전 during, 독립 RF3/ISR3 확인 직후 after, 전송 종료 후 drain입니다. phase-boundaries.json에 UTC/monotonic 시각을 보존합니다. RF 변경 중 같은 workload 프로세스와 같은 producer/consumer 객체를 유지하며 재접속은 drain 이후에만 수행합니다.

## root의 WSL 격리 시험 준비

실제 시험은 이 인계 후 root가 별도 Linux 환경에서 진행합니다. Kafka3.9.1/JDK17 경로를 지정하고 매회 새로운 데이터 디렉터리와 별도 포트를 사용합니다. 예시 16KiB/s는 **시험용 관측값이며 운영 권장속도가 아닙니다**.

```bash
export KAFKA_HOME=/실제/kafka_2.13-3.9.1
export JAVA_HOME=/실제/jdk17
python3 scripts/verify_standalone.py --environment local --throttle 16384 --port-base 46000 --directory "$PWD/runtime/local-new-01"
python3 scripts/summarize_standalone.py "$PWD/runtime/local-new-01" "$PWD/evidence/local-new-01"
# 두 번째는 새 디렉터리와 별도 포트 사용
python3 scripts/verify_standalone.py --environment local --throttle 16384 --port-base 47000 --directory "$PWD/runtime/local-new-02"
python3 scripts/summarize_standalone.py "$PWD/runtime/local-new-02" "$PWD/evidence/local-new-02"
```

작은 offsets 토픽은 낮은 throttle로도 빨리 완료할 수 있습니다. in-flight 관측 0회 또는 send/consume/commit 진행 증거 부족이면 `inflight_client_progress_confirmed=false`이며 RF 변경 도중 연속 진행을 입증했다고 주장하지 않습니다. 새 WSL 결과를 NAS 검증으로 표현하지 않습니다. TLS/SASL·물리 3노드·실제 운영 client 옵션은 별도 검증 범위입니다.

수동 helper의 verify --apply에도 execute 때 승인한 `--throttle "$THROTTLE"`을 전달합니다. 생략 기본값은 1048576이며 실행 때 값과 다르면 해제를 거부합니다. clearing 부분 해제 후 재개는 단일 실행기의 저장 상태로 처리합니다.

기존 Kafka/UI/Codex/Telegram 중지·재시작·설정 변경, rm, git push, DSM 조작은 수행하지 않았습니다. 기존 Cloudera 계획과 미커밋 파일은 보존했습니다. 단위시험 로그는 runtime/safety-unit-20261006.log에 보존합니다.

후속 공개 점검: NAS 시험 시 상시 조회 주소는 `--existing-bootstrap` 또는 `RF3_EXISTING_BOOTSTRAP`으로 별도 지정합니다. 실제 주소는 저장소에 넣지 않으며 미지정 시 시작하지 않습니다. 배포 .sh에는 이 테스트 옵션이 필요하지 않습니다.
