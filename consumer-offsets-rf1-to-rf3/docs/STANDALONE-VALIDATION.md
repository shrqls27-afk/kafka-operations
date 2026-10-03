# 회사용 offsets-rf3.sh 실제 전체 실행 검증

검증일: **2026-10-03, Asia/Seoul**. 대상 원본 커밋은 `a1c1cd7776354c3a71a9a3c6d5cea759f5b83a2a`이며 시작 전 로컬 HEAD와 GitHub main이 일치하고 작업 트리가 깨끗함을 확인했습니다. 회사 서버에는 접근하거나 적용하지 않았습니다.

**결론:** 회사용 단일 `.sh` 자체의 사전 조회 → 계획 생성 → dry-run → 검토 입력 → execute → 전체 RF3/ISR3 확인 → throttle 해제/원복 비교가 실제 격리 Kafka 클러스터에서 종료코드 **0**으로 완료됐습니다. 변경 직전 NO 및 완료 후 --resume도 아래 범위에서 실제 검증했습니다. 이전 helper 실험의 수치를 재사용하지 않았습니다.

## 검증한 파일과 환경

최종 `.sh` SHA256:

```text
3a4433458075a07cc0eeb13410430aeb39e40f32f388df1f3d674f20f640ce3c
```

NAS Linux 직접 설치, Apache Kafka 3.9.1, Temurin JDK/javac 17.0.20.1, Python 3.8.12, Bash를 사용했습니다. 사전 가용 메모리와 디스크의 정확한 수치는 `evidence/standalone-20261003-01/summary.json`의 preflight에 있습니다. 약 4GB 가용 메모리, 약 2.2TB 디스크 여유로 시작했고 실행 중 약 3GB 이상 가용 메모리를 확인했습니다. Docker와 패키지 다운로드는 사용하지 않았습니다.

실험 노드 ID 201/202/203, broker 포트 43001/43002/43003, controller 포트 43004/43005/43006을 모두 loopback에 바인딩했습니다. 기존 NAS 클러스터와 config/data/log 경로를 분리했습니다. 테스트 서버는 각각 힙 256MB, controlled.shutdown.enable=false입니다. 이 설정은 workload 완료 후 **테스트 전체 종료**의 quorum 대기를 피하기 위한 것이며 회사 운영 설정 변경 권고가 아닙니다.

별도 테스트 HOME 아래 `kafka/current`를 기존 Kafka 배포본에 연결했습니다. HOME은 **실행기 프로세스와 그 자식에만** 지정했습니다. 실제 NAS 홈이나 기존 링크를 변경하지 않았습니다. `standalone/`에는 복사한 offsets-rf3.sh **한 파일만** 있었고 주변 scripts/tests 없이 실행했습니다. 실행기가 테스트 HOME 안에 자기 내장 소스를 풀어 사용하는 경로까지 검증했습니다.

실제 내부 `__consumer_offsets`는 별도 실험 코드에서 offsets.topic.replication.factor=1로 설정한 새 클러스터에 stable consumer group을 접속해 **50개 전체 RF1/ISR1로 처음 생성**했습니다. 회사용 `.sh`에는 클러스터 생성/RF1 준비/토픽 삭제/RF 감소 동작을 추가하지 않았습니다.

## 실제 입력과 검토

입력 주소는 `127.0.0.1:43001,127.0.0.1:43002,127.0.0.1:43003`, 인증은 `-`, 대표 group은 `rf-lab-stable-group`, throttle은 `1048576` bytes/sec였습니다. 이 값은 격리 실험값이며 운영 권장값이 아닙니다.

검토 질문을 제거하거나 건너뛰지 않았습니다. 각 질문에서 독립 Admin 조회로 테스트 cluster ID·실제 broker ID·전체 50개 파티션·원본 replica 보존 계획을 대조한 뒤 실험 전용 stdin으로 응답했습니다. 사전 자원 확인과 실행 중 가용 메모리, 대표 group commit/lag 및 지속 workload 기록을 함께 관측했습니다. 실제 cluster ID와 입력·단계 기록은 공유 가능한 JSON에 있습니다.

| 시나리오 | 시작 시각(KST) | 실제 입력 | 종료코드 | 결과 |
|---|---|---|---:|---|
| 변경 직전 보류 | 21:51:15 | 주소 / - / group / YES / 1048576 / **NO** | 1 | 예상한 사용자 보류. 클러스터 변경 없음 |
| 전체 변경 | 21:52:23 | 주소 / - / group / **YES / 1048576 / YES / YES** | 0 | 전체 RF3/ISR3, throttle 원복, state=done |
| 정상 완료 후 재개 | 21:54:36 | `--resume`과 완료 작업 경로, 추가 입력 없음 | 0 | execute 재전송 없음, 독립 상태 불변 |

NO 전후에는 assignment뿐 아니라 leader/ISR/configs, cluster ID, 진행 중 재할당이 같았고 state는 prepared였습니다. 단일 실행기의 dry-run이 실제 실행을 하지 않았음을 확인했습니다. 전체 변경 뒤 실제 execute 접수 성공 로그는 **1개**이며 완료 후 resume 전후 execute 로그 집합은 같았습니다.

사전 broker API/quorum status·replication, unhealthy partition 조회, group 조회, plan, dry-run, execute, snapshot, verify의 내부 명령은 전체 실행의 fail-fast 구조와 완료를 근거로 모두 종료코드 0임을 확인했습니다. JSON의 helper_cli_exit_code_basis에 이 판정 방법을 명시했습니다. 독립 Admin/CLI 명령과 전체 .sh 실행의 종료코드는 실험 코드가 직접 기록했습니다. NO의 종료코드 1은 예기치 않은 실패가 아닙니다.

## 독립 검증 결과

- 실제 내부 offsets **50개 전체**의 원래 replica가 목표 replica 목록 첫 위치에 유지됐습니다.
- 모든 파티션 replica=3, ISR=3, replica/ISR 집합 일치, leader는 ISR 안에 있었고 target assignment 순서와 일치했습니다.
- 독립 Admin snapshot 및 kafka-topics describe 결과를 실행기 완료 메시지와 대조했습니다. kafka-reassign-partitions --list에서도 진행 중 작업이 없었습니다.
- throttle 해제 승인 직전 broker leader/follower rate=1048576과 topic throttled replica 목록을 독립 snapshot으로 보존했습니다. 해제 뒤 configs는 **변경 전 configs와 완전히 동일**했습니다.
- broker 설정 offsets.topic.replication.factor는 1인 채 기존 내부 토픽 RF가 3으로 바뀌었습니다. broker/producer/consumer를 재할당 구간에 계획적으로 중지하지 않았습니다.

## 이번 실행의 메시지와 지연

새 workload 관측 구간: **21:50:43~21:55:31 KST**, 약 **290초**. producer는 단일 파티션, acks=all/idempotence, 약 20건/초, consumer는 고정 group과 명시적 commitSync를 사용했습니다. NO 시나리오와 전체 실행·완료 후 resume 동안 모두 계속 실행했습니다.

| 항목 | 이번 실험의 관측 |
|---|---:|
| 전송 성공 / 고유 소비 | 4,769 / 4,769 |
| 전송 실패 / 소비 오류 / 커밋 오류 | 0 / 0 / 0 |
| 중복 / drain 후 ack된 ID 누락 / ack 없는 소비 | 0 / 0 / 0 |
| 재접속 전 committed offset | 4,769 |
| 재접속 첫 offset / 추가 marker 소비 / 이전 메시지 replay | 4,769 / 20 / 0 |
| 최종 committed / log end / lag | 4,789 / 4,789 / 0 |
| 전체 변경 phase 최대 전송 지연 / commit 지연 | 1,142ms / 68ms |
| 전체 변경 phase 최대 소비 간격 | **1,194ms** |
| 전체 변경 phase 2초 간격 유효 lag 표본 최대 | 0 |
| 초기 모니터링 Admin sample timeout | **2회** |

전송 시도·성공 ack·소비 ID 집합과 소비 이벤트 개수를 원본 이벤트에서 다시 계산해 일치함을 확인했습니다. 누락은 전송 중 lag가 아니라 **전송 종료 후 drain**으로 판정했습니다. consumer close/reconnect와 추가 20개 marker는 변경/관측이 끝난 뒤 별도 검증 단계에서 수행했습니다.

전체 변경 phase는 단일 파일의 사전 조회·계획/승인 대기·독립 검증·복제·throttle 해제까지 포함합니다. 이 시간이나 지연을 순수 Kafka 복제 시간으로 해석하지 않습니다. Admin sampling을 consumer 스레드에서도 수행하므로 처리 간격에는 모니터링 및 NAS CPU 경합이 포함됩니다. 2초 sampling lag 최대 0은 사이 구간의 lag가 항상 0이었다는 뜻이 아닙니다. 실제 실행기의 group CLI 조회에서는 순간 lag 1~2도 나타났습니다.

초기 sample timeout 2회는 그룹/offsets 생성 준비 시 Admin 조회 5초 timeout이며 앱 send/commit 실패와 구분했습니다. 초기 생성까지 포함한 전체 구간 최대 전송 지연은 3,724ms였습니다. sample 결과와 단계별 p95/max 및 rebalance callback 수는 JSON에 있습니다. 내부 client retry 전체 횟수와 실제 회사 비즈니스 트랜잭션은 측정하지 않았습니다.

## 종료, 소스 일치와 변경사항

workload는 종료코드 0, 세 테스트 broker는 소유 PID/명령 확인 후 SIGTERM으로 종료코드 143이었습니다. 모든 테스트 포트가 닫혔고 기존 Kafka 3노드/UI/Codex/Telegram의 PID와 전체 명령이 시작 전과 동일함을 확인했습니다. rm, 광범위 pkill, 기존 서비스 재시작, 기존 offsets RF 감소를 하지 않았습니다. 원본 로그와 데이터는 NAS `runtime/standalone-20261003-01/`에 보존했습니다.

회사용 .sh의 실제 실행에서 수정이 필요한 오류는 발견하지 않았습니다. `build_company_launcher.py`로 다시 생성해도 SHA256이 같고 내장된 company.py/reassign.py/java.sh/OffsetsLab.java가 현재 소스와 바이트 단위로 같았습니다. 회사용 파일은 a1c1cd7과 동일하며 이번 변경은 실제 검증 코드·작은 evidence·문서입니다. 기존 모의 테스트 15개도 통과했습니다.

## 미검증 범위와 회사 적용 전 전제

- **TLS/SASL/회사 ACL 인증은 미검증**입니다. 이번 연결은 인증 없는 loopback PLAINTEXT입니다.
- **재할당 진행 중 Ctrl+C/연결 단절 후 --resume은 실제 미검증**입니다. 이번 실제 resume은 정상 완료(state=done) 후 조회 재개만 검증했습니다. uncertain/submitted의 execute 재전송 방지는 기존 모의 테스트이며 이번 실제 결과와 구분합니다.
- 클라이언트 실행기 종료와 Kafka 서버 재할당 취소는 다릅니다. 이번 실험에서는 진행 중 중단이나 `--cancel`을 실행하지 않았습니다.
- 30분 timeout, 장기 복제 정체, quorum/broker 장애, 동시 다른 계정의 변경, 인증 파일 오류, 운영 대용량 offsets는 실제 미검증입니다. 소규모 복제가 빨리 끝나 장시간 반복 polling 경로도 실부하로 검증하지 않았습니다.
- 회사는 JDK17/javac·Python3.8+·Kafka3.9.1·`~/kafka/current`, 필요한 관리 권한, 3개 실제 broker와 정상 quorum, **전체 RF1/ISR1**, disk/network/compaction 여유, 기존 throttle와 동시 변경 부재를 사전 확인해야 합니다. 중요한 모든 그룹의 commit/lag/업무 지표와 승인된 관찰 기간은 담당자가 판단합니다.

계획적 애플리케이션 중지 없이 완료했으나 **지연 및 모니터링 오류가 있었습니다**. 이 짧은 NAS 한 대의 실험은 회사 물리 서버 3대의 오류·지연 없는 무중단 보장이 아닙니다. 시작 안내는 [ONE-SCRIPT](ONE-SCRIPT.md), 운영 판단은 [COMPANY-PROCEDURE](COMPANY-PROCEDURE.md)를 읽으세요.

재현(회사에서는 실행 금지):

```bash
export KAFKA_HOME=/실험용/Kafka3.9.1/설치경로
export JAVA_HOME=/실험용/JDK17/설치경로
python3 scripts/verify_standalone.py --directory "$PWD/runtime/새실험경로" --port-base 43000
python3 scripts/summarize_standalone.py "$PWD/runtime/새실험경로" "$PWD/evidence/새검증요약"
```

실험 코드는 테스트 cluster ID/포트/계획을 조회한 후에만 YES/NO를 stdin으로 전달하며, 별도 HOME 생성과 RF1 준비는 이 코드에서만 수행합니다. 새 경로를 사용해야 하며 기존 경로를 삭제하거나 재사용하지 않습니다.
