# 실제 NAS 실험 결과

실행일: **2026-10-03 (Asia/Seoul)**. Apache Kafka 3.9.1, Temurin JDK 17.0.20.1, Synology Linux 직접 설치. Docker 사용 없음. 회사 서버에는 접근/적용하지 않았습니다.

## 실행 환경과 범위

8GB NAS에서 기존 Kafka 3노드/UI/Codex/Telegram과 분리된 combined KRaft 3 JVM을 띄웠습니다. 사전 MemAvailable은 각각 4,144,060KB와 4,137,388KB였고 디스크 여유는 약 2.2TB였습니다. 테스트 노드당 힙 256MB, 측정 JVM 192MB이며 설치된 Kafka 바이너리를 읽기 전용으로 재사용했습니다. data/config/log는 각 `runtime/run-20261003-0N/`에 따로 생성했습니다.

1차 broker 포트 41001~41003, controller 41004~41006; 2차는 42001~42006입니다. 모두 127.0.0.1로 제한했습니다. 2차 시작 전 이전 포트 재사용 사전점검에서 Address already in use를 발견해 생성 전에 중단했고, 새 포트를 지정했습니다. 기존 서비스의 포트를 재사용하지 않았습니다.

노드 ID는 실험에서 101/102/103입니다. 계획은 이 값을 하드코딩해 만들지 않고 Admin API의 실제 broker/partition 목록에서 생성했습니다. 50개 `__consumer_offsets` 파티션 모두 RF=1·ISR=1로 처음 생성된 것을 baseline에서 확인했습니다. stable group의 coordinator/commit 요청으로 실제 내부 토픽이 생성됐으며 별도의 흉내 토픽을 사용하지 않았습니다. 모든 기존 replica를 target assignment의 첫 위치에 보존했습니다.

## 결과 비교

| 항목 | 1차 run-20261003-01 | 2차 run-20261003-02 |
|---|---:|---:|
| 연속 workload 관측 시간 | 약 161초 | 약 159초 |
| 전송 성공 / 고유 소비 | 2,503 / 2,503 | 2,501 / 2,501 |
| 전송 실패 / 소비 오류 / 커밋 오류 | 0 / 0 / 0 | 0 / 0 / 0 |
| 중복 / drain 뒤 ack된 ID 누락 | 0 / 0 | 0 / 0 |
| 커밋 진행(최종 재접속 전) | 2,503 | 2,501 |
| 동일 group 재접속 첫 offset | 2,503 | 2,501 |
| 재접속 후 추가 marker 성공 / 이전 메시지 replay | 20 / 0 | 20 / 0 |
| 최종 group committed / log end / lag | 2,523 / 2,523 / 0 | 2,521 / 2,521 / 0 |
| 전체 파티션 RF=3·ISR=3 | 50 / 50 | 50 / 50 |
| sampled offsets leader 변화 | 0 | 0 |
| 초기 모니터링 sample timeout | 2 | 1 |

raw 이벤트의 전송 시도 ID·성공 ack ID·소비 ID 집합과 길이를 다시 계산하여 요약 수치를 검증했습니다. 재접속은 재할당/after/drain 뒤 별도 검증 단계에서 consumer를 정상 close하고 재생성한 것입니다. 이 검증을 재할당 중 애플리케이션 중지로 섞지 않았습니다. phase 파일 전환 도중 진행 중인 요청은 시도와 성공 이벤트가 다른 phase에 들어갈 수 있습니다.

## 지연, commit, lag와 rebalance

단위 ms, p95는 이벤트를 정렬한 nearest rank입니다. 소비 간격은 연속 메시지의 애플리케이션 관측 간격으로 순수 broker 응답시간이 아닙니다. 특히 Admin snapshot을 같은 consumer 스레드에서 수행하므로 모니터링 자체와 NAS CPU 경합도 포함합니다. 전체 재할당 phase는 execute 전 재조회/컴파일과 verify/throttle 해제까지 포함하며 순수 서버 복제 소요시간으로 해석하지 않습니다.

| 단계 | 1차 전송 p95/max | 1차 commit p95/max | 1차 소비 간격 p95/max | 2차 전송 p95/max | 2차 commit p95/max | 2차 소비 간격 p95/max |
|---|---:|---:|---:|---:|---:|---:|
| baseline (초기 생성 포함) | 23 / 2,562 | 8 / 77 | 71 / 133 | 21 / 3,766 | 7 / 69 | 71 / 171 |
| reassignment | 23 / 1,662 | 19 / 140 | 77 / 1,708 | 19 / 1,041 | 13 / 79 | 72 / 1,099 |
| after | 8 / 29 | 8 / 136 | 58 / 138 | 8 / 21 | 6 / 28 | 58 / 67 |

측정된 group lag의 reassignment 최대값은 1차 2, 2차 1이었고 after 최대는 모두 0이었습니다. 초기 committed offset 미생성 시 lag -1은 정상 유효값과 분리했습니다. initial group 배정과 별도 재접속/close의 rebalance는 기록됐으며 재할당 phase에서는 assign/revoke callback이 없었습니다. 2초 간격 snapshot에서 leader 변화가 발견되지 않았지만 짧은 전환이나 내부 client retry가 없었다고 증명하지는 못합니다.

초기 sample timeout은 Admin 모니터링 future의 5초 timeout으로, offsets/그룹 생성 및 조회 준비 구간에 있었습니다. 앱의 send/commit 실패로 집계하지 않았고 별도 오류로 공개합니다. 내부 retry 전체 횟수나 회사 애플리케이션의 비즈니스 트랜잭션 시간은 측정하지 않았습니다.

## 재할당과 설정 확인

1MiB/s inter-broker throttle을 execute로 설정했습니다. 중간 snapshot에서 broker 101/102/103의 leader/follower replication rate와 offsets topic의 throttled replica 목록을 확인했습니다. `--verify --preserve-throttles`로 진행을 확인하고, RF=3·ISR=3 및 진행 중 재할당 없음 확인 후 `--verify`로 해제했습니다. after snapshot에서 dynamic rate 제거 및 topic throttle 목록의 default 복귀를 확인했습니다. 모든 파티션 before/after는 `evidence/run-*/partitions.json`에 있습니다.

재할당 중 broker 재시작 없이 완료했습니다. broker의 `offsets.topic.replication.factor` 설정은 after에도 **1**인 채 실제 topic RF가 **3**으로 바뀌었습니다. 이 관측은 재할당과 생성용 broker 설정을 구분해야 한다는 공식 문서/3.9.1 코드의 설명과 일치합니다. 설정 3으로 변경만 한 후 자동 RF 변경 여부를 시험하는 별도 restart 실험은 하지 않았습니다.

## 정상 종료와 보완

1차 검증 완료 후 순차 SIGTERM에서 마지막 combined 노드가 과반수 상실로 controlled shutdown을 대기했습니다. 소유 PID/명령을 확인한 상태에서 **테스트 노드 102/103만** 다시 기동하여 quorum을 복구했고 마지막 노드 종료를 확인했습니다. 복구 기동한 테스트 노드는 controlled.shutdown.enable=false로 최종 SIGTERM 종료했습니다. 원래 측정 결과는 보존했고 이 종료 단계의 leader 이동은 연속 workload 측정 결과와 구분했습니다.

재현 스크립트는 실험 전용 combined 클러스터에 controlled.shutdown.enable=false를 명시해 전체 종료 대기를 피하도록 보완했습니다. 이는 workload 종료 후 OS SIGTERM으로 flush/정리를 수행하며 강제 kill이 아닙니다. 회사 운영 설정의 권장 변경이 아닙니다. 이 보완을 실제 검증하기 위해 2차를 실행했고 모든 broker가 SIGTERM 종료(exit 143), workload는 정상 종료(exit 0)했습니다. 최종 /proc와 포트 확인에서 테스트 프로세스/리스너가 모두 사라졌습니다. 1차 지연 시점 shutdown.json의 null은 당시 상태 그대로 보존하고 최종 복구 결과는 별도 `cleanup-recovery.json`에 기록했습니다.

기존 Kafka/UI/Codex/Telegram의 처음 확인한 PID가 최종에도 동일하게 살아 있었고 기존 offsets 50개 파티션 RF=3·ISR=3을 **조회만** 해서 확인했습니다. 기존 topic RF 감소, 서비스 재시작, rm, 광범위 pkill은 수행하지 않았습니다. 데이터와 원본 로그는 NAS runtime에 보존했습니다.

## 한계와 미실행 항목

회사 환경 접근, 운영 적용, 물리 서버/네트워크 장애 및 각 broker 강제 장애 실험은 수행하지 않았습니다. RF 확대의 온라인 절차와 정상 종료 검증이 목적이고, 한 NAS 안의 combined 3개 노드는 물리 3대의 장애영역을 재현하지 못하므로 장애 내성/지연 보장 주장을 추가하지 않았습니다. 처리 부하는 단일 group/consumer와 약 20건/초, offsets 데이터 크기도 작습니다. 많은 그룹·대용량 compacted offsets·장시간 트래픽·회사 인증/ACL·client 설정·실제 disk/network 특성은 추가 검토 대상입니다.

이번 관측에서 **재할당 구간의 계획적 producer/consumer 중지는 없었으나 지연은 있었습니다**. 성공 API 호출 안의 재시도는 숨겨질 수 있습니다. 짧은 실험을 회사의 오류/지연 없는 무중단 보장으로 사용하지 마세요. 적용 판단과 보류 조건은 [RUNBOOK](RUNBOOK.md)을 따릅니다.
