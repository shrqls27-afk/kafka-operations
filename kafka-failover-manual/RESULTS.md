# NAS 실제 검증 결과

**attempt05의 순차 SIGTERM failover 검증과 전수 대조가 통과했다.** 실제 시험일은 2026-10-06, 완료 run 분석·현재 서비스 재확인은 2026-10-07이다. 앞선 실패01~04를 보존했고 완료 run을 다시 장애 시험하지 않았다.

## 환경·완료 판정

- Apache Kafka3.9.1, 실측 JDK17.0.20.1, NAS Linux 단일 호스트의 격리 combined broker/controller JVM3개. JVM heap128~256MiB, 새 clusterUUID, nodeID101/102/103, 기존과 다른 data/config/log 경로.
- listener는 loopback 미사용6포트이며 OS ephemeral 범위 밖에서 선택했다. 상시 서비스 설정/sysctl 변경 없음. 데이터 topic3partition/RF3/minISR2, offsets3partition/RF3, unclean leader election 비활성.
- 기본 console producer `--sync`·acks=all·idempotence·retries=2147483647·delivery.timeout120초·request.timeout30초·linger0·RoundRobinPartitioner. [실제 ProducerConfig](evidence/final/producer-applied-config.json)로 확인했다.
- 기본 console consumer1개, 같은 group·auto commit interval1초. producer PID21499와 consumer PID21497 및 cmdline hash가 측정 중 동일했다. 측정 종료 뒤만 입력 EOF/drain/consumer SIGTERM 수행.
- RUN_ID `failover-20261006T132619Z`. 입력·소비 ID1~11532 전수 값 일치, 고유소비11532, 누락/예상외/중복/해석오류/partition별 순서오류 모두0. 각 partition3844건. 전역 순서는 검증하거나 보장하지 않는다.
- producer 정상 EOF/exit0. 최종3partition 모두 commit=end=3844, lag0. `failure=null`, 전체 RF3/ISR3 및 quorum 안정 후 완료. 입력 수는 ACK 수가 아니며, sync 구현·EOF/exit0·전수 대조에 근거한 완료 추론이다. 메시지별 ACK 로그/ACK 지연은 수집하지 않았다.

근거: [요약](evidence/final/summary.json), [57개 독립 CLI 상태 관측](evidence/final/snapshots.json), [순차 이벤트](evidence/final/events.json), [broker 재등록·unfence 로그 시각](evidence/final/broker-recovery.json). 각 복구의 broker API CLI를 별도로 호출했다고 주장하지 않는다. 실제 복구 판정은 독립 topics/quorum/group CLI의 ISR3·commit/lag 및 broker 등록 로그를 사용했다.

## 한 대씩 종료·복구

아래 시각은 2026-10-06 KST다. 상세 UTC/소수점 시각은 events.json에 있다. stop→start는 SIGTERM 요청부터 재기동 요청까지이며, ISR2 관측 구간은 연속 정상 gate가 확인된 down-start→down-end로 더 좁다. 45초 대기 뒤 끝점 gate를 재조회하면서 실제 정지시간이 늘어났다.

| 대상 | SIGTERM 요청 | 재기동 요청 | 완전 복구 gate | stop→start / ISR2 관측 | JVM PID 이전→복구 |
|---|---|---|---|---|---|
| node1 (101) | 22:30:40.402 | 22:33:16.748 | 22:34:11.504 | 156.346 / 115.207초 | 18677 → 16300 |
| node2 (102) | 22:35:41.190 | 22:37:57.151 | 22:38:51.204 | 135.961 / 96.590초 | 18678 → 7468 |
| node3 (103) | 22:41:34.263 | 22:44:29.218 | 22:46:36.821 | 174.955 / 133.121초 | 18680 → 8847 |

이전 노드의 완전 복구 뒤에만 다음 노드를 종료했다. 종료 시작·끝 모두 전체6partition ISR2와 살아있는 두 voter Lag0, 동일한 controller leader/epoch를 간격 있는 연속2회 확인했다. membership은 계속3voter이며 종료된 voter의 lag 증가는 별도로 남았다. 복구는 전체6partition RF3/ISR3·세 voter Lag0·leader/epoch 연속 안정과 live group lag 회복을 확인했다. 소형10건/초 시험의 live lag gate는 3partition 합계30 미만이며, drain 후에는0을 요구했다.

각 종료 전 테스트 데이터의 선호 leader를 복원하여 세 노드에 분산시켰다.

| 종료 대상 | partition0 leader | partition1 leader | partition2 leader | controller leader |
|---|---|---|---|---|
| node1/101 | **101→102** | 102→102 | 103→103 | 103→103 |
| node2/102 | 101→101 | **102→103** | 103→103 | 103→103 |
| node3/103 | 101→101 | 102→102 | **103→101** | **103→101** |

node3 종료 직전 실제 active controller가103이었고 종료 후101로 바뀌었다. 추가 controller 종료 시험 없이 실제 전환을 관측했다. 각 down 구간의 독립 group CURRENT-OFFSET와 실제 소비 누계를 partition0/1/2 순서로 표시한다. commit 호출 횟수나 ACK 횟수가 아니다.

| 종료 대상 | commit 위치 시작→끝 (p0 / p1 / p2) | 실제 소비 누계 시작→끝 (p0 / p1 / p2) |
|---|---|---|
| node1 | 693→1075 / 694→1076 / 693→1075 | 696→1079 / 697→1079 / 696→1079 |
| node2 | 1686→2005 / 1687→2005 / 1686→2005 | 1688→2008 / 1689→2009 / 1688→2009 |
| node3 | 2864→3305 / 2864→3305 / 2864→3305 | 2866→3307 / 2867→3308 / 2867→3308 |

## 지연·재탐색·오류를 분리한 결과

입력 생성시각→consumer stdout observer 관측시각의 지연이다. 같은 호스트 시계를 사용했고 producer pipe 대기·client 초기화·재시도·소비 stdout 및 OS 스케줄링이 포함된다. 브로커 ACK 지연이나 외부 처리 완료 지연이 아니다. CLI 관측은 주기적 표본이며 순간 중단이 전혀 없었다고 보장하지 않는다.

| 구간 | 대조한 소비 ID 수 | p95 / 최대(ms) |
|---|---:|---:|
| warmup, baseline gate 이전 | 413 | 10378 / **12387** |
| 안정 baseline gate→node1 종료 전 | 1268 | 17 / 37 |
| node1 SIGTERM→완전 복구 | 2102 | 11 / 1395 |
| node2 SIGTERM→완전 복구 | 1891 | 11 / 1214 |
| node3 SIGTERM→완전 복구 | 3007 | 9 / 725 |
| node3 복구 후→입력 EOF | 338 | 43 / 1477 |
| 전체 입력·소비 | 11532 | 17 / 12387 |

전체 최대12387ms는 warmup에서 발생했다. 노드 종료→복구 구간의 최대값과 혼동하지 않는다. down-start→down-end 안에 실제 입력/소비한 건수는 node1 1148/1148, node2 962/962, node3 1324/1324다. 표의 구간별 ID는 **입력 시각으로 선택한 뒤 최종 소비와 대조**한 수다. 순서 보장 범위는 partition별뿐이다.

| client stderr 집계 | 실제 ERROR 레벨 | WARN 레벨 | Exception 언급 | 연결/재접속 언급 |
|---|---:|---:|---:|---:|
| producer | 0 | 19 | 8 | 10 |
| consumer | 0 | 3 | 9 | 58 |

Exception이라는 단어만으로 fatal이라고 판정하지 않았다. 로그 문구 집계상 coordinator 발견21회, unavailable/invalid/dead 언급20회, join 요청11회, 성공 join1회, assignment 추가1회다. 정확한 내부 재시도·rebalance 횟수 계측이 아니며 consumer1개로 다중 consumer partition 이동을 시험하지 않았다. coordinator 재탐색과 일시 재시도·지연을 관측했으므로 오류/지연0이나 무중단 SLA를 보장하지 않는다. 자동 commit은 외부 처리와 원자적이지 않고 재접속 시 중복 가능성이 있다. 측정 중 consumer를 재시작하지 않았으며 commit 재접속 복구는 별도 실측 범위 밖이다.

**정리 구간 broker ERROR1은 별도다.** 측정/drain이 끝난22:47:35.227부터 전체 격리랩에 SIGTERM을 전달해 정리했다. node2의22:47:43.660 로그에 Raft graceful shutdown 5000ms timeout/ERROR1이 있었다. 순차 장애 주입·client ERROR0와 혼동하지 않는다. 모든 소유 프로세스는 SIGTERM 뒤 종료됐고 시험 포트6개 폐쇄를 확인했다. 최종 run에는 SIGKILL을 사용하지 않았다. 전체 랩 정리에서 동시에 종료 신호를 전달한 것은 측정 이후 작업이며 두 노드 동시 장애 검증으로 계산하지 않는다.

근거: [구간·로그 집계](evidence/final/observations.json), [원본 파일 SHA256](evidence/final/raw-evidence-sha256.json). 원본 로그/data는 Git 밖 runtime에 보존했다.

## 상시 보존·검사·한계

상시 Kafka3/UI/Codex/Telegram의 PID와 cmdline이 시험 전후 동일했다. 기존 clusterUUID가 같고53partition 전체ISR·offsets50partition RF3/ISR3·quorum3 voter Lag0였다. 2026-10-07 재조회에서도 동일한6개 서비스와 기존 상태·시험 포트 폐쇄를 확인했다. [현재 재확인](evidence/final/current-service-recheck.json). 시험 중 scrub/raid 재동기화/OOM gate 차단이 없었고, 기록된 최소 MemAvailable은2770440KiB다. [자원 표본](evidence/final/resources.json).

안전 gate11개와 분석기11개, 총22개 회귀가 통과했다. Python 문법, 문서 Bash 명령 문법, 링크·공개파일 보안 검사, 실행본/배포본 hash 일치를 확인한다. 수동 [feed/observer helper 검증](evidence/final/manual-helper-validation.json)은 정상 EOF·증가ID 기록·파일 tail/drain을 실제 실행했다. 수동 helper를 이용한 Kafka 전체 장애 시험을 별도로 반복했다고 주장하지 않는다.

- live_lab.py SHA256: `f31f8152d51f98f7f7711a485d178d07787a916ce925aaf864276f82ea669113`
- gates.py SHA256: `e541b61f3ea75f065e516ce2c77e0ba0aca7e36fe4b4236e8fea621c22711c06`
- [설치본 옵션 근거](evidence/final/installed-cli-options.json), [공개 script 해시](evidence/final/published-scripts-sha256.json).

실측은 같은 호스트의 작은 combined 클러스터와 자동 commit consumer1개다. 물리3대·Red Hat 실기·JDK21·분리 controller·고부하·다수 group·TLS/SASL·트랜잭션·외부 처리·네트워크/전원 단절·SIGKILL 장애·두 노드 동시 장애는 검증하지 않았다. SIGTERM은 controlled shutdown을 포함하며 비정상 종료 시험으로 확장하지 않는다. 수동 운영 절차의 기본경로는 ~/kafka/current이고 실제 NAS 설치 경로는 runtime 인자로 주입했다. 수동 절차는 테스트 데이터 topic만 PREFERRED election, 하네스는 격리 topic 전체 election이다. 기본 CLI와 producer/consumer 설정은 같다.

## 앞선 시도

| 시도 | 실제 상태 | 판정·수정 |
|---|---|---|
| 01 | 상시 quorum replication의 실제 Lag는 모두0 | 측정 코드가 LogEndOffset을 Lag로 잘못 읽어 사전 차단. 격리 JVM/장애 시험 미시작 |
| 02 | 실제 follower Lag1 | 순간 사전 관측에서 보류. lag0 기준을 유지하고 bounded 연속 안정성 gate로 수정 |
| 03 | node1 종료 후 모든6partition ISR2, 살아있는 두 voter lag0인 관측과 송수신/commit 지속 확인 | 종료 끝점 단일 조회의 follower Lag1로 측정 gate 실패. node1 복구/순차3회 검증 미완료. 끝점도180초 내 연속2회 gate로 수정 |
| 04 | node3 broker listener bind 실패 | 클라이언트 시험 미시작. 시험 포트가 OS ephemeral 범위 내였음. source-port 충돌 가능성은 있으나 확정하지 않음. 범위 밖 포트와 broker 조기 종료 감지로 수정 |

03 정리에서 node2의 SIGTERM 대기60초가 초과됐다. 정확한 시험 설정 경로와 `/proc/cmdline`을 재확인한 소유 PID만 SIGKILL로 정리했다. 이는 시험 후 정리 예외이며 정상 failover/SIGKILL 장애 검증으로 계산하지 않는다. 상시 서비스는 보존했고 기존53partition 정상 상태를 확인했다. 04는 소유 시험 JVM 모두 SIGTERM 정상 종료와 포트 폐쇄·상시 보존을 확인했다.

근거: [실패 요약](evidence/failed-attempts/summary.json), 각 attempt의 `raw-sha256.json`. 원본 runtime/로그/data는 비공개로 보존한다.
