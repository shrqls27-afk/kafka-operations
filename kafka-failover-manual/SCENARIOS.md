# 시험 시나리오와 판정

승인된 시험창에서 테스트 전용 토픽과 식별한 대상 노드만 사용한다. **누적 종료 금지: 이전 노드의 RF3/ISR3·broker 재등록·quorum 안정·lag 회복 전 다음 노드를 종료하지 않는다. 두 노드 이상 동시 종료는 범위 밖이다.**

| 단계 | 장애 주입 | 독립 확인 | 진행 조건 |
|---|---|---|---|
| 정상 기준 | 없음 | broker3, 데이터 topic 3partition/RF3/ISR3, offsets RF3/ISR3, quorum3 voter lag0, leader/epoch 연속 동일 | 입력·소비·각 partition commit 증가, lag 회복, 데이터 leader 3개 노드 분산 |
| node1 | 해당 JVM SIGTERM 또는 확인한 service stop | 기존 node1 데이터 leader 교체, ISR2, 살아있는 controller 및 두 voter lag0 | 최소45초 정상 처리, 각 partition 소비·commit 증가, client PID 동일. 종료 상태 시작·끝 모두180초 이내 연속2회 ISR2·두 voter lag0·leader/epoch 동일 |
| node1 복구 | 기존 설정·기존 데이터로 start | broker API 재등록, 전체 RF3/ISR3, quorum3 lag0 | 간격 있는 연속2회 leader/epoch 동일, lag 회복 |
| node2 | node1과 동일 | node2 데이터 leader 교체·ISR2·quorum 과반수·처리 지속 | node2 완전 복구 후만 다음 단계 |
| node3 | node1과 동일 | node3 데이터 leader 교체·ISR2·quorum 과반수·처리 지속 | node3 완전 복구 |
| controller leader | 각 stop 직전 현재 leader 기록 | 대상이 active controller이면 실제 LeaderId/epoch 전환 관측 | 순차3회에서 관측되지 않으면 전부 복구 후 현재 leader 1대만 추가 시험 |
| 종료·대조 | 입력 생성 정상 종료, producer EOF, 소비 drain | producer exit0·전수 ID/값 비교·partition별 순서·최종 commit/lag | 누락/예상외/중복0, 전 partition 소비, lag0 |

회복 시 선호 replica leader를 테스트 토픽에 복원하면 다음 노드의 종료가 확실히 해당 partition leader 교체를 유발한다. 이는 시험용 부가 관리 작업이며 실제 운영에서 필요하지 않을 수 있다. 실제 NAS 하네스는 격리 클러스터 전체 테스트 파티션에 PREFERRED election을 실행했다. 사람용 절차는 데이터 토픽 3개 partition에만 수행한다.

관측 중 metadata 갱신, 연결 재시도, coordinator 재탐색, 일시 lag·지연이 발생할 수 있다. CLI consumer 1개이므로 다중 consumer 간 partition 이동을 검증하는 시험은 아니다. INFO/WARN의 coordinator·rebalance 기록과 독립 group describe를 남기되 관측하지 못한 내부 횟수를 0이라고 주장하지 않는다. auto commit은 외부 처리 완료와 원자적이지 않으며 재접속 시 중복 가능하다. 이번 측정 중 클라이언트를 재시작하지 않는다. 재접속·commit 복구 시험을 원하면 전체 failover 측정과 drain 종료 뒤 별도 RUN_ID로 분리한다.

SIGTERM 정상 종료는 controlled shutdown을 포함할 수 있다. 전원 단절/네트워크 분리/SIGKILL과 같은 비정상 장애 검증으로 표현하지 않는다. 비정상 종료 시험은 별도 승인·별도 기록으로 분리하며, 필요하더라도 새 격리 시험 PID에만 한정한다.

ISR1, quorum 과반수 소실, producer exit1/오류, 누락, OOM, scrub 또는 디스크 포화, consumer 종료, 지속 lag가 생기면 통과 기준을 낮추지 않는다. 다음 종료를 보류하고 대상 노드만 복구한다. 데이터·실패 로그는 보존한다. 기존 운영 토픽·서비스·상시 offsets RF는 변경하지 않는다.
