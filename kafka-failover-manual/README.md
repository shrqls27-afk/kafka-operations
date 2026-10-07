# Kafka 3.9.1 기본 CLI 순차 failover 검증

사람이 직접 실행하는 Linux/Red Hat KRaft 3노드 절차다. node1 종료·복구 후 node2, 복구 후 node3를 종료한다. 이전 노드의 전체 RF3/ISR3·quorum 안정·lag 회복 전 다음 노드를 종료하지 않는다.

- [단계별 수동 절차](MANUAL-RUNBOOK.md): 관리자/producer/consumer 터미널과 서버별 SSH 조작, 예상 출력·실패 보류·복구.
- [시나리오·판정](SCENARIOS.md): 데이터 leader와 active controller 전환, 처리·commit 지속, 예외 기대.
- [실측 결과](RESULTS.md): 최종 수치와 실패 시도·측정 한계.
- [공식 옵션·구현 근거](SOURCES.md).
- [선택 실측 하네스](scripts/live_lab.py), [전수 분석기](scripts/analyze.py), [안전 gate 회귀시험](scripts/test_gates.py).

2026-10-06 NAS 실측에서 node1→2→3 순차 종료·복구와 controller103→101 전환을 확인했다. **11532건 입력/소비 전수 일치**, 누락·중복·partition별 순서 오류0, 각 partition3844건, 최종lag0이다. producer/consumer PID를 유지했고 기존 상시 서비스도 보존했다. 회귀22개가 통과했다. client ERROR0과 재연결 경고, warmup 최대12387ms 및 측정 후 정리 구간 Raft ERROR1을 [결과](RESULTS.md)에 구분해 기록했다.

실측은 NAS 단일 호스트에서 Kafka3.9.1/JDK17의 **격리 combined JVM3개**로 수행했다. 물리3대·Red Hat·JDK21·분리 controller·전원 단절·네트워크 장애를 실측한 결과로 확장하지 않는다. 기존 상시 Kafka/UI/Codex/Telegram은 보호 대상이며 정지·재시작·설정 변경 대상이 아니다.

console producer `--sync`, `acks=all`, idempotence, retries=2147483647과 기본 제공 RoundRobinPartitioner를 사용한다. console consumer는 auto commit을 사용한다. 입력 수를 ACK 수로 표현하지 않으며 정상 EOF/exit0와 전수 소비 대조로 완료를 판단한다. 메시지별 ACK 로그·내부 재시도 전체 계측·오류/지연0 보장은 제공하지 않는다.

선택 하네스는 **NAS 보호 서비스6개와 상시 brokerID1/2/3을 검사하는 해당 실측용**이다. 임의 운영 클러스터에서 자동 장애 주입 도구로 실행하지 않는다. 운영자는 수동 절차를 사용한다. 하네스 실행 예(실주소는 환경변수로 입력):

```bash
python3 scripts/test_gates.py
python3 scripts/test_analysis.py
python3 scripts/live_lab.py \
  --kafka-home "$KAFKA_HOME" --java-home "$JAVA_HOME" \
  --existing-bootstrap "$EXISTING_BOOTSTRAP" --runtime "$NEW_PRIVATE_RUNTIME"
python3 scripts/analyze.py "$NEW_PRIVATE_RUNTIME" "$SANITIZED_EVIDENCE_OUTPUT"
```

runtime은 새 경로여야 하며 공개 저장소 밖에서 보존한다. raw 로그에는 내부 주소/경로가 포함될 수 있다. evidence에는 요약과 해시만 공개한다. 삭제/광역 kill/force push는 포함하지 않는다.
