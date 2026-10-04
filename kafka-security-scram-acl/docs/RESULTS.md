# NAS 격리 검증 결과

2026-10-04 실행. 실행 시각은 evidence JSON의 UTC이며 한국 시각은 UTC+9입니다. 원본 로그·데이터·비밀 파일은 Git에서 제외하고 NAS에 보존했습니다.

| 시나리오 | 실제 결과 |
| --- | --- |
| B 신규 보안 구축 | 성공. 최초 SCRAM format, TLS/mTLS, 최소권한 ACL, 24개 권한 검증 및 3노드 순차 재시작 후 같은 24개 검증 통과. 암호 회전 후 이전 암호 거부·새 암호 성공 확인. |
| A 기존 PLAINTEXT 전환 | 전체 경로 실패. 최종 PLAINTEXT 제거의 두 번째 broker가 등록 제한시간을 넘겨 안전 중단. 이후 최종 보안 상태 복구 및 24개 권한 검증 성공. 전체 전환 성공이나 운영 지속 보장이 아님. |

시작/종료 시각, 단계별 실제 프로세스 종료코드, rolling 시각, 전체 파티션 검증은 [A-summary](../evidence/A-summary.json), [B-summary](../evidence/B-summary.json)에 있습니다. 권한 사례별 exit_code는 API 성공0/예외1을 표준화한 값이며 OS 종료코드가 아닙니다. matrix 프로세스는 기대 결과 일치 시0, 불일치 시2입니다. 금지 관리 동작은 AuthorizationException으로 판정하며 단순 timeout을 권한 거부 성공으로 취급하지 않습니다.

두 최종 상태 모두 broker3개, 전체102파티션 RF3/ISR3( offsets50, transaction50, 앱2), ACL15개, 익명 ACL 없음, default deny, hostname verification HTTPS, 이전 PLAINTEXT client/controller 포트 폐쇄를 확인했습니다. 최종 quorum high watermark는 A4154/B1778, 관측 최대 metadata offset lag는 A1/B0입니다. B에서는 재시작 후 계정/ACL 유지와 dry-run이 암호를 바꾸지 않는 것도 확인했습니다.

## A 관측과 수정

최종 단계 실행은 UTC00:49:58 시작, UTC01:11:26 안전 중단됐습니다. 성공 응답9332건, 고유 소비8706건, 관측 중복0건입니다. 중단 시 성공 응답 중632건이 아직 소비 관측에 없었으나 drain이 중단되어 유실 여부는 미확정입니다. 후속 committed-offset drain 검증은 수행하지 않았습니다. send 오류7, commit 오류1 및 전환 종료 시 commit 오류1, monitor 오류15를 기록했습니다. 모두 TimeoutException이며 최대 소비 간격213.579초, 표본 최대 lag1498입니다. commit 성공5578회입니다. 자세한 근거는 [A-migration-failure](../evidence/A-migration-failure.json)에 있습니다.

등록 제한60초 내 완료하지 못한 broker의 CancellationException과 initial registration timeout을 원본 로그에서 확인했습니다. 등록 지연의 자원/선거 영향까지 단일 원인으로 확정하지 않았습니다. 보존된 데이터와 동일한 최종 보안 설정을 3노드에 맞추고 테스트 등록 제한180초로 재시작한 복구는 성공했습니다(UTC01:25:02 최종 확인). 이 테스트 제한값을 운영 권장값으로 제시하지 않습니다. 복구는 전체 rolling 경로의 재검증이 아닙니다.

고정 대기25초는 client 전환 완료를 보장하지 않았습니다. 실제 이벤트에서 최초 PLAINTEXT 제거 전 consumer 전환·commit이 완료되지 않았음을 확인하여, 신규 전송 성공과 소비·commit 성공을 모두 확인하는 migration_gate를 추가했습니다. 실제 보존 이벤트의 전환 전/후 판정과 5개 단위 검증은 통과했습니다. 수정된 gate를 포함한 A 전체 경로는 다시 실행하지 않았습니다. [gate 근거](../evidence/migration-gate-check.json)와 실행 코드 해시/최종 배포 해시를 구분해 보존했습니다.

앞선 시도에서 ephemeral port와 listener 충돌, controller 주소와 TLS SAN 불일치 가능성, quorum high watermark -1을 통과시키던 잘못된 health 기준을 발견했습니다. 포트 범위와 controller DNS를 수정하고 동일 leader/epoch 연속 표본, 유효 high watermark, RF/ISR 및 metadata lag 검사를 강화했습니다. 이전 A 측정은 pre-fix 근거이며 최종 성공 근거로 사용하지 않습니다. cold transaction 12초/consumer18초 제한 실패는 준비 상태 확인과 제한60초/45초로 보완했습니다. 제한값만 단독 원인이라고 단정하지 않습니다. 실패 증거도 evidence에 보존했습니다.

## 환경과 한계

Linux 직접 설치, Kafka3.9.1, JDK17, Python3.8, OpenSSL1.1.1, 한 호스트의 격리된 combined KRaft3노드/static quorum입니다. 별도 포트·데이터·테스트 HOME을 사용했습니다. client/inter-broker는 SASL_SSL/SCRAM-SHA-512(8192 iterations), controller는 mTLS입니다. 테스트256MB heap/작은 thread 수/controlled.shutdown.enable=false는 운영 권장 설정이 아닙니다. 기존 Kafka/UI/Codex/Telegram 서비스를 변경하거나 재시작하지 않았습니다.

A는 consumer 객체를 명시적으로 close/recreate했으며 rebalance와 소비 공백이 있습니다. 안전 중단 시 테스트 앱도 종료했으므로 전체 기간 앱 지속 성공으로 표현하지 않습니다. producer 내부 retry 횟수는 직접 측정하지 않았습니다. lag는 표본이고 소비 간격에 동기 Admin 조회/close 및 계측 오버헤드가 포함됩니다. timeout은 전송 유실 확정이 아닙니다. 짧은 단일 호스트 실험으로 운영 환경의 무중단을 보장하지 않습니다.

미검증: 물리 서버 간 네트워크/방화벽, 운영 CA/DNS/SAN 배포 및 인증서 회전, 실제 앱 배포, 장시간 부하/장애, SCRAM controller, dynamic quorum, read_committed/EOS offsets 트랜잭션, 최종9092 재사용, 수정 gate 포함 A 전체 전환 및 A 최종 drain. 실제 TLS/SASL·잘못된 암호·익명 접속·hostname 부정 검증과 지정 transactional ID 테스트는 수행했습니다. RF1 또는 ISR 부족 상태에서 rolling restart 가용성을 검증한 것이 아니며 RF1→3 선행 절차가 필요합니다.

모든 테스트 소유 PID/명령을 확인해 SIGTERM으로 종료했고 실제 종료·테스트 포트 폐쇄를 확인했습니다. 전체 종료 중 quorum 과반수 소실로 graceful-shutdown timeout 경고가 발생할 수 있으나 강제 종료나 기존 서비스 재시작을 사용하지 않았습니다.
