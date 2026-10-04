# 개발 우선순위·일정·판단 기준

**읽기 전용 PoC부터 시작하고, 운영 쓰기는 보안·감사·복구 조건을 통과한 기능만 개별 활성화합니다.** 모든 기능을 한 번에 제공하는 Cloudera 복제 제품은 목표가 아닙니다. 아래 일정은 개발 계획 추정이며 제품 구현 실적이나 견적이 아닙니다.

## 단계별 의존관계와 산출물

| 단계 | 범위·선행 조건 | 산출물 | 완료·보류 기준 |
| --- | --- | --- | --- |
| 0 요구/반입 준비 | 앱별 client 종류·옵션, topic/group/ACL·RF/minISR, 지원 OS/JDK·IdP·CA·반입 정책 확인 | 기능 목록, inventory 양식, threat model, ADR, 의존성/SBOM 목록 | secret 없이 대상·정책 합의. 데이터/보안 조건 미확정 시 write 보류 |
| 1 읽기 PoC | 단계0, 전용 read principal·접근 경로 | CID/health/partition/lag 화면, collector, stale/denied 표시, OIDC 접근 통제 | CLI 대조, 보통/과다 partition 부하, TLS·권한 거부 확인. 운영 상시 서비스 변경 없음 |
| 2 제한된 쓰기 기반 | 단계1 + RBAC·audit·job state·lock·승인 먼저 | plan/approve/execute/reconcile API, topic 생성·config allowlist | dry-run 불변·승인 hash·partial success·timeout·worker 재시작·원본 snapshot 확인 |
| 3 RF·계정/ACL MVP | 단계2, Kafka 인증/authorizer 사전 준비, 비밀 저장소·회전 경로 | RF 확대 adapter, topic/group 정책, credential 발급·rotation, deny matrix·운영 runbook | 전체 partition 사후조건·throttle 원복·관리자 잠금 방지·직접 접속 통제. PLAINTEXT 잔존 시 사용자별 완전 통제 완료 판정 불가 |
| 4 제한 운영·안정화 | 단계3 + 독립 리뷰·복구 연습 | 릴리스 패키지, systemd, 모니터링·백업·복구, 관리자 교육 | pilot 범위에서 장애/보안 시험 통과, 운영 인수·비상조치 합의 |
| 5 선택 확장 | MVP 안정화·실제 수요 | 메시지 탐색·Registry/Connect·quota·다중 cluster | integration별 직접 우회·비밀/데이터 노출·버전 계약 검증 |
| 6 재균형/DR/lifecycle | 별도 자원·SLO·복구 예산 | Cruise Control 제안·MM2 DR 계획·후기 승인 runbook | 실부하·site failure·RPO/RTO·failback 실증 전 자동 실행 비활성 |

topic 삭제, offset reset, partition 증가는 각각 손실·재처리·key 순서 영향이 있어 초기 write allowlist에 함께 넣지 않습니다. 별도 위험도·승인·복구 시험을 통과하면 추가합니다. 내부 topic·보안 전환·broker lifecycle는 기본 거부합니다. 계정/ACL 관리가 꼭 필요한 MVP이면 단계3까지를 최소 제품 범위로 봅니다.

## 인력과 일정 범위

전제: Kafka/Java·웹 개발 경험, 주당 전담 근무, 기존 Kafka 유지, 1개 cluster·제한 tenant, 인증서/IdP·보안 운영 참여 가능. 반입 승인·구매·외부 시스템 변경 대기, 앱 client 보안 전환 일정은 별도입니다. 주 단위는 달력 추정이며 각 항목을 단순 합산한 정밀 공수표가 아닙니다.

| 구간 | 개발자1명 | 개발자2~3명 | 범위를 늘리는 요인 |
| --- | --- | --- | --- |
| 준비 | 1~3주 | 1~3주 | 인트라넷 반입·CA/IdP·현재 ACL 확인 |
| 읽기 PoC | 준비 후3~5주 | 준비 후2~4주 | large groups·missing metrics·기존 UI 권한 |
| 제한 write·계정/ACL·안정화 MVP | 개발 착수부터 누적16~24주 | 누적10~16주 | 인수 기준·복구·보안 리뷰·앱별 설정 차이 |
| Registry/Connect 등 선택 확장 | MVP 후6~12주 이상 | MVP 후4~8주 이상 | plugin/serde·외부 DB·인증·권한 복잡성 |
| DR·업그레이드 자동화 | 별도 계획, 추가3~6개월 이상 가능 | 별도 계획, 병렬화보다 운영 검증 제약 큼 | site·복구 환경·정책·대량 데이터 |

1명은 API/worker→UI→시험 순서로 직렬 진행하고 운영 담당의 리뷰를 받습니다. 2~3명은 backend/worker, UI/collection, 보안·시험·배포를 분담하되 계획 상태 모델과 권한 정책은 하나로 합의합니다. 프로젝트 개발자 외 운영·보안 담당의 정기 참여가 없으면 운영 write 출시를 보류합니다.

전체 Cloudera 기능과 지원 수준 동등 구현은 다년간 여러 전문 영역의 상시 인력이 필요한 규모로 판단합니다. 이 판단은 구조적 범위에 따른 추정이며 Cloudera 개발 비용·견적 자료를 안다고 주장하지 않습니다. 플랫폼 자체 개발을 선택하는 이유는 좁은 운영 정책·승인 흐름에 대한 통제여야 합니다.

## 운영 유지 부담

초기 안정화 후 소규모 범위라도 개발/보안/운영 합계 **월0.2~0.5 FTE**를 계획상의 출발점으로 잡고 실제 incident·upgrade 빈도로 조정합니다. 24시간 지원·다중 cluster·Connect/DR까지 확장하면 이보다 크게 증가합니다. 인건비 단가·상용 구독 가격은 제시하지 않습니다.

유지 업무에는 CVE/SBOM 검토·반입, framework/client patch contract test, CA/secret 회전, IdP 변경, audit 보관, DB/metrics 용량·백업 복구, broker 버전별 API/ACL 재검증, incident 대응·권한 회수·runbook 개선이 포함됩니다. 무상 OSS는 운영 인력 비용이 없는 것이 아닙니다.

## 중간 의사결정과 중단 조건

- 읽기 PoC가 기존 UI보다 효용이 적으면 화면 개발을 중단하고 작업 관리만 평가합니다.
- 모든 관리자 작업을 플랫폼 경유로 제한할 수 없으면 승인 우회 위험을 표시하고 직접 접근을 정리하기 전 강제 통제 제품으로 출시하지 않습니다.
- 요구가 단순 topic/group 조회·message 탐색이면 Kafbat 평가·보안 설정만으로 충분할 수 있습니다.
- 지속 장애·감사 누락·비밀 노출·잠금 방지 미검증이면 write를 비활성화하고 read-only로 유지합니다.
- 기업 수준 지원·다수 connector 인증·광범위 upgrade 자동화가 필요하면 직접 개발 범위를 줄이고 지원 제품과 운영 부담을 비교합니다.

## 실제 개발 시작 전 필요한 결정

1. 첫 MVP는 읽기·토픽 변경까지만인지, 계정/ACL·RF 확대까지 필수인지?
2. 사용자/tenant 수·topic/partition/group 수, 수집 신선도, audit 보관기간과 승인 인원은 어느 정도인지?
3. 현재 앱별 Kafka client 언어/버전·보안·delivery/commit 옵션과 현재 인증 전환 계획은 무엇인지?
4. 기존 IdP·CA·비밀 저장소·alert 경로를 쓸 수 있는지, 오프라인 반입과 보안 검토 책임자는 누구인지?
5. 기존 UI 유지/격리 Kafbat 평가 중 어느 경로인지, 메시지 조회·다운로드를 허용할지?
6. 전담1명인지2~3명인지, pilot 환경·운영 sizing·장애 연습 자원이 확보되는지?
7. 어떤 작업을 절대 자동화하지 않을지, 직접 CLI 관리자와 플랫폼 lock/승인의 운영 규칙은 무엇인지?

현재 미확정 사항을 위 가정으로 처리해 계획을 완성했습니다. 이 결정이 내려지기 전 제품 구현·운영 적용을 시작하지 않습니다.
