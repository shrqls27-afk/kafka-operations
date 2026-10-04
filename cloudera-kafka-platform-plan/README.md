# Cloudera Kafka 기능 분석과 자체 운영 플랫폼 개발 계획

확인일: **2026-10-04**. 대상: **Apache Kafka 3.9.1 KRaft 3노드 / Linux·Red Hat 직접 설치 / 인트라넷**. 이번 산출물은 조사·설계·계획이며 새 제품 구현, 상용 제품 실행, 운영 변경을 수행하지 않았습니다.

**권장안은 Kafka를 새로 구현하는 것이 아니라, 기존 Kafka와 UI·관측 도구 위에 안전한 변경 관리 플랫폼을 만드는 것입니다.** 처음에는 읽기 전용 현황과 consumer group lag를 수집하고, 이후 토픽·RF·계정/ACL 변경에 계획·승인·진행 추적·감사를 붙입니다. Schema Registry, Connect, 재균형, DR 엔진까지 직접 만드는 것은 초기 범위에서 제외합니다.

| 판단 | 권장 범위 |
| --- | --- |
| 직접 개발 | 운영 정책, cluster ID 확인, 작업 계획/hash, 승인, lock, 멱등성·상태 재조회, 계정/ACL 정책 연결, 감사 |
| 기존 도구 통합 | Kafka Admin API, 기존 UI 또는 Kafbat 평가, JMX Exporter·Prometheus·Alertmanager, 기존 OIDC/LDAP 연동 |
| 후순위 | Schema Registry·Connect 관리, Cruise Control 제안 검토, MirrorMaker2 기반 DR 절차, tenant quota |
| 구현 비권장 | Kafka 저장·복제 엔진, 자체 암호화/SSO 엔진, 범용 connector·schema 엔진, 검증 없는 자동 보안 전환·자동 DR failover |

Cloudera 전체와 동등한 제품을 소수 인원으로 만드는 계획은 권하지 않습니다. UI 기능보다 버전별 통합, 권한·감사, 장애 복구, 업그레이드 검증, 지원 체계를 장기간 유지하는 부담이 큽니다. 선택한 운영 문제에 한정한 플랫폼은 현실적입니다. 상용 지원의 가치를 대체했다고 주장하지 않습니다.

## 읽는 순서

1. [기능·제품·배포 형태 비교](FEATURE-MATRIX.md)
2. [아키텍처·데이터 모델·API·변경 안전장치](ARCHITECTURE.md)
3. [개발 단계·인력·일정·완료 조건](ROADMAP.md)
4. [시험·운영·반입 계획](VALIDATION.md)
5. [공식 출처와 라이선스 확인 목록](SOURCES.md)

## 제품과 버전의 경계

- Runtime **7.3.2**는 Kafka **3.9.1** 기반으로 재구성되었고 KRaft GA가 문서화되어 있습니다. Apache 배포본과 Cloudera 패치·설정·인증·지원 조합이 같다는 뜻은 아닙니다. SP1 이후 변경은 기본 7.3.2.0과 구분합니다. [릴리스 노트](https://docs.cloudera.com/runtime/7.3.2/public-release-notes/rt-release-notes.pdf)
- CSM Operator **1.6**은 Kubernetes·Strimzi 기반이며 Kafka **4.1.1**로 재기반화되었습니다. Surveyor와 Schema Registry를 별도 구성 요소로 설치할 수 있습니다. 직접 설치 3.9.1 환경의 권장 배포 방식이나 호환 인증으로 해석하지 않습니다. [1.6 릴리스](https://docs.cloudera.com/csm-operator/1.6/release-notes/csm-op-release-notes.pdf)
- Public Cloud Data Hub의 특정 Streams Messaging 템플릿에 들어가는 구성 요소와 Base에서 따로 구성하는 서비스는 다릅니다. 계약상 포함 여부·지원 OS/JDK·정확한 서비스 팩은 실제 선정 시 확인해야 합니다. 이번에는 유료 계정·라이선스·상용 클러스터 없이 공개 문서만 조사했습니다.

## 기존 검증에서 가져오는 전제

[버전 감사](../docs/KAFKA-3.9.1-KRAFT-AUDIT.md), [RF 확대 결과](../consumer-offsets-rf1-to-rf3/docs/RESULTS.md), [보안 결과](../kafka-security-scram-acl/docs/RESULTS.md)를 참고합니다. 기존 실험을 새 플랫폼의 통합 시험으로 재사용하지 않습니다.

- RF 확대는 3.9.1 KRaft에서 기존 replica 보존·전체 RF3/ISR3를 검증했습니다. 일반적인 모든 부하에서 오류·지연이 없다는 보장은 아닙니다.
- 신규 보안 구축 B는 계정/ACL·재시작 유지 검증을 통과했습니다. 기존 운영 중 보안 전환 A는 **전체 경로 실패**였고 최종 보안 상태 복구 성공과 구분해야 합니다.
- 보안 listener 추가 공존 C는 제한된 client 설정으로 지속 처리를 검증했으나 ACL 통제 완성이나 모든 사용자 옵션의 무영향을 검증하지 않았습니다.

따라서 MVP는 broker 재시작·listener/authorizer 변경을 자동화하지 않습니다. 기존 PLAINTEXT 직접 접속이 남아 있으면 플랫폼 RBAC만으로 직접 Kafka 관리 권한을 차단할 수 없습니다.

## 필요한 규모

개발자 1명 전담 기준 읽기 PoC 3~5주, 제한된 운영 쓰기·권한·감사 MVP까지 누적 **16~24주**를 계획합니다. 2~3명 전담이면 **10~16주**를 초기 범위로 잡되 운영 담당·보안 담당의 참여가 별도로 필요합니다. 수치는 설계 추정이며 요구 확정, CA/IdP·오프라인 반입 준비와 승인 대기 시간을 별도로 더합니다. 상세 전제와 유지 부담은 [ROADMAP](ROADMAP.md)에 있습니다.
