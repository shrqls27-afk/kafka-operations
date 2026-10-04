# 기능별 분석과 개발 판단

확인일: 2026-10-04. **문서 확인은 실제 상용 실행 검증이 아닙니다.** 아래 C/A/O 번호는 [출처 목록](SOURCES.md)에 제품·버전·URL·확인 범위를 연결합니다. “미확정”은 없다는 뜻이 아니라 해당 배포/권한/버전 조합을 확인하지 못했다는 뜻입니다.

## 제품·배포 형태

| 구성 | 확인한 역할 | 배포·포함 경계 | 근거 |
| --- | --- | --- | --- |
| Cloudera Streams Messaging | Kafka 중심 messaging 제품/솔루션 범위 | 모든 하위 서비스가 모든 계약·템플릿에 기본 포함된다고 단정할 수 없음 | C01, C02 |
| Runtime/Base 7.3.2 | Kafka 3.9.1 기반, CM으로 서비스 구성·운영 | Linux 직접 설치 Apache 배포본에 CM 기능이 자동 생기지 않음 | C03, C04 |
| Public Cloud Data Hub | Streams Messaging 템플릿으로 서비스 조합 배포 | 확인한 Azure 템플릿은 Kafka·SMM·SRM·Registry·Connect·Cruise Control 포함. 다른 cloud/템플릿은 별도 확인 | C02 |
| SMM 7.3.2 | Kafka 운영 현황·메시지 탐색·관리 UI/API | CM·metrics reporter·인증 및 다른 서비스 통합 설정에 의존 | C05~C08 |
| SRM 7.3.2 | 클러스터 간 복제와 상태·offset 동기화 운영 | MM2 기반 계층과 추가 운영 서비스, DR 전환은 애플리케이션 절차 포함 | C09, C10 |
| Schema Registry 7.3.2 | schema 버전·호환성·REST | serializer/wire format·인증·DB 운영을 별도 검토 | C11 |
| Cruise Control 7.3.2 | 부하 모델·목표 기반 재균형 | 데이터 수집·용량 설정·보안·실행 권한 전제 | C12 |
| Cloudera Manager | 구성·상태·서비스 수명주기·API | broker OS·parcel·service 제어 계층. Admin API 대체물 아님 | C13, C14 |
| Ranger | Kafka 및 Connect 정책 통합 | Cloudera KRaft의 KafkaRangerAuthorizer는 Apache StandardAuthorizer와 다름 | C15, C16 |
| CSM Operator 1.6 | Kubernetes용 Strimzi·Surveyor·Registry | Kafka 4.1.1 기반. 각 구성 요소 독립 설치 가능. 현재 직접 설치 환경에 적용 제외 | C17, C18 |
| Surveyor 1.6 구성 요소 | 다중 cluster 등록·broker 조회/설정·Kafka ACL 기반 권한 | SMM과 별도 제품. Helm·registry 접근 필요, Apache3.9.1 지원 조합은 미확정 | C18, C19 |

7.3.2.0의 일부 KRaft migration 제약과 Cloud 7.3.2 SP1 개선을 혼합하면 안 됩니다. 현재 Apache3.9.1 기존 클러스터를 Cloudera 서비스가 그대로 관리할 수 있다는 상용 호환성도 확인하지 못했습니다. [지원 매트릭스](https://supportmatrix.cloudera.com/)에서 정확한 배포형태·SP·OS·JDK·서비스 조합을 재확인해야 합니다.

## 기능·데이터·권한 매트릭스

아래 Cloudera 항목은 별도 표기 없으면 Runtime **7.3.2** 문서 기준입니다. CM latest API 문서는 정확한 설치 버전의 API 지원을 보장하지 않습니다.

| 기능 | 문서로 확인한 제공 범위 / 미확정 | 자체 구현 경로·데이터 출처 | 권한과 제약 | 근거 |
| --- | --- | --- | --- | --- |
| cluster/broker/partition 현황 | SMM metadata·metrics, CM 역할·상태 | Admin describeCluster/Topics/LogDirs·quorum 조회, JMX | Describe 계열을 자원/API별로 적용. controller/JMX 접근은 별도 | C05,C06,C13,A01,A02 |
| 토픽 생성·삭제·파티션 증가·설정 | SMM REST 관리 작업 | Admin createTopics/deleteTopics/createPartitions/incrementalAlterConfigs | Create/Delete/Alter/DescribeConfigs/AlterConfigs를 최소화. 파티션 감소 불가, 증가 시 key 분배 영향 | C06,A01,A03 |
| producer/consumer 모니터링 | SMM 계측 지표와 group 상태 | broker/client JMX, committed offset·end offset, 선택적 앱 계측 | broker 메트릭만으로 모든 producer 사용자·전송 성공·업무 latency를 복원할 수 없음 | C07,A02 |
| lag·throughput·latency | SMM topic/group/client 관측, interceptor latency | lag 계산·JMX rate·앱 timestamp/trace | end-to-end latency는 앱 계측·시간 동기·정의 필요. lag는 메시지 개수와 실제 처리 미완료가 항상 동일하지 않음 | C07,A02 |
| 토픽 탐색·메시지 조회 | SMM topic content API/Data Explorer | 제한된 KafkaConsumer·허용 serde | Topic Read 필요, 일반 monitoring principal과 분리. 개인정보 마스킹·읽기 양 제한·다운로드 통제 | C06,A03 |
| 계정·SASL/TLS·ACL | Runtime 인증/암호화·Ranger 통합 | Admin SCRAM/ACL·외부 CA·보안 runbook | TLS·listener·정적 설정은 Admin만으로 전부 처리 불가. KRaft StandardAuthorizer 경로를 별도 사용 | C15,A01,A03 |
| RBAC·감사 | CM/Ranger 정책, Connect 직접 REST에도 Ranger 적용 | OIDC→platform RBAC, 실행 principal binding, audit event | 플랫폼 계정과 서비스 계정은 별개. SMM 모든 API의 세밀한 권한·SSO/감사 보장 범위는 미확정 | C13,C15,C16 |
| Schema·호환성 | Registry backward/forward/full/none·validation level | 기존 Registry API 통합 | serializer와 schema ID/권한·format 테스트 필요. 엔진 재개발 제외 | C11 |
| Connect 관리 | SMM connector API, Runtime Ranger REST 제어 | Connect REST status/config/pause/resume | Apache Connect가 Cloudera Ranger 보안을 기본 제공한다고 가정 금지. gateway 우회 차단·connector 비밀 마스킹 | C06,C16,A04 |
| replication·DR·failover | SRM 복제·그룹 offset 동기화·failover 준비 | MM2·복제 lag·checkpoint·수동 DR 계획 | 비동기 RPO·중복·fencing·schema/ACL·앱 endpoint 전환 별도. 자동 무손실 failover 확정 불가 | C09,C10,A05 |
| reassignment·밸런싱 | SMM→Cruise Control 통합·목표 기반 최적화 | Admin reassignment·원본 assignment, 이후 Cruise Control | Alter 계열, ISR/용량/throttle·기존 작업 확인. self-healing 초기 비활성 | C06,C12,A01 |
| alert | SMM alert policy/notification API, CM 상태 | Prometheus rule·Alertmanager·플랫폼 job alert | 지표 부재와 정상0 구분, 오프라인 수신 경로·권한/라우팅 설정 | C06,C08,A02 |
| capacity·성능 | Cruise Control 부하 모델·broker disk/metrics | OS exporter·JMX·logdirs·기간별 증가량 | 정확한 예측·비교 성능은 상용/실부하 미검증. cardinality와 수집 부하 제한 | C12,C18,A02 |
| rolling·설정·upgrade | CM API/rolling health check | 읽기 precheck+승인 runbook, 후기 Ansible 평가 | 설정별 동적/정적·metadata.version 및 rollback 별도. RF1 상태 restart 금지 기준 | C13,C14,A06 |
| multi-cluster | SRM 복제 토폴로지, Surveyor cluster 등록 | cluster ID별 binding·queue·secret·metrics 격리 | bootstrap 주소만으로 대상 식별 금지, cluster별 capability 검증 | C09,C19 |
| multi-tenancy | Ranger 자원 정책 활용 가능 | topic/group prefix 정책·ACL·quota·UI scope | 물리 tenant 격리·공정 용량·완전 tenant 관리 제품 제공 범위 미확정 | C15,A03 |
| UI/API/SSO | SMM REST·CM API; Surveyor LDAP 설정; Kafbat OAuth/LDAP | 기존 IdP OIDC·UI gateway·자체 change API | LDAP 로그인과 OIDC SSO는 동일하지 않음. 배포별 SMM SSO 조합 미확정 | C06,C13,C19,O03 |

Cloudera custom metrics reporter, monitoring interceptor, Ranger plugin, CM lifecycle, SRM 서비스 및 인증 통합은 단순 Apache Admin 호출로 대체되는 영역이 아닙니다. 공개 API·독립 OSS 통합을 사용하고 상용 코드·이미지·문서를 복사해 제품을 만들지 않습니다.

## 개발 판단·검증 기준

난도는 현재 팀에 Kafka/Java/보안 운영 경험이 있다는 가정의 상대값입니다.

| 기능 | 판단·이유 | 난도 / 의존성·리스크 | 완료 판정 |
| --- | --- | --- | --- |
| 현황·lag | 직접 수집, 기존 UI 통합 | 중 / ACL·large partition scan | CLI 대조·수집 부하·stale/unknown 표시 |
| topic 관리 | 제한 작업 직접 개발 | 중 / 삭제·증가 되돌리기 어려움 | dry-run 불변·승인·사전조건 충돌·직접 상태 대조 |
| RF 확대 | 기존 검증 로직을 어댑터화 | 높음 / 복제 I/O·throttle·동시 재할당 | 모든 partition 기존 replica 유지·목표 RF/ISR·throttle 원복 |
| 계정/ACL·감사 | 직접 정책 계층, Kafka API 사용 | 높음 / 관리자 잠금·credential 유출 | allow/deny matrix·직접 broker 접근 차단·rotation·감사 |
| UI/SSO | 기존 UI·IdP 통합 | 중 / 우회 write·서비스 principal 과권한 | API 직접 호출·tenant 교차·session 테스트 |
| 지표/alert | OSS 통합 | 중 / cardinality·잘못된 정상 판정 | 지표 누락·alert 전달·quiet period·기록 재현 |
| 메시지 탐색 | UI 통합, 별도 권한으로 후순위 | 중 / 민감 payload | Read deny·마스킹·limit·export 금지 |
| Registry | OSS 통합, 수요 확인 후 | 중 / serde 호환·schema ID·DB | 실제 앱 serialization/compatibility matrix |
| Connect | REST 통합, 수요 확인 후 | 높음 / plugin 코드·secret·외부 DB 영향 | REST 직접 우회 거부·config 마스킹·task 복구 |
| 재균형 | Cruise Control 통합, 제안부터 | 높음 / reporter 버전·용량 모델 | hard goal·제안 검토·실부하·중단 후 reconcile |
| DR | MM2 통합, 후기 별도 프로젝트 | 매우 높음 / site·RPO/RTO·fencing | 장애·failback·중복/누락·offset/schema 정합 |
| broker lifecycle | 초기 수동, 후기 runbook 자동화 | 매우 높음 / SSH·root·OS·KRaft quorum | 최소 ISR·registration·quorum gate·업그레이드 복구 |
| Kafka/암호/connector 엔진 전체 | 구현 비권장 | 매우 높음 / 장기 보안·호환 유지 | 검증 비용이 운영 효용을 초과 |

## 기존 UI와 겹침·부족

Provectus **v0.7.2**의 cluster/topic/message/group/Connect/Schema/ACL·UI 권한 기능은 재사용 후보입니다. 원본은 미보관 상태(archived=false)이나 GitHub 조회상 마지막 push **2024-07-26**, 최신 release **2024-04-10 v0.7.2**였습니다. Kafbat는 마지막 push **2026-09-28**, 최신 release **2026-04-20 v1.5.0**으로 활동이 확인되었습니다. push 시각은 유지보수 품질·취약점 수정 보장이 아닙니다. 기존 UI를 바로 교체하지 않고 격리 평가합니다. [O01~O03]

Kafbat의 RBAC·OIDC/LDAP·topic/group/ACL 등은 중복 개발을 줄일 후보입니다. 운영 정책에 맞는 plan hash/승인/lock/복구 추적이 요구대로 제공되는지는 미확정이므로 자체 change 계층에서 책임집니다. UI write가 이 계층을 우회하면 승인 설계가 무력화되므로 초기 UI는 read-only principal로 연결합니다. 계정 발급·암호 전달·회전·작업 이력은 별도 설계합니다.
