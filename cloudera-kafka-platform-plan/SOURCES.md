# 공식 출처·버전·라이선스 확인

모든 출처 확인일: **2026-10-04**. 문서 버전은 아래에 명시합니다. `latest`/HEAD·GitHub metadata는 확인일 당시의 상태이며 개발 착수 시 tag/commit·다운로드 artifact SHA256으로 고정해야 합니다. 검색 결과·공개문서 확인을 상용 제품 실행이나 지원 계약 확인으로 바꾸어 표현하지 않았습니다.

## Cloudera 공식 자료

| ID | 제품·버전·URL | 확인 범위와 한계 |
| --- | --- | --- |
| C01 | [Stream Processing 제품](https://www.cloudera.com/products/stream-processing.html?tab=1), 현재 마케팅 | messaging/streaming 솔루션 범위. 성능·무중단 문구는 vendor 주장, 직접 검증 아님 |
| C02 | [Data Hub Azure Streams Messaging](https://docs.cloudera.com/data-hub/cloud/create-cluster-azure/topics/dh-streams-messaging-clusters.html), Cloud 문서 | 템플릿별 구성 요소·지원 Runtime 목록. 모든 배포 기본 포함으로 일반화 금지 |
| C03 | [Runtime7.3.2 릴리스 노트](https://docs.cloudera.com/runtime/7.3.2/public-release-notes/rt-release-notes.pdf) | Kafka3.9.1 rebase·KRaft GA. Apache 배포본과 패치/운영 통합 동등성을 뜻하지 않음 |
| C04 | [Cloud7.3.2 업그레이드](https://docs.cloudera.com/cdp-public-cloud/cloud/cdp-upgrade-advisor/topics/mc-upgrading_to_runtime_7_3_2.html) |7.3.2.0·SP1 차이, JDK/기존 KRaft upgrade 제약. Cloud 내용을 Base에 일반화 금지 |
| C05 | [SMM7.3.2 overview](https://docs.cloudera.com/runtime/7.3.2/smm-overview/index.html), [사용 설명](https://docs.cloudera.com/runtime/7.3.2/smm-using/smm-using.pdf) | UI·운영 관측·탐색, 상용 실행 미검증 |
| C06 | [SMM7.3.2 REST](https://docs.cloudera.com/runtime/7.3.2/smm-rest-api-reference/index.html) | topic/partition/config·metrics·content·alert·Connect·CruiseControl endpoints. API 존재와 사용자별 허용은 별도 |
| C07 | [SMM7.3.2 interceptors](https://docs.cloudera.com/runtime/7.3.2/smm-using/topics/smm-enabling-interceptors.html) | 앱 계측 의존·metrics topic·latency. 일반 broker metrics와 구분 |
| C08 | [SMM Base7.3.2 구성](https://docs.cloudera.com/cdp-private-cloud-base/7.3.2/smm-configuring/smm-configuring.pdf) | CM·DB·metrics reporter/Prometheus 통합. Apache 기본 endpoint로 오인 금지 |
| C09 | [SRM7.3.2 overview](https://docs.cloudera.com/runtime/7.3.2/srm-overview/index.html) | driver/Connect·service·monitoring·REST·group offset 동기화 구조 |
| C10 | [SRM7.3.2 failover/failback 준비](https://docs.cloudera.com/runtime/7.3.2/srm-using/topics/srm-failover-failback-conf.html) | 복제 대상·group·checkpoint 준비. 앱과 RPO/RTO의 자동 보장은 별도 |
| C11 | [Registry7.3.2 호환 정책](https://docs.cloudera.com/runtime/7.3.2/schema-registry-overview/topics/csp-compatibility_policies.html) | 모드·validation 수준, API/UI 차이. format별 모든 조합은 미확정 |
| C12 | [CruiseControl7.3.2 overview](https://docs.cloudera.com/runtime/7.3.2/cctrl-overview/index.html), [7.3.1 load balancing 설명](https://docs.cloudera.com/runtime/7.3.1/cctrl-overview/index.html) |7.3.2 구성 범위, 상세 부하 모델/목표 설명은7.3.1 확인. 버전별 모든goal 지원 미확정 |
| C13 | [CM API](https://docs.cloudera.com/cloudera-manager/latest/configuring-clusters/topics/cm-api-config-files.html), latest | 설정·lifecycle·health/metrics. 실제 설치 CM의 API 버전은 미확정 |
| C14 | [Kafka7.3.2 구성](https://docs.cloudera.com/runtime/7.3.2/kafka-configuring/kafka-configuring.pdf) | CM rolling restart health checks. 정확한 OS/JDK는 [지원 매트릭스](https://supportmatrix.cloudera.com/) 별도 확인 |
| C15 | [Base7.3.2 KRaft Ranger](https://docs.cloudera.com/cdp-private-cloud-base/7.3.2/kafka-securing/topics/kafka-kraft-security-ranger-integration.html) | KafkaRangerAuthorizer·controller 권한·Ranger 감사용 ZooKeeper 의존. KRaft metadata가 ZooKeeper에 저장된다는 뜻 아님 |
| C16 | [Connect7.3.2 Ranger REST 권한](https://docs.cloudera.com/runtime/7.3.2/kafka-connect/topics/kafka-connect-securing-api-authorization-ranger.html) | Ranger+Kerberos/SPNEGO 전제, 직접REST/SMM 동일 정책 적용 |
| C17 | [CSM Operator1.6 설치](https://docs.cloudera.com/csm-operator/1.6/installation/topics/csm-op-install-overview.html) | Strimzi/Surveyor/Registry 독립 요소, Kubernetes·라이선스 artifact 경계 |
| C18 | [Operator1.6 릴리스](https://docs.cloudera.com/csm-operator/1.6/release-notes/csm-op-release-notes.pdf) | Kafka4.1.1·Strimzi0.49.1, Surveyor broker 기능, Registry 추가. Registry는 해당 배포에서 proprietary라고 명시 |
| C19 | [Surveyor1.6 설치·등록/인증](https://docs.cloudera.com/csm-operator/1.6/installation/topics/csm-op-install-surveyor-overview.html) | clusterConfigs·LDAP·KafkaACL 전제. Apache3.9.1 전체 지원 조합은 확인 못함 |

Ranger의 추가 의존성 때문에 현재 StandardAuthorizer 기반 직접 설치 환경에 그대로 복제하지 않습니다. ClouderaOSS upstream Registry와 Operator1.6의 상용 artifact가 같은 라이선스라고 가정하지 않습니다.

## Apache/API·오픈소스 공식 자료

| ID | URL·버전 | 확인 범위 |
| --- | --- | --- |
| A01 | [Admin.java3.9.1](https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/admin/Admin.java), [3.9 Javadoc](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/Admin.html) | 관리 API·비동기 결과, Javadoc 현재3.9.2 표시 주의 |
| A02 | [Kafka3.9 monitoring](https://kafka.apache.org/39/operations/monitoring/) | broker/client JMX, remote monitoring 보안. 앱 업무 latency와 구분 |
| A03 | [Kafka3.9 security](https://kafka.apache.org/39/security/) | SASL/TLS·ACL·API별 권한, idempotent/transactional client는 실제3.9.1 검증 필요 |
| A04 | [Kafka3.9 Connect](https://kafka.apache.org/39/kafka-connect/) | REST 관리·분산 worker. Cloudera plugin/Ranger 보안이 기본 Apache 기능은 아님 |
| A05 | [Kafka3.9 datacenters/MM2](https://kafka.apache.org/39/operations/datacenters/) | cluster 간 replication, 앱 전환·async 복제 고려 |
| A06 | [Kafka3.9.1 server 설정 소스](https://github.com/apache/kafka/blob/3.9.1/server/src/main/java/org/apache/kafka/server/config/ServerConfigs.java), [3.9 configuration](https://kafka.apache.org/39/configuration/) | 실행 시에는 설정별 Update Mode·3.9.1 소스/시험 확인, 정적 변경을 Admin으로 약속 금지 |
| O01 | [Provectus 저장소](https://github.com/provectus/kafka-ui), [GitHub metadata](https://api.github.com/repos/provectus/kafka-ui), [latest release](https://api.github.com/repos/provectus/kafka-ui/releases/latest) | archived=false, push2024-07-26, releasev0.7.2/2024-04-10. 현재 NAS 사용 버전과 유지보수 상태는 별개 |
| O02 | [Kafbat 저장소](https://github.com/kafbat/kafka-ui), [metadata](https://api.github.com/repos/kafbat/kafka-ui), [latest release](https://api.github.com/repos/kafbat/kafka-ui/releases/latest) | archived=false, push2026-09-28, releasev1.5.0/2026-04-20. 보안·3.9.1 인증을 뜻하지 않음 |
| O03 | [Kafbat RBAC](https://ui.docs.kafbat.io/configuration/rbac-role-based-access-control), [인증](https://ui.docs.kafbat.io/configuration/authentication) | UI 자원/action·OAuth/LDAP·Kafka SCRAM. 직접broker 통제와 구분 |
| O04 | [Spring Boot 요구 조건](https://docs.spring.io/spring-boot/system-requirements.html), current | Java17 이상. framework 지원 기간/최종 patch·Kafka client pin은 착수 시 재확인 |

## 라이선스 확인 — 법적 확답 아님

아래는 **공식 LICENSE 본문 또는 저장소 license endpoint를 실제 조회**한 결과입니다. Apache2.0은 NOTICE·저작권·수정 고지·특허 조건, MIT/PostgreSQL은 고지 유지, AGPL은 수정·네트워크 제공·배포의 소스 의무 등을 검토해야 합니다. 의존성·plugin·binary 배포물은 서로 다른 조건일 수 있고 조직 법무 검토를 대체하지 않습니다.

| 후보·용도 | 확인한 라이선스 | 공식 근거 |
| --- | --- | --- |
| Kafka·Connect·MM2 | Apache2.0 | [LICENSE](https://github.com/apache/kafka/blob/3.9.1/LICENSE) |
| Provectus UI | Apache2.0 | [LICENSE](https://github.com/provectus/kafka-ui/blob/master/LICENSE) |
| Kafbat UI | Apache2.0 | [LICENSE](https://github.com/kafbat/kafka-ui/blob/main/LICENSE) |
| Cruise Control | Apache2.0 | [LICENSE](https://github.com/linkedin/cruise-control/blob/main/LICENSE) |
| upstream Hortonworks Registry 후보 | Apache2.0 | [LICENSE.txt](https://github.com/hortonworks/registry/blob/master/LICENSE.txt) |
| Apicurio Registry 대안 | Apache2.0 | [LICENSE](https://github.com/Apicurio/apicurio-registry/blob/main/LICENSE) |
| Prometheus | Apache2.0 | [LICENSE](https://github.com/prometheus/prometheus/blob/main/LICENSE) |
| JMX Exporter | Apache2.0 | [LICENSE](https://github.com/prometheus/jmx_exporter/blob/main/LICENSE) |
| Alertmanager | Apache2.0 | [LICENSE](https://github.com/prometheus/alertmanager/blob/main/LICENSE) |
| Grafana OSS 선택 사항 | AGPL3.0 | [LICENSE](https://github.com/grafana/grafana/blob/main/LICENSE) |
| Keycloak | Apache2.0 | [LICENSE.txt](https://github.com/keycloak/keycloak/blob/main/LICENSE.txt) |
| Spring Boot | Apache2.0 | [LICENSE.txt](https://github.com/spring-projects/spring-boot/blob/main/LICENSE.txt) |
| React | MIT | [LICENSE](https://github.com/facebook/react/blob/main/LICENSE) |
| PostgreSQL | PostgreSQL License | [공식 license](https://www.postgresql.org/about/licence/) |

상용 Cloudera 기능/배포 artifact 전체를 Apache2.0이라고 해석하지 않습니다. connector마다 LICENSE·지원 버전·의존성과 데이터 시스템 계약을 따로 검토합니다. Vault/OpenBao 등 비밀 저장소는 아직 제품을 선정하지 않았으므로 라이선스 확인 완료 후보로 적지 않았습니다. 실제 채택 전에 공식 LICENSE·지원·오프라인 운영 조건을 확인해야 합니다.
