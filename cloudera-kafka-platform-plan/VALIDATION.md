# 검증·운영·오프라인 반입 계획

이번 작업은 공개 문서 조사와 설계 검토입니다. **새 플랫폼이나 Cloudera 제품을 실행 검증하지 않았습니다.** 기존 NAS 실험은3.9.1 KRaft에서 개별 절차를 검증한 근거이며 신규 서비스 통합 시험을 대체하지 않습니다.

## 단계별 시험

| 시험 | 주요 사례 | 통과 기준 |
| --- | --- | --- |
| 단위 | ACL resource pattern·default deny·tenant 경계, canonical hash, TTL·승인 분리, secret redaction, 상태 전이 | 금지 자원·변경 계획·타 tenant를 모두 거부, key/JAAS 유출 없음 |
| contract |3.9.1 Admin/Connect/metric 예외·지원 여부, partial results·unknown offsets | 실제API와 일치,3.9.2/4.x 기능을3.9.1 제공으로 사용하지 않음 |
| 읽기 통합 | metadata/lag/ISR/config를 독립CLI와 대조, denied/stale/missing |0과 미확정 구분, group/type·isolation 차이 표시 |
| 쓰기 통합 | 토픽·config·RF·ACL·SCRAM 허용/금지,NO/dry-run, 승인 hash 변경 | dry-run 상태 불변·전체 사후조건·원본보존·권한별 deny matrix |
| 작업 장애 | API 응답 유실·timeout·worker crash·DB lease 만료·중복 요청·외부CLI 충돌 | 실제상태 reconcile 후 중복 변경 금지·unknown 보류·감사 연결 |
| Kafka 장애 | 테스트 전용 broker/controller 정지·ISR 부족·quorum 부재·throttle 충돌 | 단계별 gate·안전 보류, 정상 서비스와 격리 |
| 보안 | OIDC/session/CSRF·API 우회·tenant 교차·SSRF·주입·암호 회전·audit 변조 | API·worker에서 권한 재확인, gateway 우회 차단,secret 미노출 |
| 데이터 영향 | 지속 메시지ID·ACK·consume·commit·rebalance·lag·처리 공백 | 전송 구간과 drain 구분, ACK set과 drain소비 set 대조·중복·재개 확인 |
| 운영 복구 | DB/secret 백업 복원·API 재배포·audit sink 중단·certificate expiry | 조회/승인/작업 복구·모니터링·write 보류 정책 재현 |
| 성능 | 실제 topic/partition/group 범위·JMX/cardinality·큐 적체 | broker/client 영향과 UI 신선도 예산 충족,수집 limit·backpressure |

모든 테스트는 OS/JDK/client·Kafka artifact/hash·KRaft 구성·입력 정책·CID 익명별칭·원본 근거·종료코드·오류종류·실행시각을 기록합니다. 보안 TLS/SASL을 생략한 실험을 보안 통합 성공으로 표시하지 않습니다. transactional/idempotent/static group/cooperative rebalance 등은 앱별 실제 설정 사례를 선정하고 미수행 항목을 따로 남깁니다.

## 기존 결과가 제한하는 자동화

[보안 A/B/C 결과](../kafka-security-scram-acl/docs/RESULTS.md)와 [버전 감사](../docs/KAFKA-3.9.1-KRAFT-AUDIT.md)를 기준으로 다음을 구분합니다.

- B 신규 보안 구축과 계정/ACL의 재시작 유지 검증은 성공했습니다.
- A는 최종 PLAINTEXT 제거 rolling 중 broker 등록 timeout으로 전체 경로 실패했습니다. 복구 후 보안 상태·권한 테스트 성공은 전체 전환 성공이 아닙니다. drain 미완료라 남은 메시지를 유실/무손실로 판정하지 않습니다.
- C 공존 최종 검증은 ACK/소비3610건·앱API 오류0·중복/누락0이나 최대 처리 공백13.314초, monitor timeout4건을 기록했습니다. authorizer 도입 없이 보안 listener를 추가한 범위이고 모든 client 옵션을 보장하지 않습니다.

플랫폼은 RF1/ISR 부족·quorum 불안정 시 rolling을 보류하고, 앱별 client retry/delivery/commit/transaction 설정과 계정/ACL 준비·전환을 독립 확인해야 합니다. UI에서 “애플리케이션 계획 중지 없음”, “관측 오류/처리 지연”, “consumer 재시작”, “검증 미완료”를 각각 표시합니다. “무중단 버튼”으로 보안 전환을 제공하지 않습니다.

## 반입과 직접 설치

반입물: 검증된 JDK17 Linux 배포본, Kafka3.9.1 client artifact·소스/NOTICE, API/UI release, frontend bundle, JDBC·DB 패키지, systemd unit, JMX exporter·Prometheus/Alertmanager, 필요한 IdP/registry 추가물, 의존성 lockfile·SBOM·라이선스·SHA256/서명, 문서/복구 runbook. CA/키·secret은 배포 아카이브와 분리해 안전한 채널로 전달합니다. Python은 운영 보조 스크립트를 채택할 때만 지원 버전·오프라인 wheel을 함께 고정합니다.

빌드 환경에서 npm/Maven 의존성을 잠그고 사전 캐시·서명 검사·CVE 검토 후 완성 artifact를 반입합니다. 운영 서버에서 인터넷 다운로드·latest 해석을 요구하지 않습니다. 호스트명/SAN·CA trust·시간 동기·firewall·advertised address·IdP discovery/JWKS 접근을 확인합니다. TLS hostname verification을 끄지 않습니다.

전용 Linux 사용자·최소 filesystem 권한·systemd resource limit·로그 redaction·재시작 정책을 적용합니다. broker 홈/데이터를 공유하지 않고 기존 서비스 설정을 변경하지 않습니다. NAS는 PoC이며 운영3대 broker와 플랫폼/DB를 같은 자원으로 sizing한 것으로 간주하지 않습니다.

## sizing·운영 기준

PoC 시작 예산으로 API+worker1~2vCPU/2~4GiB, PostgreSQL1~2GiB, metrics 별도2~4GiB를 **가설**로 잡되 NAS 실제 여유가 없으면 서비스를 추가하지 않습니다. 운영 sizing은 partition/group/cardinality, scrape 주기, retention, queue 처리량, TLS·IdP·DB HA, 장애시 여유를 측정해 산정합니다. 위 숫자는 벤치마크나 운영 권장이 아닙니다.

DB에는 계획·감사·작업 상태를 보관하고 metrics 원본은 TSDB retention으로 관리합니다. audit 보존과 metric retention을 구분하고 증가량을 측정합니다. DB/secret key 백업은 분리하고 restore 훈련·복구시간·쓰기 차단 동안의 read-only 동작을 검증합니다. API/worker가 멈춰도 Kafka 앱 처리가 계속되는 구조를 유지합니다.

## 이번 문서 검토 범위

확인한 것: 공식문서·3.9.1 Admin source·공식LICENSE·UI GitHub metadata 조회, 기존 검증 문서 대조, 신규 문서 로컬 링크·금지 문구/민감 경로 패턴 점검.

미확인: 상용 SMM/Surveyor 실제 로그인/API 권한/SSO·감사 완전성·계약 포함/지원 조합, 모든 connector·schema format 호환성, 플랫폼 실제 부하·보안·장애 시험, 상용 성능·운영 무중단 보장. 이번에는 서비스를 시작하거나 운영 설정을 변경하지 않았습니다.

검토 결과: 문서6개·외부URL53개 HTTP 접근 확인, 상대 파일 링크 확인, 금지 문구·실제 개인 경로·private key/token 패턴 미발견. URL 접근 성공은 해당 기능의 실행 성공·지원 인증을 뜻하지 않습니다. 조사 단계에서는 기존 추적 파일 변경·stage·commit·push를 수행하지 않았습니다. 후속 업로드는 사용자 요청으로 이 문서 폴더만 대상으로 합니다.
