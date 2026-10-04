# Kafka 3.9.1 SCRAM/TLS 및 최소 권한 ACL

Linux 직접 설치·3개 KRaft 노드를 대상으로 한 운영 절차와 격리 실험입니다. 기존 RF 변경 자료는 별도 폴더에 보존합니다.

- 기존9092 앱 유지 우선 경로: [KEEP-PLAINTEXT](docs/KEEP-PLAINTEXT.md)
- 기존 PLAINTEXT 전체 전환 참고: [MIGRATE-PLAINTEXT](docs/MIGRATE-PLAINTEXT.md)
- 신규 빈 클러스터: [NEW-CLUSTER](docs/NEW-CLUSTER.md)
- 실제 검증 범위와 한계: [RESULTS](docs/RESULTS.md)
- 공식 문서와 3.9.1 코드: [SOURCES](docs/SOURCES.md)

**검증 상태:** B 신규 구축은 성공했습니다. A 전체 전환은 중단됐고 최종 보안 상태 복구만 검증했습니다. 수정된 전환 gate를 포함한 전체 A 경로는 미검증입니다. C 공존 검증은 기존 PLAINTEXT 앱을 유지하며 보안 포트만 추가했습니다. 참조 시험에서 앱 API 오류0·누락0이었지만 처리 공백13.314초가 관측됐고 NAS용 quorum 조정 조건입니다. ACL 통제 완성이나 운영 영향0을 보장하지 않습니다. 상세 결과를 먼저 읽으세요.

필수 반입물: Apache Kafka 3.9.1 배포본, JDK17(java/javac), Python3.8 이상, bash, 운영 CA가 발급한 SAN 포함 서버/내부 통신 인증서 및 PKCS12 truststore/keystore. 실험 재현에는 OpenSSL/keytool도 필요합니다. 인터넷 설치나 Docker는 사용하지 않습니다.

`~/kafka/current`를 배포본에 연결하고 JAVA_HOME을 설정합니다. 이 진입점은 주변 scripts/ 및 tests/를 함께 반입해야 합니다. 전체 전환을 자동 판단하거나 client 배포를 자동 수행하지 않습니다.

```sh
export JAVA_HOME=/path/to/jdk17
bash kafka-security.sh --help
bash kafka-security.sh inspect --client /protected/admin.properties --output /protected/health-new.json
bash kafka-security.sh issue-users --client /protected/admin.properties --input /protected/users.properties
bash kafka-security.sh apply-acls --client /protected/admin.properties --input config-examples/acl.json
# 계획과 cluster ID를 확인한 후 해당 명령에 --apply --confirm ACTUAL_CLUSTER_ID 추가
python3 -m unittest discover -s tests -p 'test_*.py'
```

계정 파일은 `user=password` 형식의 600 권한 일반 파일입니다. 예제 암호를 실제로 사용하지 마세요. 비밀을 argv/셸 history에 넣지 말고 보호된 파일 편집 또는 비노출 입력으로 준비하세요. NAS ACL 환경에서는 umask만 신뢰하지 말고 실제 권한을 확인해야 합니다. 진입점의 초기 SCRAM 비밀번호 형식 제한은 scripts/security.py에 명시되어 있습니다.

격리 실험(운영 자동화 아님): KAFKA_HOME/JAVA_HOME을 지정하고 `python3 scripts/lab.py --scenario A --directory runtime/A-NEW --port-base 16000`; A 종료 후 B를 다른 새 경로에서 실행합니다. 데이터/원본 로그/키는 runtime에 남고 Git에서 제외됩니다. 시작 전 가용 RAM 2.5GB·디스크 5GB·전용 포트와 임시 포트 범위 충돌을 확인합니다. 실험의 loopback/짧은 인증서/작은 heap 설정을 운영 환경에 복사하지 마세요.

운영 검증은 전용 검증 topic/group 및 사전 승인된 관리 API 대상으로 수행합니다. `SecurityProbe matrix`는 실험용 고정 topic을 생성·변경·삭제하므로 운영 환경에 그대로 실행하지 마세요. 운영 앱 계정의 read/write/commit을 각 실제 client로 확인하고 다른 topic/group 및 관리 API가 명시적 AuthorizationException으로 거부되는지 기록합니다. timeout만으로 ACL 거부 성공이라고 판단하지 않습니다. 익명 SSL-only 보안 포트 테스트는 handshake/연결 실패를 관측하는 예외이며 권한 거부와 구분합니다.

| 역할 | 권한 범위 |
|---|---|
| security-root | 계정·ACL 관리 및 bootstrap용 super user; 비밀 엄격 보호 |
| topic-admin | topic 생성·삭제·파티션 증가·설정 변경, Cluster Describe |
| producer | 지정 topic Write/Describe; 필요할 때만 지정 transactional ID 권한 추가 |
| consumer | 지정 topic Read/Describe와 지정 group Read/Describe |
| 내부 통신 | 노드별 SCRAM broker principal 및 mTLS controller 인증서 principal |

앱별 user/password와 ACL을 분리하여 발급합니다. 예제 producer/consumer는 논리적 테스트 역할이며 실제 개인 또는 운영 계정명이 아닙니다.

변경 진입점의 건전성 gate는 보수적으로 broker3개·quorum high watermark 유효·모든 조회된 topic 파티션의 RF3/ISR3을 요구합니다. RF2 등 다른 복제 설계를 자동 판단하지 않습니다. 조건이 다르면 기본 조회/계획 결과를 검토하고 운영 설계의 RF/minISR 및 장애 허용 범위를 먼저 결정해야 합니다.
