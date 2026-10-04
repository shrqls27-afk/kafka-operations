# Kafka 3.9.1 KRaft 기준 재확인

감사일: 2026-10-04. [원본 기반 공개 근거](../evidence/kafka-3.9.1-kraft-audit.json).

**기존 RF 변경, 단일 실행기, 보안 A/B/C 실험은 모두 Apache Kafka3.9.1 KRaft로 실행한 것이 맞습니다.** 다른 버전 또는 ZooKeeper 모드에서 실행한 자료를 3.9.1 KRaft 결과로 사용한 경우는 발견하지 않았습니다. 따라서 버전/모드 불일치에 따른 클러스터 재실험은 필요하지 않았고, 보존된 원본을 재검사했습니다. 이 판정은 기능 검증의 성공/실패와 구분합니다.

## 확인한 직접 근거

- 원본12개 실험 디렉터리의 broker 기동 로그75개 모두 `Kafka version: 3.9.1`, `Kafka commitId: f745dfdcee2b9851`입니다. 각 로그의 startup `process.roles=[broker, controller]`도 확인했습니다. 실패·보류 실행도 이 숫자에 포함되며 전체 기능 성공을 뜻하지 않습니다.
- 각 실험의 노드 설정3개씩 총36개에서 `process.roles=broker,controller`, `node.id`, static `controller.quorum.voters`3개를 확인했습니다. `zookeeper.connect`는 없습니다. 로그의 기동 당시 역할과 보존된 설정을 대조했으며 현재 설정 파일만으로 과거 모드를 추정하지 않았습니다.
- 설치된 배포본의 `kafka-topics.sh --version`도3.9.1, 종료코드0입니다. 공식 [3.9.1 태그 gradle.properties](https://github.com/apache/kafka/blob/3.9.1/gradle.properties)의 버전과 일치합니다. 이는 NAS 배포본에 대한 확인이며 원격 운영 서버 버전을 조회한 것은 아닙니다.
- 주요 실험7개의 원본 증거52개를 기존 공개 SHA256과 재대조하여 모두 일치했습니다. 신규 감사 JSON에는 기동 로그·설정의 해시와 버전/모드에 필요한 허용 필드만 기록했습니다. 원본 주소·경로·인증·전체 로그는 포함하지 않습니다.
- 현재 offsets-rf3.sh의 SHA256은 `d25d3633b0d02390fc9a16ece7cf49596cf7ccfdab1ad565976bca9dce79de5c`입니다. 내장 payload가 실제3.9.1 KRaft 실험에서 실행한 원본과 동일하고 현재 Python/Java/Bash 소스와 바이트 단위로 일치합니다. 문구 변경 후 파일 전체 해시는 달라졌지만 실행 payload는 동일합니다.
- 현재 RF 단위 검증15개와 보안 안전장치 단위 검증13개가 통과했습니다. RF 모의 예외 경로에서 operation.lock의 ResourceWarning이 관측됐으며 이를 숨기지 않았습니다. 단위 검증은 실제 클러스터 전체 실행을 대신하지 않습니다.

## 자료별 실제 검증 범위

| 대상 | 버전·모드 | 유지하는 실제 결론 |
| --- | --- | --- |
| RF1→RF3 helper | 3.9.1 KRaft3노드 | 전체 offsets50개 RF3/ISR3·원본 replica 유지·지속 workload·drain·commit 재개 검증 |
| 단일 offsets-rf3.sh | 3.9.1 KRaft3노드 | 한 파일 전체 실행, NO 시 불변, 정상 완료 후 resume, 4769건 소비·drain 검증. TLS/SASL 및 진행 중 중단/resume은 미검증 |
| 보안 B 신규 구축 | 3.9.1 KRaft3노드 | SCRAM storage format·TLS/mTLS·StandardAuthorizer·최소권한, 최초24개 및 재시작 후24개 권한 검증 성공 |
| 보안 A 기존 전환 | 3.9.1 KRaft3노드 | 전체 전환 실패. 최종 상태 복구와24개 권한 검증 성공으로만 판정 |
| 공존 C | 3.9.1 KRaft3노드 | 기존 PLAINTEXT 앱/내부 경로 유지·새 SASL_SSL listener 추가. 참조 조건3610건/누락0/앱 API 오류0, 최대 소비 간격13.314초. ACL 전체 전환은 미검증 |

모든 실험은 Linux 직접 설치, 한 NAS의 combined broker/controller와 static quorum입니다. 물리3대의 독립 장애영역, 전용 controller 구성, dynamic quorum, 운영 네트워크·인증서·사용자별 앱 설정을 그대로 재현한 것은 아닙니다. C 참조 성공은 NAS용 quorum 시간 조정 조건이며 이를 운영 권장 설정으로 바꾸지 않습니다. 상세 실패/제약은 기존 결과 문서를 그대로 유지합니다.

producer/consumer 사용자의 client 옵션·버전은 서로 다를 수 있습니다. 이번 Java 측정 client는 Kafka3.9.1 라이브러리와 명시된 test profile을 사용했습니다. **broker 버전/모드가 같다는 사실이 모든 운영 앱 설정의 호환성·오류/지연0을 검증했다는 뜻은 아닙니다.** 운영 앱의 retry·timeout·commit·TLS/SASL 지원과 허용 지연을 앱별로 확인해야 합니다.

## 운영 환경에서 읽기 전용으로 확인

각3노드에서 실제 실행 중 PID/명령·설치 경로와 현재 기동 로그를 확인합니다. symlink가 바뀔 수 있으므로 디스크의 CLI 버전만 보고 실행 중 broker 버전을 판정하지 않습니다.

```sh
bash ~/kafka/current/bin/kafka-topics.sh --version
# 실행 중 broker 기동 로그의 Kafka version/commitId와 process.roles 확인
# 실제 server.properties의 node.id/controller.listener.names/controller.quorum.voters 확인
bash ~/kafka/current/bin/kafka-metadata-quorum.sh --bootstrap-server node1.example.invalid:9092,node2.example.invalid:9092,node3.example.invalid:9092 describe --status
```

인증 listener를 조회할 때는 보호된 `--command-config` 파일을 사용합니다. quorum 조회 성공은 KRaft 경로 확인에 도움을 주지만 해당 API만으로 원격 broker의 정확한 patch 버전을 판정하지는 않습니다. 절대 기존 데이터에 storage format을 실행하지 않습니다.

감사 재현은 NAS에 보존된 runtime이 필요합니다. 공개 저장소만 clone한 상태에는 원본 로그/비밀 설정이 없으므로 재감사를 실행할 수 없습니다. 기본 감사는 원본을 읽고 신규 JSON만 생성합니다. 선택적인 --run-unit-tests는 임시 fixture를 사용하며 운영 Kafka에는 접근하지 않습니다.

```sh
export JAVA_HOME=/path/to/jdk17
python3 scripts/audit-kraft-evidence.py --output evidence/new-audit.json --kafka-home /path/to/kafka-3.9.1 --run-unit-tests
```

새 output만 허용하며 기존 증거를 덮어쓰지 않습니다. 이번 감사에서는 broker/앱 시작·재시작, RF 감소, reassignment, format 및 기존 서비스 설정 변경을 하지 않았습니다.
