# 기존 9092 앱을 유지하면서 보안 포트 준비

이 문서는 기존 PLAINTEXT 앱 보호를 우선하는 첫 적용 경로입니다. 기존 A의 전체 보안 전환이 성공한 것으로 취급하지 않습니다. **오류·처리 지연이 단 한 번도 허용되지 않는다면 기존 클러스터 설정 변경/재시작을 실행하지 마세요.** 단일 broker 재시작도 leader/coordinator 재연결과 retry를 유발할 수 있습니다. Kafka만으로 앱 영향 0을 보장하는 방법은 없습니다. 별도 신규 보안 클러스터(B)는 기존 broker 변경을 피하지만 데이터·offset 이관과 앱 전환 영향은 별도 검증이 필요합니다.

## 변경을 작은 단계로 분리

| 단계 | 기존 앱/9092 | 내부 통신·ACL | 진행 기준 |
| --- | --- | --- | --- |
| 준비 | 변경 없음 | 현행 유지 | 앱별 설정·허용 지연·보안 인벤토리 확인 |
| 보안 client 포트 추가 | 9092 PLAINTEXT와 기존 앱 설정/프로세스 유지 | controller와 inter-broker 유지, authorizer 추가하지 않음 | 한 broker씩 정상 종료/기동 후 RF/ISR·quorum·앱 진행 회복 |
| 공존 관찰 | 9092 계속 제공 | 새 포트는 TLS/SCRAM 인증만, ACL 통제 미완료 | 새 계정으로 별도 topic/group의 전송·소비·commit 및 TLS 부정 검증 |
| 앱별 전환 | 아직 남은 앱 때문에 9092 유지 | 사전 검증한 새 포트 이용 | 앱 소유자가 승인한 배포·재연결과 메시지/commit/drain 검증 |
| 내부 경로/권한 통제 | 자동 연속 실행하지 않음 | 내부 인증·StandardAuthorizer·default deny | 별도 변경 계획과 동일 조건 리허설 성공 필요 |
| 기존 포트 제거 | 모든 legacy 사용이 없을 때만 | 임시 허용 해제 | 실제 새 client 전송·소비·commit·lag 회복, 모든 legacy/내부 연결 점검 |

기존 listener 이름과 advertised9092 endpoint를 그대로 보존하면 해당 listener를 통해 받은 metadata는 기존 client 경로를 제공합니다. 신규 listener는 같은 topic/partition 데이터를 별도 보안 endpoint로 제공합니다. 새 클러스터로 데이터를 복사하는 방식이 아니며 topic/group 이름과 offset 데이터를 유지합니다. 실험에서는 두 listener의 cluster ID와 파티션 assignment가 같은지도 독립 조회로 비교합니다.

처음부터 9092의 protocol을 SASL_SSL로 바꾸지 않습니다. 동일 host:port가 PLAINTEXT와 SASL_SSL을 동시에 받도록 구성하지 않습니다. 새 client 포트는 예를 들어9094이며 실제 포트는 입력받아 방화벽과 advertised endpoint를 확인합니다. **9092가 inter-broker 경로라면 앱 전환만 끝났다고 닫을 수 없습니다.** controller 경로도 별도로 확인합니다.

## 적용 전 필수 확인

- Kafka3.9.1/KRaft, 실제 node ID/cluster ID와 현행 설정·동적 overrides를 비공개 백업합니다. 모든 업무/내부 topic의 RF3/ISR3, minISR 및 quorum 상태를 확인합니다. __consumer_offsets RF1이면 먼저 RF3으로 올리고 전체 ISR 회복을 확인합니다. RF3은 앱 영향 0의 보장이 아닙니다.
- controlled.shutdown.enable의 실제값과 정상 종료 방식을 확인합니다. C는 true를 사용했으며 false 환경의 동일 동작을 검증하지 않았습니다. broker/controller가 같은 프로세스인지, 데이터/metadata 디스크 지연 및 quorum 선거 안정성도 확인합니다. 설정이 다르면 동일 조건 리허설을 추가합니다.
- producer acks/retries/delivery.timeout.ms/max.block.ms, bootstrap broker 목록, idempotence/transaction, 프레임워크 예외 처리와 consumer session.timeout.ms/max.poll.interval.ms, commit 방식·허용 지연을 앱 소유자와 확인합니다. 임의로 timeout을 늘려 SLA를 충족했다고 처리하지 않습니다. 앱 정보/허용 지연이 확인되지 않으면 운영 변경을 보류합니다.
- 현재 앱의 고유 ID/전송 성공과 오류/소비/중복/commit/lag/처리 공백 기준을 확보합니다. 성공 ACK와 실제 시도를 분리하고 마지막 drain 후 누락을 판정합니다. commit 재개와 앱 수준 부작용도 확인합니다.
- CA/SAN은 실제 advertised DNS/IP와 일치해야 합니다. truststore/keystore와 비밀은600 권한 파일로 배포하고 hostname verification은 유지합니다. 기존 listener의 advertised host/port는 그대로 유지합니다. 새 포트는 필요한 앱/관리 호스트에서만 접근하도록 제한합니다.

## 한 broker에서 추가할 설정

[delta 예제](../config-examples/add-client-listener.delta.properties.example)는 전체 server.properties가 아닙니다. 기존 listener 이름/주소/프로토콜을 읽고 병합하여 검토합니다. 기존 controller.listener.names/controller.quorum.voters/inter.broker.listener.name/node.id/log.dirs는 변경하지 않습니다. 운영 데이터에 format을 실행하지 않습니다.

기존 관리 경로에서 SCRAM 계정을 먼저 발급하고 새 listener용 서버 JAAS를 보호된 설정에 준비합니다. `kafka-security.sh issue-users`는 먼저 dry-run하고 실제 cluster ID 확인 후 --apply합니다. authorizer 없는 기존 관리 경로는 인증되지 않은 다른 이용자도 관리 API를 실행할 수 있으므로 네트워크 제한이 필수입니다.

KRaft3.9.1은 listener 추가를 동적 변경으로 처리하지 않으므로 설정 병합 후 한 broker씩 정상 SIGTERM/관리 서비스 방식으로 재시작합니다. 새 포트 추가와 controller/internal/authorizer 변경을 같은 재시작에 묶지 않습니다. 한 broker를 중지하기 전에 나머지 두 노드와 전체 ISR 및 앱의 정상 진행을 확인합니다. 재기동 후 broker 등록, quorum high watermark/leader/epoch, 전체 ISR3, 기존 앱의 send/consume/commit·lag가 기준으로 회복한 것을 확인한 후 다음 노드로 진행합니다.

새 포트만 추가한 상태는 **인증과 암호화는 있지만 topic/group ACL 제한은 없는 공존 상태**입니다. 계정이 있으면 관리 API 접근도 가능할 수 있습니다. 일반 앱에 관리자 전용 통제가 완료됐다고 안내하지 마세요. 검증 전 신규 계정/포트의 접근 범위를 제한하고 승인된 shadow client로만 확인합니다. User:ANONYMOUS super user나 default allow로 이 단계를 완성 상태처럼 만들지 않습니다.

## 중단 기준과 복구

broker 등록 실패, quorum 불안정, offline partition/ISR 부족, 예상 밖 인증/권한 오류, send/commit 오류 또는 앱별 지연 기준 초과 시 다음 broker를 변경하지 않습니다. timeout을 늘려 자동 진행하지 않습니다. 이미 정상인 두 노드는 유지하고 실패 노드의 비공개 원래 설정으로 해당 노드만 복구합니다. 앱이 새 포트로 옮겨졌다면 listener를 제거하기 전에 기존 경로로 복귀하거나 호환 endpoint를 먼저 확보해야 합니다. 데이터/cluster ID/log.dirs를 바꾸거나 재format하지 않습니다.

이전 A 실패는 최종 기존 포트 제거 중 발생했습니다. 새 경로는 해당 단계를 자동 수행하지 않으며 client 전환도 수행하지 않습니다. 후속 전체 보안 전환은 [기존 상세 절차](MIGRATE-PLAINTEXT.md)를 설계 참고로만 사용하고, 동일 조건에서 전체 리허설을 통과하기 전 운영 승인 대상으로 삼지 않습니다.

## 격리 재현

```sh
export KAFKA_HOME=/path/to/kafka-3.9.1
export JAVA_HOME=/path/to/jdk17
python3 scripts/lab.py --scenario C --directory runtime/C-NEW --port-base 16500 --client-profile reference
```

reference는 producer delivery120초/max-block60초/request30초, consumer API/commit60초를 사용하는 참조 제한시간입니다. NAS 동일 호스트 실험에서 기본 quorum 시간으로 선거 반복이 관측되면 변경을 보류해야 합니다. 필요시 격리 재현에만 --lab-quorum-timeouts extended를 추가할 수 있으며, 이 설정은 baseline부터 election10초/fetch20초/request20초입니다. 운영 broker 설정에 복사하지 마세요. 기본 quorum 시간과 다른 시험 조건이므로 client 제한시간만의 효과를 비교한 실험이 아닙니다. 운영 앱 설정을 자동 변경하지 않으며 운영 SLA 충족을 의미하지 않습니다. --client-profile short는 비교용 짧은 제한시간을 사용합니다.

C는 전용 새 데이터/포트/테스트 HOME만 사용하며 PLAINTEXT 클러스터를 먼저 생성합니다. 앱을 변경하지 않고 새 SASL_SSL 포트만 추가하여 순차 재시작, 별도 secure topic/group, 잘못된 암호/hostname/익명 접속, drain 및 기존 commit 위치 재개를 검증합니다. C는 운영 실행기가 아닙니다. 결과와 실제 한계는 [RESULTS](RESULTS.md)에 기록합니다.

근거: [Apache Kafka 실행 중 보안 도입](https://kafka.apache.org/39/security/incorporating-security-features-in-a-running-cluster/)은 기존 PLAINTEXT를 유지하면서 secure 포트를 추가하고 client와 내부 통신 전환을 단계로 나누도록 안내합니다. [3.9.1 DynamicBrokerConfig](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/DynamicBrokerConfig.scala)의 KRaft listener 변경 제한도 확인했습니다. 일반 공식 순서는 운영 앱 오류 0에 대한 보증이 아닙니다.
