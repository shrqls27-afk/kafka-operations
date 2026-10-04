# 공식 근거 (Kafka 3.9.1)

조사·실험: 2026-10-03~04. 아래 3.9 문서와 3.9.1 태그 코드를 함께 확인했습니다.

- [실행 중 클러스터에 보안 도입](https://kafka.apache.org/39/security/incorporating-security-features-in-a-running-cluster/): secure port 추가, client 전환, 내부 통신 및 plain 제거 rolling 순서.
- [SASL](https://kafka.apache.org/39/security/authentication-using-sasl/): SCRAM/SASL 구성. 문서의 ZooKeeper 예시는 KRaft 신규 format에 사용하지 않습니다.
- [ACL](https://kafka.apache.org/39/security/authorization-and-acls/): StandardAuthorizer, super.users, default deny, forwarding, producer shortcut Create.
- [Listener](https://kafka.apache.org/39/security/listener-configuration/), [TLS](https://kafka.apache.org/39/security/encryption-and-authentication-using-ssl/): listener별 protocol, CA/SAN 및 hostname 검증.
- [StorageTool.scala](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/tools/StorageTool.scala): 신규 format --add-scram 및 @file 인수 파싱.
- [DynamicBrokerConfig.scala](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/DynamicBrokerConfig.scala): KRaft listener 추가/제거 동적 제한 및 protocol 변경 제한.
- [SocketServer.scala](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/network/SocketServer.scala): controller.listener.names 복수 inbound listener.
- [RaftManager.scala](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/raft/RaftManager.scala), [KafkaRaftClient.java](https://github.com/apache/kafka/blob/3.9.1/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java): 첫 controller listener 보안 경로와 정적 voter endpoint 매핑. dual listener mTLS 전환은 이 코드에 근거한 실험 범위의 추론입니다.
- [StandardAuthorizerData.java](https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/metadata/authorizer/StandardAuthorizerData.java): 초기 ACL loading 이전 super user 허용.
- [KafkaApis.scala](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala): Envelope/InitProducerId/계정·ACL API 권한 확인.
- [KAFKA-15513](https://issues.apache.org/jira/browse/KAFKA-15513): KRaft controller SCRAM bootstrap 제약을 검토하여 controller는 mTLS로 분리했습니다.

- [KafkaConfig.scala](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaConfig.scala): effectiveAdvertisedControllerListeners는 명시적 controller advertised endpoint 또는 listeners의 host를 사용합니다. voters/bootstrap의 DNS만 SAN과 일치시켜서는 충분하지 않습니다.

- [Producer 설정](https://kafka.apache.org/39/configuration/producer-configs/), [Consumer 설정](https://kafka.apache.org/39/configuration/consumer-configs/): retry는 delivery timeout 내에서만 지속되며 bootstrap 목록·session/max poll 설정과 앱 오류 처리의 적합성을 별도로 확인해야 합니다.

- [Broker 설정](https://kafka.apache.org/39/configuration/broker-configs/): controller quorum election/fetch/request 제한시간의 의미와 기본값. NAS 격리 실험용 변경은 운영 권장값이 아닙니다.
