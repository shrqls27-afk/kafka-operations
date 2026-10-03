# 공식 출처와 판단

조사일: 2026-10-03. 대상 실행 버전: Apache Kafka 3.9.1. 3.9 문서는 minor 버전 공통이며 구현 확인은 3.9.1 tag로 구분했습니다.

1. [Apache Kafka 3.9 Basic Kafka Operations](https://kafka.apache.org/39/operations/basic-kafka-operations/), Increasing replication factor / Limiting Bandwidth Usage / Safe usage of throttled replication: 추가 replica를 custom JSON에 명시하고 `--execute`로 RF를 확대합니다. 같은 JSON으로 `--verify`하고 완료 시 throttle을 해제합니다. throttle이 유입량보다 작으면 복제가 진전하지 못할 수 있습니다. 기존 assignment를 보존해야 합니다. 이는 온라인 admin 절차이며 broker 재시작 단계가 없습니다.
2. [3.9 Broker Configs](https://kafka.apache.org/39/configuration/broker-configs/), offsets.topic.replication.factor: 내부 토픽 생성 RF이며 기본값 3, update mode read-only입니다. 이 설정 자체를 실행 broker에 반영하려면 재시작이 필요하지만 **기존 토픽 replica 재할당에는 설정 변경/재시작이 필요하지 않습니다**. 복제본 수와 min.insync.replicas는 서로 다른 조건입니다.
3. [Apache Kafka 3.9.1 AutoTopicCreationManager.scala](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/AutoTopicCreationManager.scala): offsets topic용 CreatableTopic에 offsetsTopicReplicationFactor를 지정하는 생성 경로를 확인했습니다. 설정은 생성 요청에 사용되며 기존 assignment를 변경하는 경로가 아닙니다. 이것과 운영 문서의 별도 재할당 절차를 함께 근거로, 설정 수정만으로 기존 RF가 자동 변경되지 않는다고 판단합니다.
4. [KIP-115](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/67639958/KIP-115%2BEnforce%2Boffsets.topic.replication.factor%2Bupon%2B__consumer_offsets%2Bauto%2Btopic%2Bcreation): 내부 토픽 자동 생성 시 replication factor 요구를 만족하는 broker 수가 필요함을 설명합니다.
5. [Apache Kafka 3.9.1 KafkaConsumer.java](https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java), Javadoc의 offset management 및 commitSync: 커밋 위치는 다음 소비 위치이고, coordinator 탐색/일시 오류에 대한 재시도가 존재하지만 timeout/commit 실패 가능성이 있습니다. API가 성공했다는 것과 내부에서 재시도가 전혀 없었다는 것은 다릅니다.
6. [Apache Kafka 3.9 KRaft](https://kafka.apache.org/39/operations/kraft/): KRaft controller quorum 구성과 combined broker/controller 운용에 관한 설명. NAS 실험은 절약을 위해 combined 역할이며 회사의 물리 분리/네트워크 특성을 재현하지 못합니다.
7. [Apache Kafka 3.9 Design](https://kafka.apache.org/39/design/design/): 전달 보장과 커밋 위치, 처리 후 commit 전 실패 시 재처리 가능성을 설명합니다. offsets RF=3만으로 비즈니스 exactly-once를 보장하지 않습니다.
8. [Apache Kafka 3.9.1 GroupCoordinator.scala](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/group/GroupCoordinator.scala): offsets partition leader의 election/resignation에 따른 group metadata 로딩/해제 경로가 존재합니다. leader 변경은 coordinator 상태 전환과 재탐색/재시도 지연을 유발할 수 있습니다.

공식 문서를 짧은 NAS 실험 결과로 대체하지 않습니다. 기존 replica를 첫 위치에 유지하면 불필요한 preferred leader 변경을 줄일 수 있지만 현재 leader 유지나 지연 0을 보장하는 계약은 아닙니다. 네트워크, disk, ISR 축소, controller/leader 전환, quota, client timeout에 따라 일시 지연과 오류가 발생할 수 있습니다.
