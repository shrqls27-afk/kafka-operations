# 공식 근거

확인일: 2026-10-06. 실제 설치본 `--version`은 3.9.1, JDK는 17.0.20.1이다. 설치본 CLI `--help` 원본은 비공개 runtime에 보존하고 해시를 evidence에 기록한다. 문서는 3.9 계열 설명이며 옵션·코드 판단에는 **3.9.1 태그**를 우선했다.

- [ConsoleProducer.scala 3.9.1](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/tools/ConsoleProducer.scala): sync 경로 `send(record).get()`, 정상 EOF/close 후 exit0, 예외 exit1. 메시지별 성공 ACK 출력은 제공하지 않는다.
- [CommandLineUtils.java 3.9.1](https://github.com/apache/kafka/blob/3.9.1/server-common/src/main/java/org/apache/kafka/server/util/CommandLineUtils.java): 명시 CLI 전용 옵션 → 이미 설정한 producer property → CLI 기본값 순서로 merge한다. `message-send-max-retries` 기본 3과 달리 본 시험은 property `retries=2147483647`을 지정한다. 전용 옵션을 추가로 지정해 덮어쓰지 않는다.
- [ConsoleConsumer.java 3.9.1](https://github.com/apache/kafka/blob/3.9.1/tools/src/main/java/org/apache/kafka/tools/consumer/ConsoleConsumer.java): 기본 consumer 실행과 종료 처리.
- [DefaultMessageFormatter.java 3.9.1](https://github.com/apache/kafka/blob/3.9.1/tools/src/main/java/org/apache/kafka/tools/consumer/DefaultMessageFormatter.java): partition/offset/timestamp 출력.
- [KRaft](https://kafka.apache.org/39/operations/kraft/): combined 역할, 과반수 quorum, storage format 및 metadata-quorum 조회. 중요한 운영 배포에서는 controller 분리 고려.
- [Producer 설정](https://kafka.apache.org/39/configuration/producer-configs/): acks, idempotence, retries, delivery/request timeout, partitioner.
- [Broker 설정](https://kafka.apache.org/39/configuration/broker-configs/): offsets RF, minISR, unclean leader election.
- [Consumer 설정](https://kafka.apache.org/39/configuration/consumer-configs/): auto commit과 interval.
- [Kafka 3.9.1 기본 스크립트](https://github.com/apache/kafka/tree/3.9.1/bin): storage/server-start/topics/console-producer/console-consumer/consumer-groups/metadata-quorum/broker-api-versions/leader-election.

delivery.timeout은 재시도 총 시간의 상한이지 반드시 그 시간 동안 재시도한다는 보장이 아니다. 본 시험에서는 큰 retries와 120초 delivery timeout을 함께 지정했으나, 실제 내부 재시도 횟수는 CLI 출력으로 전수 계측하지 않았다.
