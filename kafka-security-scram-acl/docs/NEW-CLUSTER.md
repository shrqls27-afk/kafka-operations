# 처음부터 보안 설정한 신규 KRaft

기존 데이터에는 kafka-storage format을 실행하지 않습니다. 신규 전용 root와 그 하위의 빈 데이터 경로만 사용합니다. 세 node.id는 서로 다르고 cluster ID는 동일해야 합니다. 운영 배포 전에 구성/포트/CA/인증서와 모든 경로를 검토합니다.

1. JDK17·Kafka3.9.1·Python3.8을 반입하고 ~/kafka/current를 준비합니다. 노드별 SASL_SSL broker listener와 mTLS controller listener, static voters를 구성합니다. controller.listener.names는 controller SSL listener를 지정하며 SCRAM을 controller 경로에 추측하여 사용하지 않습니다.
2. 인증서 SAN과 advertised 주소를 일치시키고 CA truststore와 노드별 keystore를 보호된 경로에 배포합니다. controller ssl.client.auth=required, hostname verification=HTTPS를 적용합니다. listener별 TLS 설정을 검토하세요.
3. 별도 security-root, topic-admin, 앱별 producer/consumer, 노드별 broker 계정을 준비합니다. StandardAuthorizer를 broker/controller 양쪽에 설정하며 default deny와 내부/root super.users를 먼저 준비합니다. controller 인증서 DN principal을 정확히 확인하세요. super user는 ACL을 우회하므로 최소한의 내부/bootstrap 역할만 지정합니다.
4. prepare-new는 존재하지 않는 신규 root만 허용합니다. 기본은 계획입니다. 아래 명령은 대상 검토 후에만 실행합니다.

```sh
bash kafka-security.sh prepare-new --new-root /new/cluster --apply --confirm 'NEW EMPTY CLUSTER'
bash ~/kafka/current/bin/kafka-storage.sh random-uuid
bash kafka-security.sh format-new --new-root /new/cluster --server-config /protected/node.properties --cluster-id CLUSTER_ID --input /protected/users.properties
# plan 확인 후 --apply --confirm CLUSTER_ID 추가
```

format-new는 marker/하위 빈 절대 데이터 경로/authorizer/default deny/ANONYMOUS superuser 금지를 검사하고 기존 데이터나 --ignore-formatted를 허용하지 않습니다. Kafka3.9.1 StorageTool의 --add-scram을 보호된 @argfile로 전달합니다. 비밀은 명령줄에 나오지 않으며 argfile은 600으로 보존됩니다. 세 노드의 최초 SCRAM 계정은 동일하게 준비합니다. marker만으로 모든 운영 경로를 자동 식별할 수 없으므로 경로 검토 책임은 관리자에게 있습니다.

5. 세 노드를 시작하여 quorum/관리 접속을 확인하고 관리/앱 ACL을 적용합니다. topic 생성은 topic-admin/root가 수행합니다. 일반 앱에게 Create/Delete/Alter/ACL/계정 관리 권한을 주지 않습니다. producer는 Write/Describe, consumer는 topic Read/Describe 및 허용 group Read/Describe를 갖습니다.
6. 정상/거부 테스트와 전체 내부·업무 topic의 RF/ISR를 확인하고 rolling restart 이후 자격 증명/ACL 지속 여부를 확인합니다. 로그의 민감 정보를 보호하고 변경 기록만 익명화하여 공유합니다.

Kafka3.9.1의 비transactional idempotent producer는 cluster IdempotentWrite 또는 어떤 topic Write 권한으로 InitProducerId가 허용됩니다. 이 실험은 topic Write만 가진 idempotent producer 성공을 확인합니다. transactional producer는 지정 transactional ID Write와 topic Write를 준비하며 다른 transactional ID는 거부해야 합니다. 트랜잭션에 consumer offsets를 넣는 EOS 경로의 group 권한은 별도 검증 대상입니다.

TLS 키/CA 발급·만료/rotation과 외부 host/firewall 검증은 운영 PKI 정책에 맞게 수행합니다. 신규 설치 예제의 placeholder를 실제 환경으로 대체해야 하며 바이너리와 비밀 파일은 공개 저장소에 넣지 않습니다.

controller가 실제 광고하는 endpoint도 인증서 SAN과 일치해야 합니다. Kafka3.9.1은 controller advertised endpoint를 명시하지 않으면 listeners의 host를 상속합니다. DNS SAN만 가진 인증서에 숫자 IP controller endpoint를 광고하면 올바른 voters DNS 설정만으로 문제를 해결할 수 없습니다. 운영에서는 이 주소와 CA 신뢰·hostname 검증을 각각 확인하세요.
