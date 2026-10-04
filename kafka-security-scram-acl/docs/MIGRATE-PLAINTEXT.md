# 기존 PLAINTEXT에서 단계적 보안 전환

**운영 앱을 유지하는 우선 경로는 [KEEP-PLAINTEXT](KEEP-PLAINTEXT.md)입니다.** 아래 전체 전환은 A에서 끝까지 성공하지 못한 설계 참고 절차입니다. 이를 운영에서 연속 자동 실행하지 마세요. 기존9092 앱이 남아 있으면 포트 제거·protocol 변경·client 강제 전환을 진행하지 않습니다.

시작 조건: Kafka 3.9.1 KRaft 정적 quorum 3노드. 현행 listeners/advertised.listeners, controller.listener.names, controller.quorum.voters, inter.broker.listener.name, listener.security.protocol.map과 모든 client 종류/호스트/버전을 기록합니다. RF·minISR·전체 ISR·quorum을 확인합니다. __consumer_offsets가 RF1이면 [기존 RF1→3 자료](../../consumer-offsets-rf1-to-rf3/README.md)를 먼저 검토하고 완료해야 합니다. RF1 또는 ISR 부족 상태에서 rolling restart의 운영 지속을 보장할 수 없습니다.

1. 설정·원래 ACL·계정 목록·인증서 유효기간·원래 listener 경로를 비공개로 보존합니다. 운영 CA의 SAN은 실제 advertised DNS/IP와 일치해야 하며 모든 client/broker/controller가 CA를 신뢰해야 합니다. firewall과 advertised 주소의 왕복 접근성을 확인합니다. hostname verification은 HTTPS로 유지합니다.
2. authorizer를 켜기 전에 관리자와 노드별 broker SCRAM 계정을 기존 관리 경로로 발급합니다. 현행 무인증 경로는 변경 동안 네트워크에서 제한합니다. 진입점 issue-users는 기본 계획이며 --apply/실제 cluster ID 확인이 있어야 발급합니다. 노드별 비밀 파일은 600으로 배포합니다.
3. 기존 9092 PLAINTEXT를 유지하면서 별도 SASL_SSL client listener(예:9094)와 controller mTLS listener를 추가합니다. 한 노드씩 재시작하고 전체 ISR/quorum 회복 후 다음 노드로 진행합니다. KRaft3.9.1에서 listener 추가/삭제는 재시작이 필요하고 같은 이름의 protocol 교체를 동적 설정으로 해결할 수 없습니다. SCRAM 사용자와 ACL은 Admin API로 변경하며 별도 broker 재시작을 요구하지 않습니다.
4. controller 전환은 정적 quorum/동일 node ID 조건에서 두 inbound controller listener를 모든 노드에 먼저 준비한 후, controller.listener.names 첫 항목과 같은 ID의 voters endpoint를 mTLS 경로로 순차 전환합니다. 이는 3.9.1 코드와 격리 실험에 근거한 제한된 절차입니다. 동적 quorum이나 다른 버전에 일반화하지 마세요. 같은 host:port를 plain/TLS로 동시에 제공하지 않습니다.
5. broker 내부 통신을 SASL_SSL/SCRAM으로 전환하면서 broker/controller 모두 StandardAuthorizer를 적용합니다. security-root와 노드별 내부 SCRAM principal 및 controller 인증서 principal(DN)을 먼저 super.users에 설정합니다. ACL 로딩 전에도 이 내부 principal들이 허용되어야 합니다. Forwarded Envelope는 broker ClusterAction 및 전달받은 client 권한을 각각 확인합니다. 앱을 super user로 두지 않습니다.
6. 실험에서는 이 단계에만 allow.everyone.if.no.acl.found=true를 사용했습니다. 임시 허용 동안 기존 plain과 신규 보안 listener 모두를 필요한 관리자/노드 및 기존 앱 주소로 firewall 제한하고, 신규 앱의 보안 listener 접근은 default deny와 앱 ACL 준비 후 개방합니다. 관리/앱 ACL과 기존 무인증 client의 임시 User:ANONYMOUS ACL을 등록한 후 즉시 false로 순차 재시작합니다. ANONYMOUS는 super user로 사용하지 않습니다. 무인증 client는 인증된 개인 신원으로 구분할 수 없으며 source host·네트워크 경계·topic/group 제한만 가능합니다. wildcard 호스트/관리 권한을 임시 허용하지 마세요. 실제 운영 IP의 NAT/주소 변환을 확인해야 합니다.
7. 앱별로 truststore와 SCRAM 계정, 신규 bootstrap 주소를 배포하고 전환합니다. 실제 client 재생성/consumer 재시작이 필요한 경우 중단과 rebalance를 기록합니다. 전송/소비/commit·lag·오류를 확인하고 기존 listener 접속을 제거합니다.
8. 모든 client/내부 경로 전환을 확인한 후 OLD PLAINTEXT와 이전 controller listener를 rolling 제거합니다. 임시 ANONYMOUS ACL을 삭제하고 **allow.everyone.if.no.acl.found=false**임을 확인합니다. 금지 API와 익명 접속 거부를 확인하는 시점부터 완전 권한 통제 상태로 판단합니다.

관리자는 topic Create/Delete/Alter/AlterConfigs 및 Describe를 갖고, 별도 security-root가 계정/ACL을 관리합니다. producer는 특정 topic Write/Describe만, consumer는 topic Read/Describe와 group Read/Describe만 허용합니다. --producer 단축 옵션은 Create를 포함하므로 사용하지 않습니다. auto.create.topics.enable=false 및 consumer allow.auto.create.topics=false를 적용합니다. offsets 내부 topic에 앱 직접 Write를 주지 않습니다.

최종 9092를 SASL_SSL로 재사용하려면 기존 plain 제거 이후 별도 secure listener를 9092에 추가하고 client를 다시 이동하는 변경이 필요합니다. 재연결/재시작과 영향 검증을 별도로 수행하세요.

보류: quorum 이상, offline/under-replicated partition, minISR 미달, SAN/신뢰 실패, 새 관리자 접속 실패, 내부 인증/권한 실패, 예상하지 못한 client 오류, 처리 지연 기준 초과. 다음 노드 변경을 멈추고 원인을 해결합니다. 이전 설정 복구도 rolling이며 현재 client/내부 경로와 호환되는 listener를 먼저 확보해야 합니다. 보안 전환 후 plain 재노출은 위험하고 ACL/자격 증명을 무조건 삭제하면 잠길 수 있습니다. 각 wave의 원본 설정으로 복구 가능성과 승인 범위를 검토하세요.

암호 회전은 새 자격 증명 배포·검증 후 기존 비밀을 폐기합니다. 동일 SCRAM username의 password 교체는 새 인증에서 기존 비밀번호를 즉시 거부하므로 겹침이 필요하면 새 username과 동일 최소 ACL을 먼저 마련하세요. 이미 연결된 세션의 즉시 차단까지 보장하지 않습니다.

운영 지속은 계획적 앱 중지를 줄이는 절차이며 오류/지연 0 또는 완전 무중단 보장이 아닙니다. ISR3/RF3도 물리 서버·네트워크 장애와 실제 부하에 대한 보장 근거가 아닙니다.

권한/접속 확인 예시(보호된 config 파일만 전달):

```sh
bash ~/kafka/current/bin/kafka-topics.sh --bootstrap-server node1.example.invalid:9094 --command-config /protected/admin.properties --describe
bash ~/kafka/current/bin/kafka-acls.sh --bootstrap-server node1.example.invalid:9094 --command-config /protected/security-root.properties --list
bash ~/kafka/current/bin/kafka-configs.sh --bootstrap-server node1.example.invalid:9094 --command-config /protected/security-root.properties --describe --entity-type users
bash kafka-security.sh audit --client /protected/security-root.properties --output /protected/audit-new.json
```

재시작 명령은 설치 방식에 따라 다릅니다. Red Hat systemd라면 검토한 **한 노드에서만** `sudo systemctl restart kafka.service`를 수행하고 해당 서비스의 설정 파일 경로·실행 사용자·환경 변수를 사전에 확인합니다. 수동 설치라면 관리 중인 PID/명령을 확인하여 TERM으로 종료하고 `~/kafka/current/bin/kafka-server-start.sh /protected/server.properties`로 시작합니다. 모든 노드에 동시에 실행하지 않습니다. 실험용 lab.py는 별도 테스트 프로세스에만 적용하며 운영 종료 스크립트로 사용하지 않습니다.

controller가 실제 광고하는 endpoint도 인증서 SAN과 일치해야 합니다. Kafka3.9.1은 controller advertised endpoint를 명시하지 않으면 listeners의 host를 상속합니다. DNS SAN만 가진 인증서에 숫자 IP controller endpoint를 광고하면 올바른 voters DNS 설정만으로 문제를 해결할 수 없습니다. 운영에서는 이 주소와 CA 신뢰·hostname 검증을 각각 확인하세요.

실험은 Kafka 표준 principal builder를 사용했습니다. custom principal builder를 사용하는 운영 환경이라면 KafkaPrincipalSerde의 전달 principal 직렬화/역직렬화 지원을 별도로 확인하세요.

임시 legacy ACL은 최종 진입점에서 금지하므로 아래 공식 CLI를 별도 검토하여 사용합니다. APPROVED_SOURCE_IP/APP_TOPIC/APP_GROUP는 실제 승인된 값으로 입력하고 모든 기존 client의 source 주소(NAT 포함)를 확인합니다. 인증되지 않은 주소 기반 허용은 사용자별 신원을 보장하지 않습니다.

```sh
bash ~/kafka/current/bin/kafka-acls.sh --bootstrap-server node1.example.invalid:9094 --command-config /protected/security-root.properties --add --allow-principal User:ANONYMOUS --allow-host APPROVED_SOURCE_IP --operation Read --operation Write --operation Describe --topic APP_TOPIC
bash ~/kafka/current/bin/kafka-acls.sh --bootstrap-server node1.example.invalid:9094 --command-config /protected/security-root.properties --add --allow-principal User:ANONYMOUS --allow-host APPROVED_SOURCE_IP --operation Read --operation Describe --group APP_GROUP
# 전환 완료 후 동일 조건의 --add를 --remove로 바꾸어 삭제하고 --list로 잔존 여부 확인
```

최종 전환 기준은 새 계정의 허용/거부 검증, default deny, legacy 포트 폐쇄, 임시 ACL 없음이며, 단계별 설정만 바뀌었다고 완료로 판단하지 않습니다.
