> 이 문서는 수정 전 파일에 대한 과거 검토입니다. 현재 배포본의 SHA256과 실제 NAS·WSL 결과는 [최종 검증](LIVE-VALIDATION-20261006.md)을 확인하세요.

# 단일 실행기 최종 재검토

검토일:2026-10-06. Apache Kafka3.9.1 KRaft·Linux 직접 설치 기준입니다. 스크립트 실행 로직을 변경하지 않았으며 운영 클러스터에 명령을 전송하지 않았습니다.

## 판정

기존 RF1→RF3 전체 실행 검증에 사용한 내장 소스와 현재 소스가 동일하고, 이번 문법·컴파일·안전장치 시험도 통과했습니다. 검토 범위에서 RF 변경을 잘못 실행하는 치명적 결함은 발견하지 않았습니다. 모든 환경에서 문제없음·무중단 보장이라는 판정은 아닙니다.

현재 SHA256:

```text
d25d3633b0d02390fc9a16ece7cf49596cf7ccfdab1ad565976bca9dce79de5c
```

| 확인 | 결과 |
| --- | --- |
|GitHub main과 로컬 실행기|바이트 단위 동일|
|내장 company.py/reassign.py/java.sh/OffsetsLab.java|현재 원본 4개와 바이트 단위 동일|
|Bash 문법|통과|
|안전장치 단위/모의 시험|15개 통과|
|JDK17.0.20.1 + Kafka3.9.1 라이브러리 컴파일|종료코드0|
|별도 HOME에서 bash 및 직접 ./ 실행 --help|종료코드0, Kafka 접속 없음|
|보존된 원본 workload 재계산|ACK4769·고유 소비4769·중복0·drain 후 누락0, send/commit 오류0|
|보존된 실제 실행 요약|전체50partition RF3/ISR3·원래replica 보존·throttle 원복·재할당 없음·전체실행0·완료후resume0|

원본 이벤트의 drain ACK/seen 집합과 reconnect marker20개·이전 메시지 replay0도 대조했습니다. 실제 클러스터 전체 실행은 [2026-10-03 검증](STANDALONE-VALIDATION.md)이며 이번에는 실행 로직이 동일하므로 반복하지 않았습니다. 이번 컴파일/모의시험을 새 실제 RF 변경 실험으로 표현하지 않습니다.

## 확인한 제한과 경미한 사항

- 단위 시험에서 operation.lock 파일의 명시적 close가 없어 ResourceWarning이 관측됩니다. 직접 실행 프로세스가 종료되면 OS가 descriptor/lock을 해제합니다. 현재 시험은 통과했고 실행기 코드를 변경하지 않았습니다.
- 최대30분은 polling loop의 관측 기준입니다. subprocess.run에 별도의 강제 wall-clock timeout이 없으므로 개별 CLI 호출 대기까지 반드시30분 안에 종료한다고 보장할 수 없습니다. 연결 장애 시 반복 execute 대신 기록과 실제 서버 상태를 확인합니다.
- state=done의 --resume은 RF/ISR·assignment·현재 재할당을 조회하지만, 현재 throttle configs를 원본과 다시 비교하는 단계는 생략합니다. 최초 완료/clearing에서는 원복 비교를 수행합니다. 완료 이후 새 throttle 변경이 있었다면 done 메시지를 현재 throttle 무설정의 증명으로 사용하지 말고 별도 조회합니다.
- 다른 서버/계정의 동시 재할당·throttle 변경은 로컬 lock으로 막을 수 없습니다. throttle 해제 직전 담당자 확인이 필요합니다.
- TLS/SASL 인증과 진행 중 단절 뒤 resume의 실제 전체 실행은 미검증입니다. 기존 실험은 PLAINTEXT 격리 환경이며 정상 완료 후 resume만 실제 검증했습니다.

전제는 실제broker3개·정상quorum·offsets 전체RF1/ISR1·기존throttle/동시작업 없음·disk/network 여유·JDK17/javac·Python3.8+·Kafka3.9.1·설치계정의 ~/kafka/current입니다. RF3/혼합RF는 새 실행 대상이 아닙니다. 중요한 모든 그룹의 commit/lag·업무 오류/지연은 별도 관측해야 합니다. producer/consumer의 계획적 중지 없이 수행하는 절차와 오류/지연0 보장은 구분합니다.

검증 로그·컴파일 결과·도움말 추출은 Git 제외 runtime에 보존합니다. 이 검토 문서는 로컬 작성이며 commit/push는 수행하지 않았습니다.
