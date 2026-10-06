# producer·consumer 유지 상태 RF1 → RF3 실제 검증

검증일: 2026-10-06. **최종 단일 실행 파일을 NAS Linux와 WSL Linux의 격리 3노드에서 각각 실제 실행했습니다.** RF 변경 중 broker·producer·consumer를 중지하거나 재시작하지 않았으며, 모든 offsets 파티션의 RF3/ISR3와 송수신·커밋 지속을 확인했습니다. 운영 물리 서버에 접속하거나 변경한 결과는 아닙니다.

## 실행 파일과 조건

- 실행 파일: [offsets-rf3.sh](../offsets-rf3.sh)
- SHA256: `53782445337b55fd4c5681198e671fbe558778778c18e97e05863e50cf07ef7c`
- Apache Kafka 3.9.1, JDK17, KRaft combined 3노드, offsets 50파티션.
- 새 격리 토픽의 RF1 준비는 검증 코드에서만 수행합니다. 배포 실행기는 기존 RF1을 RF3으로 확대하며 RF를 낮추지 않습니다.
- 데이터 토픽 1파티션/RF3/minISR2, Java producer `acks=all`·idempotence, consumer 1그룹·수동 `commitSync`, 순차 전송 목표 약20건/초. 전송 성공 ACK와 소비 ID를 전수 대조했습니다.
- 복제 throttle 16,384bytes/sec는 재할당 관측을 위한 **시험값이며 운영 권장값이 아닙니다.** 관측용 Admin 조회는 소비 루프와 분리하여 약250ms 간격으로 수행했습니다.

## 최종 배포본 결과

| 항목 | NAS Linux | WSL Linux |
| --- | ---: | ---: |
| 송신 성공 / 고유 소비 | 6762 / 6762 | 3966 / 3966 |
| 송신·소비·커밋 오류 | 모두0 | 모두0 |
| producer 재시도 누계 | 0 | 0 |
| drain 후 누락 / 중복 / ACK 없는 소비 | 모두0 | 모두0 |
| 실제 재할당 진행 상태 관측 | 54회 | 47회 |
| 관측한 재할당 구간 송신 / 소비 / commit | 238 / 238 / 220 | 228 / 228 / 221 |
| 최종 offsets RF3/ISR3 | 50/50 | 50/50 |
| throttle 원래 설정 복원 | 일치 | 일치 |
| 완료 후 재개 시 execute 중복 전송 | 없음 | 없음 |
| drain 후 커밋 위치 재접속 / 추가 marker | 정상 / 20개 | 정상 / 20개 |

표의 commit은 메시지 수가 아니라 성공한 commit 호출 수입니다. 각 시험에서 실제 변경 CLI 실행은 1회이며, 변경 직전 `NO` 시 assignment·leader·ISR·설정 불변도 확인했습니다. 의도적인 consumer 재접속은 RF 변경 완료와 drain **이후 별도 검증**이며 변경 중에는 동일 프로세스와 client 객체를 유지했습니다.

근거: [NAS 요약](../evidence/nas-live-20261006-02/summary.json), [WSL 요약](../evidence/local-20261006-03/summary.json), 각 폴더의 `partitions.json`, `original.json`, `target.json`, `raw-evidence-sha256.json`. 원본 이벤트·CLI 로그·테스트 데이터는 Git 제외 runtime/시험 경로에 보존했습니다.

## 지연은 0이 아니었습니다

단위 ms, 각 셀은 **p95 / 최대**입니다. during은 실제 실행 승인 직전부터 독립 RF3/ISR3 확인까지의 구간이며, 위 표의 실제 재할당 관측 구간보다 넓습니다.

| 환경·구간 | 송신 ACK 지연 | commit 지연 | 송신 시도→소비 지연 |
| --- | ---: | ---: | ---: |
| NAS baseline | 19 / 33 | 7 / 21 | 20 / 47 |
| NAS during | 13 / 756 | 8 / 1310 | 14 / 1260 |
| NAS after | 9 / 57 | 7 / 51 | 8 / 58 |
| WSL baseline | 3 / 7 | 1 / 2 | 4 / 8 |
| WSL during | 2 / 6 | 1 / 493 | 2 / 443 |
| WSL after | 1 / 3 | 1 / 7 | 2 / 3 |

**계획적인 중지·재시작 없이 처리 지속은 확인했지만, 지연 변화가 없다는 결과는 아닙니다.** NAS 변경 중 최대 commit 1,310ms와 소비 지연1,260ms가 관측됐습니다. 실제 앱의 지연 허용 기준과 비교해야 합니다. 내부 consumer 재시도 전체를 계측한 것은 아니며, 표의 오류0은 관측한 앱 호출·필수 측정 오류 기준입니다.

## NAS 상시 서비스 보존

시험 전후 기존 Kafka3개·UI·Codex·Telegram의 PID와 명령이 동일했습니다. 테스트 프로세스만 종료하고 테스트 포트6개 폐쇄를 확인했습니다. 독립 사후 조회에서도 기존 cluster ID 일치, broker3개, 정상 파티션53개와 유효한 quorum leader/high watermark를 확인했습니다. [익명화한 사전·사후 상태](../evidence/nas-live-20261006-02/permanent-cluster-health.json)

## 이번에 수정한 내용

1. 테스트 준비는 전체 노드 format 완료 후 전체 기동하며, 고정10초 대기 대신 quorum/broker/ISR readiness를 확인합니다. NAS 상시 클러스터가 불건전하면 격리 시험도 시작하지 않습니다.
2. 배포 실행기는 status→replication→status와 간격 있는 연속2회 관측으로 CID·voter·leader/epoch·HWM·lag를 검사합니다. 일시적인 전환은 제한시간 내 재조회하고, 지속 불안정은 실행 전에 보류합니다.
3. throttle 해제 전 이번 작업의 예상 값·설정 출처와 실제 값을 비교합니다. 다른 관리자 변경을 덮어쓰지 않으며, 값 비교와 실행 사이의 경쟁 조건 때문에 동시 관리 작업 금지는 여전히 필요합니다.
4. 최적화된 Python에서도 안전 검사가 생략되지 않도록 명시적 검사를 적용했습니다. Java 컴파일은 소스 기반 캐시와 flock을 사용하고, 컴파일·실행 UTF-8을 명시해 NAS의 ASCII locale 문제를 수정했습니다.
5. 관측용 Admin 조회를 consumer poll과 분리하고 warmup/baseline/during/after/drain, 재할당 실제 관측, ACK·소비 전수 비교를 기록합니다.

회귀시험 **47개가 NAS Python3.8 및 WSL Python3.12에서 통과**했습니다. Python/Bash 문법·Java 컴파일·최종 .sh 내장4파일 일치와 실행된 파일의 SHA256을 확인했습니다.

공개 전 점검에서 시험 도구의 상시 broker 주소를 `--existing-bootstrap` 또는 `RF3_EXISTING_BOOTSTRAP`으로 주입하도록 바꿨습니다. NAS 주소 미지정은 사전 차단하며 회귀3개를 추가했습니다. 이 변경은 시험 도구에만 해당하고, 실제 검증한 배포 `.sh`의 SHA256은 바뀌지 않았습니다. 실행 당시 시험 도구 원본도 비공개 runtime에 보존했습니다.

## 앞선 실패와 한계

- 오전 NAS 시도는 RF 변경 전에 기동 실패했습니다. [기존 실패 기록](LIVE-VALIDATION-ATTEMPT-20261006.md)은 수정하거나 성공으로 바꾸지 않았습니다. 예약 디스크 검사가 실행 중일 때 디스크 포화를 관측했지만 단일 근본 원인으로 확정하지 않습니다. 최종 NAS 시험 전에는 해당 검사가 실행 중이지 않았습니다.
- WSL 첫 수정본은 쿼럼 리더 전환·follower lag1에서 사전 점검이 차단됐습니다. 이를 무시하지 않고 연속 안정성 gate로 보완했습니다. UTF-8 수정 전 추가 성공 시험도 별도 [이전 수정본 결과](../evidence/local-20261006-02/summary.json)에 남겨 최종 파일과 구분합니다.
- 단일 호스트의 3개 JVM, 작은 offsets 데이터와 한 consumer group을 사용했습니다. 물리3노드·대규모 offsets/트래픽·다수 그룹·다른 client 언어·자동 commit·트랜잭션·TLS/SASL·업무 DB 처리·변경 중 장애 및 실제 중도 단절/resume는 이번 실측 범위가 아닙니다. 완료 후 resume와 불명확 요청 재전송 방지의 모의 검증을 실제 중도 단절 검증으로 표현하지 않습니다.
- 모든 운영 환경에서 오류·지연0을 보장하지 않습니다. 실제 적용은 정상 quorum/ISR, 자원 여유, 승인한 throttle, 중요 그룹의 오류·commit·lag 상시 관측이 전제입니다.

## 운영 실행

Kafka 설치 계정에서 `~/kafka/current`를 사용합니다. JDK17(javac 포함), Python3.8+, Bash, util-linux의 flock을 준비하고 최종 .sh 한 파일을 반입하세요.

```bash
bash offsets-rf3.sh
```

서버3개의 broker 주소, 기존 인증 파일(없으면 `-`), 관측 그룹과 승인된 throttle을 입력합니다. 계획을 확인한 뒤 실행하며 broker·producer·consumer를 종료하는 명령은 포함하지 않습니다. 상세 사용법은 [ONE-SCRIPT](ONE-SCRIPT.md)를 참고하세요.
