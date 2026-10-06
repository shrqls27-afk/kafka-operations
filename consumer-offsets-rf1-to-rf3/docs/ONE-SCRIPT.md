> 2026-10-06 최종본은 NAS·WSL에서 producer/consumer를 유지한 실제 RF 변경을 검증했습니다. 오류·누락·중복0, 지연 증가는 관측됐습니다. [현재 파일의 결과와 한계](LIVE-VALIDATION-20261006.md)를 확인하세요.

# 운영 환경에서는 .sh 하나로 실행

GitHub에서 **[offsets-rf3.sh](../offsets-rf3.sh)**의 Raw 파일을 다운로드해 운영 서버로 반입합니다.
이 파일 하나만 있으면 됩니다. 실행 시 필요한 도구 소스를 자체적으로 풀며 인터넷에 접속하거나 패키지를 설치하지 않습니다.

Kafka 설치 경로는 **`~/kafka/current`**로 고정했습니다. Kafka 설치 계정으로 서버 한 대에서 실행하세요.
`sudo`로 실행하면 `~`가 root 홈으로 바뀌므로 사용하지 않습니다.

```bash
bash offsets-rf3.sh
```

화면에서 입력할 항목:

1. broker 주소:포트 — 서버 3개의 broker 포트를 쉼표로 구분합니다. controller 포트가 아닙니다.
2. 기존 client.properties 경로 — TLS/SASL 인증이 없다면 Enter(`-`). 비밀번호를 직접 입력하지 않습니다.
3. 관측할 consumer group.id — 없으면 Enter. 중요 그룹의 업무 지표는 별도 관측합니다.
4. 표시된 cluster ID·broker·파티션별 계획과 서버 자원을 확인하고 `YES`.
5. 운영 환경에서 정한 복제 속도(bytes/sec)를 입력하고 실제 변경에 `YES`.
6. RF3/ISR3 완료 후 업무 지표와 다른 throttle 작업이 없는지 확인하고 해제에 `YES`.

**자동 수행:** 도구 버전 확인 → broker/quorum/불건전 파티션 조회 → 원본/목표 JSON 생성 → dry-run →
계획 해시 검증 → 재할당 → 30초 간격 RF/ISR 조회 → 완료 검증 → 확인 후 throttle 해제·원복 비교.

JDK 17(`javac` 포함), Python 3.8+, Kafka 3.9.1, Bash와 util-linux의 `flock`이 필요합니다. `JAVA_HOME` 또는 PATH의 javac로 JDK를 찾습니다.
이 전제는 .sh 파일만으로 대체할 수 없습니다. 필요한 관리 권한은 기존 인증 파일을 사용합니다.
기존 producer/consumer/broker를 중지·재시작하지 않습니다. AI 호출도 하지 않습니다.

## 끊겼을 때

증거와 진행 상태는 `~/kafka-rf3-work/run-.../`에 권한을 제한해 저장됩니다.
터미널 종료나 Ctrl+C는 Kafka 서버에서 진행 중인 재할당을 취소하지 않습니다.

프로그램이 출력한 실제 경로로 아래 명령을 실행합니다.

```bash
bash offsets-rf3.sh --resume ~/kafka-rf3-work/run-실제작업번호
```

재개는 조회·완료 확인부터 수행하고 **execute를 자동 재전송하지 않습니다.**
변경 요청 전 중단은 새로 실행하여 새 계획을 만듭니다. 변경 요청 실패/timeout 이후에는 반드시 `--resume`으로
실제 상태를 확인하세요. 최대 30분 동안 관측한 뒤에도 미완료이면 증거를 남기고 종료합니다.
정체 원인을 해결한 후 재개할 수 있으며, 자동 rollback이나 토픽 삭제는 하지 않습니다.

## 담당자가 판단할 사항

- 서버 3대의 disk/network 여유, quorum 건전성, 업무 commit 지연/lag, 변경 창은 사람이 확인합니다.
- 스크립트는 모든 중요 그룹의 업무 정상 여부를 자동 판정하지 않습니다. 대표 group 조회만 제공합니다.
- 같은 계정의 중복 실행은 막지만 다른 서버·계정의 동시 재할당/throttle 변경은 막을 수 없습니다.
- `1048576`은 1MiB/s 단위 예시이며 운영 권장값이 아닙니다. 운영 환경 부하에 맞게 입력합니다.
- RF3이 이미 적용됐거나 혼합 RF이면 새 계획 생성을 거부합니다. RF를 낮추지 않습니다.
- NAS 실험 성공이 운영 환경의 오류·지연 없는 무중단을 보장하지 않습니다.
- 상세 판단과 수동 대응: [운영 환경 상세 절차](COMPANY-PROCEDURE.md), [RUNBOOK](RUNBOOK.md).

## 소스와 검증

단일 파일은 `scripts/build_company_launcher.py`로 생성합니다. 포함된 소스는
`company.py`, `reassign.py`, `java.sh`, `tests/OffsetsLab.java`이며 변경 전 검토할 수 있습니다.
전체 tests/OffsetsLab.java 소스를 포함하지만 실행기는 plan/snapshot/check만 호출하고 workload/lab는 실행하지 않습니다.

```bash
python3 scripts/build_company_launcher.py
bash -n offsets-rf3.sh
python3 -m unittest discover -s tests -v
```

실행 흐름 모의 테스트와 별도로 **단일 .sh 전체 실행, 변경 직전 NO, 정상 완료 후 --resume을 NAS 격리 클러스터에서 실제 검증**했습니다.
현재 파일 SHA256·지표·미검증 범위는 [최종 실제 검증](LIVE-VALIDATION-20261006.md)에 있습니다. [이전 단일 파일 검증](STANDALONE-VALIDATION.md)은 당시 파일의 역사적 기록입니다.
진행 중 단절/재개와 TLS/SASL은 이번 실제 실험에서 검증하지 않았습니다. 불명확한 요청 결과의 재실행 방지·해시 변경 차단은 모의 테스트 범위입니다.
기존 재할당 helper의 기록은 [RESULTS](RESULTS.md)에 따로 보존했습니다. 어떤 NAS 실험도 운영 환경 운영 적용 결과나 무중단 보장은 아닙니다.
