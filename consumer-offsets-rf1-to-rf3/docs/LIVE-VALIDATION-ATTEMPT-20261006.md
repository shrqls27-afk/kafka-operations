# RF 변경 중 영향 재검증 시도

2026-10-06 NAS Linux·Kafka3.9.1 KRaft에서 재시도했습니다. **이번에는 RF 변경 전 준비 단계에서 실패하여 producer/consumer의 변경 중 영향을 측정하지 못했습니다.** 성공이나 오류·지연0으로 보고하지 않습니다.

## 실제 확인한 것

- 초기 가용 메모리 약5.5GiB·디스크 약2.2TB, 기존 Kafka/UI/Codex/Telegram PID를 식별했습니다.
- 기존 클러스터 설정에는 offsets RF3이 지정돼 있었습니다. 최초 loopback 주소는 설정된 listener 밖이었고, 실제 설정된 broker 주소로도60초 조회 timeout이 발생했습니다. 현재 전체 파티션 RF/ISR·quorum 정상 여부는 확정하지 못했습니다. 기존 RF를 낮추지 않았습니다.
- 새 전용 data/config/HOME와 loopback44001~44006 포트에3노드를 준비했습니다. format3개 종료코드0, 복사한 offsets-rf3.sh는 현재 배포본과 같은 SHA256입니다.
- 테스트 broker의 controller quorum 등록 timeout과 startup shutdown을 관측했습니다. 테스트 토픽 생성CLI도 timeout(종료코드1)으로 끝났으며, workload와 offsets RF 변경은 시작하지 못했습니다.
- 정리 중 상시 Kafka3개의 PID 변경을 발견했습니다. UI/Codex/Telegram PID는 유지됐습니다. 테스트 코드가 기존 서비스를 stop/restart하지 않았지만 원인은 확정하지 못했고, 전체 기존 서비스 동일PID 유지 검증은 실패했습니다.
- 소유 PID·명령을 확인해 테스트 프로세스에만 SIGTERM을 보냈습니다. 테스트 broker3개 종료코드1/1/143, 테스트6포트 폐쇄를 확인했습니다. 데이터·원본 로그를 보존했으며 삭제·광범위kill·기존 서비스 재시작을 하지 않았습니다.

## 해석과 보류

초기화 단계 간격이 약1분이었고, 동일NAS의3broker 기동·quorum 형성이 등록 제한시간 안에 안정되지 못했습니다. CPU 부하·기동 순서·quorum timing은 검토 대상이지만 근본 원인을 확정한 결과는 아닙니다. format 후 노드를 순차 시작하는 실험 코드의 구성도 점검이 필요합니다. 상시 클러스터 접속 불조는 추가 테스트 시작 전에 관측했습니다. 추가 테스트 부하가 상시 서비스의 PID 변경에 미친 영향은 미확정입니다.

상시 서비스 안정성을 확인하지 못해 추가 부하·자동 재시작·설정 변경을 이어가지 않았습니다. 이번 클러스터 준비 실패에서 RF 변경 실행기까지 도달하지 않았으므로 실행기 재할당 실패로 집계하지 않습니다. 이 시도로 오류/지연 없는 변경을 추가 검증했다고도 주장하지 않습니다.

현재 대상 클러스터의 broker 접속·quorum·전체ISR를 독립 확인하고, 상시 서비스 안정과CPU/메모리/네트워크 여유를 확보한 뒤 재시험해야 합니다. 기존RF3을RF1로 낮추는 시험은 하지 않습니다. 운영 물리3노드·실제 client 옵션은 이NAS와 다르며 오류·지연0을 보장할 수 없습니다.

[익명화한 이번 결과](../evidence/standalone-20261006-attempt/summary.json). 원본은Git 제외 runtime/standalone-20261006-01/에 보존했습니다. 이전 성공한[단일 실행기 검증](STANDALONE-VALIDATION.md)과 별도 시도입니다. 이번commit/push는 하지 않았습니다.
