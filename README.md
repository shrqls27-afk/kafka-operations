# Kafka 운영 검증

Linux 직접 설치 Kafka 운영 절차와 격리된 실험 기록입니다.

- [기존 전체 검증의 Kafka3.9.1 KRaft 기준 감사](docs/KAFKA-3.9.1-KRAFT-AUDIT.md)
- [기존 PLAINTEXT 앱 유지·보안 포트 준비](kafka-security-scram-acl/docs/KEEP-PLAINTEXT.md)
- **[운영 환경 실행: .sh 하나로 진행](consumer-offsets-rf1-to-rf3/docs/ONE-SCRIPT.md)**
- [운영 서버 단계별 실행 절차](consumer-offsets-rf1-to-rf3/docs/COMPANY-PROCEDURE.md)
- [상세 운영 판단](consumer-offsets-rf1-to-rf3/docs/RUNBOOK.md)
- [RF=1 → RF=3 실험](consumer-offsets-rf1-to-rf3/README.md)

각 주제는 docs/(절차), scripts/(실행 도구), tests/(검증 코드), evidence/(공유 가능한 요약) 구조를 사용합니다. 바이너리, 인증, 실행 데이터와 원본 로그는 업로드하지 않습니다.

주제 공통 버전/모드 감사는 루트 docs/, evidence/, scripts/audit-kraft-evidence.py에 있으며 보존된 비공개 runtime을 읽어 익명화된 근거를 생성합니다.
