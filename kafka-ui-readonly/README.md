# Kafka UI v0.7.2 읽기 전용 보완 빌드

**목적:** 단일 `LOGIN_FORM` 계정을 유지하면서 Broker → Configs 변경을 포함한 관리 쓰기 요청을 차단한다. OAuth2나 다중 계정 기능을 추가하는 수정은 아니다.

공식 Provectus v0.7.2를 기반으로 한 **로컬 수정 빌드 `readonly.1`**다. 공식 배포본이나 최신 보안 패치를 모두 반영한 제품으로 표현하지 않는다. 기존 UI, Kafka broker, producer/consumer는 이 작업에서 교체·재시작하지 않는다.

## 설정과 사용

수정 JAR에만 아래 새 옵션이 적용된다. **공식 원본 JAR에 추가하면 차단 기능이 생기지 않는다.**

```yaml
ui:
  read-only: true

kafka:
  clusters:
    - name: example-cluster
      bootstrapServers: "broker-a.example.invalid:9092,broker-b.example.invalid:9092"
      readOnly: true

dynamic:
  config:
    enabled: false
```

기존 `auth.type: LOGIN_FORM` 및 `spring.security.user` 설정은 유지한다. 위 예제로 기존 인증·Kafka TLS/SASL 설정을 통째로 덮어쓰지 않는다. 전역 `ui.read-only: true`를 권장한다. 클러스터별 `readOnly`만 사용하면 다른 클러스터나 전역 설정 API까지 차단하는 것은 아니다.

1. [Release](https://github.com/shrqls27-afk/kafka-operations/releases/tag/kafka-ui-v0.7.2-readonly.1)의 JAR와 SHA256SUMS를 별도 디렉터리에 다운로드한다.
2. `sha256sum -c SHA256SUMS`로 대조한다. 기존 JAR를 덮어쓰지 않는다.
3. 비공개 설정을 복사하고 위 옵션을 추가한다. 별도 UI는 `server.address: 127.0.0.1`, 기존과 다른 포트로 실행한다. 설정 파일 권한은 0600으로 제한한다.
4. 기존 UI가 사용 중인 Java 17 환경에서 검증 JAR를 실행한다. 다음 경로는 설치 예시이며 실제 배치 경로에 맞춘다.

```bash
"$JAVA_HOME/bin/java" -Xms128m -Xmx512m \
  "-Dspring.config.additional-location=file:$HOME/ui-readonly-check/ui.yml" \
  -jar "$HOME/ui-readonly-check/kafka-ui-api-v0.7.2-readonly.1.jar"
```

5. 로그인 후 Broker → Configs의 Edit가 비활성화되는지 확인한다. 조회·메시지 열람은 기본적으로 가능하다. 읽기 전용은 데이터 열람 권한을 없애는 옵션이 아니다.
6. 권한 정책 조회 `GET /api/readonly-policy`가 `{"readOnly":true}`인지 확인한다. 로그인 세션은 필요하다. 브라우저 Network에서 변경 요청이 **403 / UI_READ_ONLY**인지 확인한다. 운영 broker 설정을 바꾸는 테스트는 하지 않는다.
7. 검증 완료 후 승인된 시점에 UI만 JAR/설정 경로를 전환한다. 기존 stack 통합 stop/start는 사용하지 않는다. UI 세션 단절·재로그인이 가능하며 Kafka 프로세스 변경은 필요 없다.
8. 문제 시 원래 JAR/설정 경로로 되돌려 UI만 기동한다. 원본 JAR는 같은 전체 읽기 전용 보호를 제공하지 않으므로 공개 접근을 제한한 상태에서 복구한다.

## 차단 방식과 영향

- 서버 WebFilter가 controller 실행 전에 `/api/**`의 **GET/HEAD/OPTIONS 외 메서드**를 거부한다. 전역 옵션은 토픽·broker 설정·RF/파티션·offset reset·ACL·Connect·Schema·KSQL·UI 설정 변경 API에 적용된다.
- 기존 `kafka.clusters[].readOnly: true`도 해당 클러스터 경로에 대해 서버가 검사한다.
- 이 정책은 보수적이다. 캐시 갱신, 스키마 호환성 검사, connector 설정 검증, smart filter 시험, 토픽 분석 시작/취소처럼 비파괴적인 POST/PUT 기능도 차단된다. KSQL 실행도 차단한다. 메시지 조회 GET는 차단하지 않는다.
- 로그인 POST `/auth`는 관리 API가 아니므로 유지한다. Spring Security 뒤에서 동작하며 기존 인증을 우회하지 않는다.
- 프런트엔드의 권한 hook은 정책을 조회해 broker Edit와 생성/변경 controls를 제한한다. 정책 조회 실패/로딩 중에는 변경 control을 비활성화한다. 화면 제어가 누락되더라도 서버 차단이 최종 경계다.
- 설정 반영은 UI 재기동이 필요하다. 이미 진행 중인 이전 UI의 작업을 취소하는 기능은 아니다.
- Kafka 직접 접속, 별도 UI 인스턴스, 파일 접근 권한에는 영향을 주지 않는다. broker 수준 보호는 Kafka 인증·최소권한 ACL로 별도 구성한다.
- API 계약을 조사한 버전은 v0.7.2다. 미래 버전의 GET 변경 API나 신규 경로까지 보호한다고 약속하지 않는다. `/api` 외 신규 관리 API를 추가하면 검토해야 한다.

## 소스와 빌드 구조

- `src/`: 새 WebFilter 및 수정 frontend hook 원문.
- `upstream-v0.7.2.patch`: 원본 태그 대비 소스 패치(0줄 context, 적용 시 `git apply --unidiff-zero`).
- `scripts/build.py`: 공식 원본 JAR 해시 확인 → 새 Java 클래스 컴파일 → frontend 생성/빌드 → 별도 JAR 조립. 원본 backend 라이브러리/클래스는 그대로 유지한다.
- `tests/`: API 계약 경로 목록, 서버 필터 검증, React hook 검증, 별도 HTTP 프로세스 검증.
- `evidence/`: 익명화한 실제 검사 결과 및 최종 JAR 해시.
- `runtime/`: 반입본·빌드 도구·전체 로그·비밀 설정. Git 제외.

백엔드 전체 Maven 재빌드 방식이 아니라 **해시가 고정된 공식 JAR + 새 필터 클래스 + 전체 재빌드한 frontend** 조립 방식이다. 원본 Kafka client는 공식 JAR에 포함된 버전을 유지하며 Kafka 3.9.1로 라이브러리를 임의 교체하지 않는다. 빌드 절차 재현을 제공하며, ZIP timestamp/도구 환경까지 동일한 bit-for-bit 재현은 보장하지 않는다.

### 빌드 준비

JDK17, Node18 계열, pnpm8.6.12, Python3, OpenAPI generator6.6.0, 공식 v0.7.2 소스와 JAR가 필요하다. 인터넷 구간에서 검증해 반입하고 인트라넷에는 의존성 저장소도 준비한다. 버전 고정은 최신 보안성을 보증하지 않는다.

```bash
# 별도 작업 폴더에서 실행. 기존 설치 경로에는 압축 해제하지 않는다.
curl --fail --location -o upstream-v0.7.2.tar.gz \
  https://github.com/provectus/kafka-ui/archive/refs/tags/v0.7.2.tar.gz
curl --fail --location -o kafka-ui-api-v0.7.2.jar \
  https://github.com/provectus/kafka-ui/releases/download/v0.7.2/kafka-ui-api-v0.7.2.jar
curl --fail --location -o openapi-generator-6.6.0.jar \
  https://repo.maven.apache.org/maven2/org/openapitools/openapi-generator-cli/6.6.0/openapi-generator-cli-6.6.0.jar
sha256sum upstream-v0.7.2.tar.gz kafka-ui-api-v0.7.2.jar openapi-generator-6.6.0.jar
# 아래 명령은 새 경로에서만 수행한다.
tar -xzf upstream-v0.7.2.tar.gz
npm install --prefix tools pnpm@8.6.12 --no-audit --no-fund
```

공식 원본 JAR 허용 해시:

```text
d5e4c2819cb8c819b0baf9261ce22c63b610bb1d9e5f52ee51424c725e6b2cb3
```

저장소의 `kafka-ui-readonly/`에서 경로를 실제 준비 위치로 바꿔 실행한다. 매번 **새 output-dir**를 사용한다.

```bash
python3 scripts/build.py \
  --upstream /path/to/kafka-ui-0.7.2 \
  --base-jar /path/to/kafka-ui-api-v0.7.2.jar \
  --java-home /path/to/jdk17 \
  --pnpm /path/to/tools/node_modules/.bin/pnpm \
  --generator /path/to/openapi-generator-6.6.0.jar \
  --output-dir /path/to/new-build-output
```

설치된 pnpm 의존성 저장소를 반입한 환경에서는 `--offline`을 추가할 수 있다. generator/JDK/원본 파일은 모두 로컬 경로이며 빌드 코드가 외부에서 몰래 다운로드하지 않는다. 패키지 설치는 frozen lockfile을 사용한다. frontend 및 backend build를 함께 배포해야 한다.

### 서버 필터 검사

Spring-test6.0.11 JAR를 별도 반입한 뒤 위 빌드 결과의 compile/classes를 사용한다.

```bash
javac --release 17 \
  -cp '/path/to/output/compile/*:/path/to/output/compile/BOOT-INF/classes:/path/to/output/classes:/path/to/spring-test-6.0.11.jar' \
  -d /path/to/output/classes tests/ReadOnlyGuardTest.java
java -cp '/path/to/output/compile/*:/path/to/output/compile/BOOT-INF/classes:/path/to/output/classes:/path/to/spring-test-6.0.11.jar' \
  ReadOnlyGuardTest tests/api-routes.txt
```

테스트 클래스를 배포 JAR에 추가하지 않는다. HTTP 검증은 새 비공개 작업 경로와 loopback 포트를 사용한다. 실제 Kafka 연결 없이 빈 cluster 설정으로 수행한다.

```bash
python3 tests/http_smoke.py \
  --jar /path/to/kafka-ui-api-v0.7.2-readonly.1.jar \
  --java-home /path/to/jdk17 \
  --work /path/to/new-private-http-test
```

## 검증 범위와 라이선스

실제 결과는 [evidence/results.json](evidence/results.json)에 기록한다. 실제 Kafka broker의 설정 변경이나 현재 NAS UI 교체는 수행하지 않는다. UI 권한 거부를 검증하기 위해 운영 데이터나 설정을 수정하지 않는다.

원본은 [Apache-2.0 LICENSE](LICENSE)를 사용한다. 원본 저작권/라이브러리 고지를 유지하고 새 [NOTICE](NOTICE)로 로컬 변경을 표시한다. 원본 소스와 패치 링크를 제공하며 JAR에도 새 클래스·원본 라이브러리와 고지를 보존한다. 상표 사용이나 모든 의존성의 법적 적합성을 일괄 보증하는 것은 아니며 배포자는 포함 라이선스를 확인해야 한다.

출처(2026-10-15 확인): [원본 태그](https://github.com/provectus/kafka-ui/tree/v0.7.2), [원본 Release](https://github.com/provectus/kafka-ui/releases/tag/v0.7.2), [broker 변경 controller](https://github.com/provectus/kafka-ui/blob/v0.7.2/kafka-ui-api/src/main/java/com/provectus/kafka/ui/controller/BrokersController.java), [RBAC 서비스](https://github.com/provectus/kafka-ui/blob/v0.7.2/kafka-ui-api/src/main/java/com/provectus/kafka/ui/service/rbac/AccessControlService.java).
