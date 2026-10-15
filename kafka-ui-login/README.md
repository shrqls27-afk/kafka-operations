# Kafka UI 로그인 설정 가이드

현재 실행 구성 확인일: 2026-10-15. 대상: **Provectus UI for Apache Kafka v0.7.2**, Linux 직접 설치(JAR 실행).

현재 로그인 화면은 외부 IdP나 Kafka SCRAM 로그인이 아니라, `auth.type: LOGIN_FORM`과 Spring Security 로컬 계정 설정으로 활성화되어 있다. 실제 계정·암호·호스트 주소는 이 문서에 포함하지 않는다.

## 현재 적용 구조

- 설정 파일: `~/kafka-stack/config/ui.yml`
- 실행 파일: `~/kafka-stack/kafka-ui/kafka-ui-api-v0.7.2.jar`
- Java 실행 옵션: `-Dspring.config.additional-location=file:…/config/ui.yml`
- UI HTTP 포트: `18080`. 실제 바인딩 주소는 로컬 설정 파일에서 관리한다.
- `dynamic.config.enabled: false`: 동적 설정 기능을 비활성화한 상태다.
- 실행 중인 Java 프로세스의 설정 파일 인자와 설정 키를 읽어서 확인했다. 서비스 재시작이나 설정 변경, 실제 비밀번호 로그인 시험은 수행하지 않았다.

## 설정 예제

다음 값은 공개용 예제다. 기존 설정을 통째로 덮어쓰지 말고 필요한 항목만 비교한다.

```yaml
server:
  address: 127.0.0.1
  port: 18080
spring:
  security:
    user:
      name: "<UI_LOGIN_USER>"
      password: "<STRONG_UNIQUE_PASSWORD>"
auth:
  type: LOGIN_FORM
kafka:
  clusters:
    - name: "example-cluster"
      bootstrapServers: "broker-a.example.invalid:9092,broker-b.example.invalid:9092,broker-c.example.invalid:9092"
dynamic:
  config:
    enabled: false
```

`server.address` 예제는 로컬 접근만 허용한다. 다른 장비에서 접근하려면 승인된 인터페이스 또는 TLS reverse proxy와 방화벽 정책을 별도로 설정한다. 현재 실행 환경의 바인딩 주소를 이 예제로 임의 변경하지 않는다.

1. 실행 계정으로 설정 파일을 백업하고 편집한다. 암호는 명령행·Git·스크린샷에 넣지 않는다.
2. 로컬 계정 이름과 강한 고유 암호를 입력한다. YAML 문자열은 따옴표로 감싸고 특수문자 escaping을 확인한다.
3. 설정 파일 읽기 권한을 실행 계정으로 제한한다.

```sh
chmod 600 "$HOME/kafka-stack/config/ui.yml"
```

4. 신규 설치에서의 JAR 실행 형식은 다음과 같다. **이미 UI가 떠 있으면 중복 실행하지 않는다.** Java 경로는 설치 환경에 맞춘다.

```sh
"$JAVA_HOME/bin/java" -Xms128m -Xmx512m \
  "-Dspring.config.additional-location=file:$HOME/kafka-stack/config/ui.yml" \
  -jar "$HOME/kafka-stack/kafka-ui/kafka-ui-api-v0.7.2.jar"
```

현재 실행 구성도 위 설정 파일 전달 방식을 사용한다. 기존 계정 변경은 승인된 유지보수 시점에 UI 프로세스만 재시작해야 반영된다. 통합 stack stop/start는 Kafka까지 영향을 줄 수 있으므로 계정 변경용 명령으로 사용하지 않는다. UI 재시작 시 웹 세션과 UI 작업은 영향을 받을 수 있다.

## 확인 방법

브라우저의 새 비공개 창에서 UI 주소를 연다. 미인증 상태의 로그인 요구, 잘못된 암호 거부, 올바른 계정 로그인, 로그아웃 후 재인증 요구를 확인한다. v0.7.2 소스의 로그인 경로는 `/auth`, 로그아웃 경로는 `/logout`이다. 일부 공개 경로는 인증 예외이므로 모든 URL이 로그인 화면으로 이동한다고 가정하지 않는다.

로그인이 되지 않으면 실행 인자의 설정 파일 경로, YAML 들여쓰기, `auth.type`, `spring.security.user.name/password`, UI 로그의 인증 모드와 설정 로딩 오류를 확인한다. 로그나 설정 파일 전체를 공개 이슈에 붙이지 않는다.

## 범위와 보안 주의점

- UI 로그인은 **웹 UI 접근 인증**이다. producer/consumer의 broker 직접 접속이나 토픽 생성 권한을 제한하지 않는다.
- Kafka SASL/TLS·ACL은 broker 및 UI의 Kafka 클라이언트 연결 설정에서 따로 구성해야 한다. UI 사용자 이름이 Kafka principal로 자동 전달되는 것은 아니다.
- 단일 로컬 계정 로그인 구성이며 사용자별 역할 분리는 이 설정만으로 제공되지 않는다. 다중 사용자·SSO·RBAC는 별도 인증 및 권한 설계가 필요하다.
- HTTP 로그인에서는 암호와 세션이 네트워크 암호화로 보호되지 않는다. 원격 접근에는 인증서 검증이 가능한 HTTPS 또는 안전한 터널을 사용한다. 이 가이드는 TLS를 이미 적용했다고 주장하지 않는다.
- 로컬 YAML 암호는 비밀 파일로 취급한다. 저장소에는 placeholder 예제만 넣는다.
- v0.7.2의 로그인 동작 확인이지 최신 버전의 보안성 보증이나 업그레이드 검증은 아니다.

## 근거

- 로컬 실행 프로세스 및 설정 키 확인: 2026-10-15, 비밀 값 비공개.
- [Provectus v0.7.2 LOGIN_FORM 구현](https://github.com/provectus/kafka-ui/blob/v0.7.2/kafka-ui-api/src/main/java/com/provectus/kafka/ui/config/auth/BasicAuthSecurityConfig.java): 인증 모드, 로그인·로그아웃 경로와 인증 요구 확인.
- [Kafbat 공식 로컬 로그인 설정 설명](https://ui.docs.kafbat.io/configuration/authentication/for-the-ui/basic-authentication): 설정 키와 단일 로컬 사용자 방식 참고. 후속 프로젝트 문서이므로 실행 버전 근거는 위 v0.7.2 소스를 우선한다.
