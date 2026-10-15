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


## 계정별 권한 분리: OAuth2 + RBAC 구성

이 절은 **v0.7.2 소스에 맞춘 설정 예제**이며 현재 UI에 적용하거나 IdP 연동 시험을 완료한 결과는 아니다. 기존 `LOGIN_FORM` 설정에 로컬 사용자를 여러 명 추가하는 방식이 아니다. 내부 OAuth2 인증 서버가 각 계정을 인증하고 UI가 계정별 역할을 부여한다. LDAP도 대안이지만 아래는 OAuth2 방식 하나로 범위를 한정한다.

### 1. 인증 서버와 연결 정보 준비

내부 IdP에 UI용 confidential client를 등록하고 authorization-code 흐름을 사용한다. 콜백 주소는 `https://kafka-ui.example.invalid/login/oauth2/code/internal`처럼 UI 외부 주소와 정확히 일치시킨다. UI에서 IdP의 authorization/token/userinfo endpoint에 접근할 수 있어야 하며 HTTPS CA 신뢰를 설정한다. TLS 검증을 끄지 않는다.

아래 예제는 **일반 OAuth2 userinfo 흐름**으로, `openid` scope를 넣지 않는다. v0.7.2 일반 OAuth 권한 추출기는 `DefaultOAuth2User`를 가정하므로 OIDC 설정을 그대로 혼용하지 않는다. IdP가 일반 OAuth2 userinfo 흐름을 지원하지 않으면 이 예제를 그대로 적용하지 말고 해당 IdP와 버전의 OIDC/RBAC 연동을 별도 검증한다.

userinfo가 `sub`라는 고유 사용자 식별자와 필요한 scope를 제공하도록 IdP를 설정한다. 실제 scope 및 endpoint는 IdP 관리자에게 확인한다. 역할 claim을 사용할 때는 평면 `roles` 필드가 필요하며 중첩 claim을 자동 해석한다고 가정하지 않는다.

### 2. 인증 설정 교체

기존 `ui.yml`의 `auth` 블록을 아래 구조로 교체하고 `spring.security.user` 로컬 계정 블록은 제거한다. 기존 Kafka 연결 설정은 유지한다. YAML의 동일 키를 중복 선언하지 않는다.

```yaml
auth:
  type: OAUTH2
  oauth2:
    client:
      internal:
        provider: oauth
        clientId: "<IDP_CLIENT_ID>"
        clientSecret: "${UI_OAUTH_CLIENT_SECRET}"
        clientName: "Internal Login"
        authorizationGrantType: authorization_code
        redirectUri: "https://kafka-ui.example.invalid/login/oauth2/code/internal"
        scope: [profile]
        authorizationUri: "https://idp.example.invalid/oauth/authorize"
        tokenUri: "https://idp.example.invalid/oauth/token"
        userInfoUri: "https://idp.example.invalid/oauth/userinfo"
        userNameAttribute: sub
        customParams:
          type: oauth
          roles-field: roles
```

`UI_OAUTH_CLIENT_SECRET`은 UI 프로세스에만 비밀 환경변수로 전달한다. systemd라면 실행 계정 접근만 허용한 별도 EnvironmentFile로 관리하고 Git에는 넣지 않는다. 기존 실행 스크립트를 사용한다면 그 실행 경로에서 변수를 주입해야 한다. 명령행 인자에 secret을 직접 넣지 않는다.

### 3. 역할 설정 추가

같은 `ui.yml`에 아래 `rbac` 블록을 추가한다. `clusters`는 기존 `kafka.clusters[].name`과 정확히 일치해야 한다. `subjects.value`는 IdP의 `sub` 실제 값으로 바꾸며 표시용 이름이나 UI 로컬 계정명을 임의로 사용하지 않는다.

```yaml
rbac:
  roles:
    - name: topic-viewer
      clusters: [example-cluster]
      subjects:
        - provider: oauth
          type: user
          value: "<VIEWER_SUB>"
      permissions:
        - resource: clusterconfig
          actions: [view]
        - resource: topic
          value: ".*"
          actions: [view]
        - resource: consumer
          value: ".*"
          actions: [view]
    - name: topic-config-admin
      clusters: [example-cluster]
      subjects:
        - provider: oauth
          type: user
          value: "<ADMIN_SUB>"
      permissions:
        - resource: clusterconfig
          actions: [view]
        - resource: topic
          value: ".*"
          actions: [view, edit]
        - resource: consumer
          value: ".*"
          actions: [view]
```

이 예제에서 조회자는 Properties를 조회하고, 설정 관리자는 토픽 Properties를 변경할 수 있다. 관리자는 토픽 생성/삭제 권한까지 받지는 않는다. 필요할 때만 topic의 `create`, `delete`를 별도로 추가한다. 메시지 조회는 `messages_read`, 전송은 `messages_produce` 권한이므로 단순 `view`에 포함되지 않는다.

토픽의 `edit`는 특정 설정 키 하나에 대한 권한이 아니라 토픽 편집 권한이다. retention만 변경 가능하도록 키별 제한이 제공된다고 가정하지 않는다. broker 설정 Properties 변경은 별도로 `clusterconfig: [view, edit]` 권한이 필요하다. UI 연결 구성 편집에 관한 `applicationconfig`나 ACL 편집 권한은 위 역할에 부여하지 않는다.

특정 토픽만 허용하려면 `value`를 `^orders-[a-z0-9-]+$` 같은 검토된 정규식으로 제한한다. 일반 OAuth의 역할 claim 매핑은 `provider: oauth`, `type: role`, `value: <IDP_ROLE>` 형태이며 위 계정 직접 매핑 대신 사용할 수 있다. **이 버전에서 일반 OAuth subject의 역할 타입은 `group`이 아니라 `role`**이다. 여러 역할이 겹치면 권한이 합쳐질 수 있으므로 조회 계정에 관리 역할이 중복 매핑되지 않도록 한다.

### 4. 적용 및 검증 순서

1. 원본 설정과 비밀 파일을 비공개 위치에 백업하고 복구 방법을 확보한다.
2. 별도 UI 인스턴스와 테스트 토픽으로 IdP 로그인·권한을 먼저 검증한다. 초기에는 Kafka 연결 계정도 읽기 전용 ACL로 제한하는 편이 안전하다.
3. 조회/설정 관리자/역할 없는 사용자 계정으로 각각 시험한다. 버튼 숨김만 확인하지 말고 해당 REST 요청의 거부도 확인한다.
4. 검증 후 승인된 시점에 **UI만** 설정 반영을 위해 재시작한다. 기존 Kafka와 producer/consumer를 재시작하지 않는다. UI 세션 재로그인이 필요할 수 있다.
5. 실패하면 원래 UI 설정으로 되돌리고 UI만 재시작한다. 인증을 끄는 방식으로 복구하지 않는다.

| 시험 계정 | 조회 | 토픽 Properties 변경 | 생성/삭제·ACL 편집 |
|---|---|---|---|
| 조회자 | 허용 | 거부 | 거부 |
| 설정 관리자 | 허용 | 지정 토픽만 허용 | 거부 |
| 역할 없는 계정 | 관리 리소스 접근 거부 확인 | 거부 | 거부 |
| 미인증 사용자 | 로그인 요구 | 거부 | 거부 |

허용/거부 API의 응답 코드와 변경 전후 실제 Kafka 설정을 대조한다. 허용 시험은 전용 테스트 토픽에서 수행하고 원복한다. 직접 REST 요청도 거부되는지, 다른 클러스터나 토픽에 권한이 확장되지 않는지, 역할 제거 후 새 세션에서 권한이 회수되는지도 확인한다. 이 표는 **필수 검증 기준이며 아직 실측 결과가 아니다.**

### UI 권한과 Kafka ACL의 경계

UI는 설정된 Kafka 연결 계정으로 broker에 요청한다. UI RBAC가 Kafka ACL을 사용자별로 자동 생성하거나 각 웹 사용자의 principal을 broker에 전파하지 않는다. UI 연결 계정에 충분한 broker 권한이 없으면 UI에서 허용해도 변경은 실패한다. 반대로 broker가 무인증 PLAINTEXT로 열려 있으면 UI 제한을 우회한 직접 접속은 UI RBAC로 막을 수 없다.

`dynamic.config.enabled: false`는 동적 연결 설정을 비활성화하는 항목이지 토픽 Properties 변경을 막는 권한 설정이 아니다. 전체 UI를 읽기 전용으로 제한하는 `kafka.clusters[].readOnly: true`도 별도 옵션이며 계정별 RBAC를 대신하지 않는다.

### 버전별 확인 근거

확인일: 2026-10-15. 아래는 모두 실행 중인 제품과 같은 v0.7.2 소스다.

- [OAuth 설정 필드](https://github.com/provectus/kafka-ui/blob/v0.7.2/kafka-ui-api/src/main/java/com/provectus/kafka/ui/config/auth/OAuthProperties.java)
- [OAuth 인증 및 메서드 권한 적용](https://github.com/provectus/kafka-ui/blob/v0.7.2/kafka-ui-api/src/main/java/com/provectus/kafka/ui/config/auth/OAuthSecurityConfig.java)
- [일반 OAuth 사용자·역할 매핑 구현과 principal 제약](https://github.com/provectus/kafka-ui/blob/v0.7.2/kafka-ui-api/src/main/java/com/provectus/kafka/ui/service/rbac/extractor/OauthAuthorityExtractor.java)
- [토픽 권한 이름](https://github.com/provectus/kafka-ui/blob/v0.7.2/kafka-ui-api/src/main/java/com/provectus/kafka/ui/model/rbac/permission/TopicAction.java)
- [공식 RBAC 설정 설명](https://github.com/provectus/kafka-ui-docs/blob/main/configuration/rbac-role-based-access-control/README.md): 가변 문서이므로 위 버전 소스를 우선한다.


## 복사·붙여넣기 실행 순서

아래는 Linux **Bash** 기준이다. 먼저 내부 IdP 관리자가 일반 OAuth2 authorization-code client와 userinfo endpoint를 준비해야 한다. IdP가 없는 상태에서 명령만 실행해 인증 서버가 생성되지는 않는다. 앞 절의 v0.7.2/OIDC 제약도 먼저 확인한다.

이 절에서는 기존 UI를 건드리지 않고 **별도 포트의 검증용 UI**로 먼저 확인한다. Kafka 클러스터 설정은 기존 파일에서 그대로 복사하지 않고 필요한 bootstrap 주소만 입력하므로, SASL/TLS Kafka 연결 환경에서는 별도 클라이언트 설정 추가가 필요하다. 아래 기본 예제는 PLAINTEXT Kafka 연결용이다.

### Step 1. 실행 계정과 준비물 확인

다음 명령은 Bash에서 같은 터미널 세션으로 이어서 실행한다. Python 3, JDK, 기존 v0.7.2 JAR가 필요하다.

```bash
bash
set -euo pipefail
UI_BASE="$HOME/kafka-stack"
UI_JAVA="$UI_BASE/java/bin/java"
UI_JAR="$UI_BASE/kafka-ui/kafka-ui-api-v0.7.2.jar"
test -x "$UI_JAVA"
test -r "$UI_JAR"
command -v python3
"$UI_JAVA" -version
umask 077
RBAC_RUN="$(mktemp -d "$HOME/kafka-ui-rbac-check.XXXXXXXX")"
export RBAC_RUN
printf '비공개 검증 경로: %s\n' "$RBAC_RUN"
```

### Step 2. IdP와 테스트 대상 값 입력

콜백은 검증용 UI에 도달할 수 있는 HTTPS reverse proxy 주소여야 한다. proxy의 upstream은 아래 `127.0.0.1:18081`로 별도 준비한다. 기존 UI proxy를 임의로 바꾸지 않는다. IdP에 등록한 콜백과 한 글자까지 일치해야 한다.

```bash
read -r -p 'Kafka bootstrap 주소(host:port,host:port): ' UI_BOOTSTRAP
read -r -p '표시할 cluster 이름: ' UI_CLUSTER
read -r -p 'IdP client ID: ' UI_CLIENT_ID
read -r -p '검증용 HTTPS 콜백 URL(.../login/oauth2/code/internal): ' UI_CALLBACK
read -r -p 'IdP authorization URL: ' UI_AUTHORIZE
read -r -p 'IdP token URL: ' UI_TOKEN
read -r -p 'IdP userinfo URL: ' UI_USERINFO
read -r -p 'IdP scope(openid 제외, 공백 구분; 예: profile): ' UI_SCOPE
read -r -p '조회 계정의 고유 sub: ' UI_VIEWER_SUB
read -r -p '설정 관리 계정의 고유 sub: ' UI_ADMIN_SUB
read -r -s -p 'IdP client secret(표시 안 됨): ' UI_OAUTH_CLIENT_SECRET
printf '\n'
export UI_BOOTSTRAP UI_CLUSTER UI_CLIENT_ID UI_CALLBACK UI_AUTHORIZE UI_TOKEN
export UI_USERINFO UI_SCOPE UI_VIEWER_SUB UI_ADMIN_SUB UI_OAUTH_CLIENT_SECRET
```

### Step 3. 검증 전용 설정 파일 생성

JSON은 YAML의 부분집합이므로 Spring 설정 로더가 읽을 수 있는 JSON 구조를 `.yml`에 작성한다. 추가 YAML 패키지는 필요 없다. 사용자 입력을 문자열로 안전하게 직렬화하며 secret 자체는 파일에 쓰지 않고 환경변수 참조만 넣는다.

```bash
python3 - <<'PYCODE'
import json, os, socket
from pathlib import Path

def required(key):
    value = os.environ[key].strip()
    if not value:
        raise SystemExit('빈 값 금지: ' + key)
    return value

keys = ['UI_BOOTSTRAP','UI_CLUSTER','UI_CLIENT_ID','UI_CALLBACK',
        'UI_AUTHORIZE','UI_TOKEN','UI_USERINFO','UI_SCOPE',
        'UI_VIEWER_SUB','UI_ADMIN_SUB']
v = {key: required(key) for key in keys}
if not os.environ.get('UI_OAUTH_CLIENT_SECRET'):
    raise SystemExit('빈 client secret 금지')
for key in ['UI_CALLBACK','UI_AUTHORIZE','UI_TOKEN','UI_USERINFO']:
    if not v[key].startswith('https://'):
        raise SystemExit('HTTPS 필요: ' + key)
if not v['UI_CALLBACK'].endswith('/login/oauth2/code/internal'):
    raise SystemExit('콜백 경로 확인 필요')
if 'openid' in v['UI_SCOPE'].split():
    raise SystemExit('이 예제는 일반 OAuth2 전용: openid scope 금지')
if v['UI_VIEWER_SUB'] == v['UI_ADMIN_SUB']:
    raise SystemExit('조회자와 관리자는 서로 다른 sub 필요')
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 18081))
roles = []
for role, subject, actions in [
    ('topic-viewer', v['UI_VIEWER_SUB'], ['view']),
    ('topic-config-admin', v['UI_ADMIN_SUB'], ['view','edit'])
]:
    roles.append({
        'name': role, 'clusters': [v['UI_CLUSTER']],
        'subjects': [{'provider':'oauth','type':'user','value':subject}],
        'permissions': [
            {'resource':'clusterconfig','actions':['view']},
            {'resource':'topic','value':'.*','actions':actions},
            {'resource':'consumer','value':'.*','actions':['view']}
        ]
    })
config = {
    'server': {'address':'127.0.0.1','port':18081},
    'auth': {'type':'OAUTH2','oauth2':{'client':{'internal':{
        'provider':'oauth','clientId':v['UI_CLIENT_ID'],
        'clientSecret':'${UI_OAUTH_CLIENT_SECRET}',
        'clientName':'Internal Login',
        'authorizationGrantType':'authorization_code',
        'redirectUri':v['UI_CALLBACK'],
        'scope':v['UI_SCOPE'].split(),
        'authorizationUri':v['UI_AUTHORIZE'],
        'tokenUri':v['UI_TOKEN'], 'userInfoUri':v['UI_USERINFO'],
        'userNameAttribute':'sub',
        'customParams':{'type':'oauth','roles-field':'roles'}
    }}}},
    'kafka':{'clusters':[{'name':v['UI_CLUSTER'],
                          'bootstrapServers':v['UI_BOOTSTRAP']}]},
    'dynamic':{'config':{'enabled':False}},
    'rbac':{'roles':roles}
}
path = Path(os.environ['RBAC_RUN'])/'ui-rbac.yml'
path.write_text(json.dumps(config, ensure_ascii=False, indent=2)+'\n')
path.chmod(0o600)
print('설정 생성 완료(비밀 값 출력 없음)')
PYCODE
```

기본 토픽 패턴 `.*`는 해당 클러스터의 모든 토픽을 뜻한다. 대상 제한이 필요하면 **실행 전에** 비공개 설정 파일의 `topic.value`와 `consumer.value`를 검토한 정규식으로 바꾼다. `clusterconfig`에는 edit 권한이 없으므로 broker Properties 수정은 허용하지 않는다.

### Step 4. 별도 UI를 전경 실행

NAS 메모리 여유를 먼저 확인한다. 여유가 부족하면 추가 UI를 실행하지 않는다. 아래 프로세스는 전경 실행되므로 종료할 때 해당 터미널에서 Ctrl+C를 사용한다. 기존 stack 관리 명령은 실행하지 않는다.

```bash
free -m
"$UI_JAVA" -Xms128m -Xmx512m \
  "-Dspring.config.additional-location=file:$RBAC_RUN/ui-rbac.yml" \
  -jar "$UI_JAR" > "$RBAC_RUN/ui.log" 2>&1
```

로그는 비공개 검증 경로에 보존한다. 다른 터미널에서 같은 경로를 지정해 필요한 오류만 로컬로 확인한다. secret·token·사용자 정보가 포함될 수 있으므로 원본 로그는 업로드하지 않는다. CA 오류가 발생하면 JVM truststore에 승인된 CA를 배포하고, hostname verification을 끄지 않는다.

### Step 5. 계정별 허용/거부 확인

1. 검증용 HTTPS 주소를 새 비공개 브라우저 창에서 연다.
2. 조회 계정으로 토픽 Properties 조회 성공과 변경 거부를 확인한다.
3. 설정 관리 계정으로 **전용 테스트 토픽만** Properties 변경 후 원복한다. 생성·삭제·ACL 변경은 거부되어야 한다.
4. 역할 없는 계정은 토픽/그룹 관리 API 접근이 거부되어야 한다.
5. 브라우저 개발자 도구의 Network에서 실제 변경 요청의 거부도 확인한다. 버튼만 숨겨진 것은 합격 기준이 아니다.
6. UI 밖의 Kafka CLI 직접 접속 통제는 별도 Kafka ACL 시험으로 확인한다.

이 단계가 실패하면 운영 UI에 적용하지 않는다. 이 문서 작성 시점에는 실제 IdP 연동 시험을 수행하지 않았다.

### Step 6. 운영 반영 계획 및 복구

검증 성공 후 운영 HTTPS 주소의 별도 콜백을 IdP에 등록하고, 운영용 client/secret·설정 파일을 준비한다. 검증 포트 `18081`과 검증 callback을 그대로 운영 UI에 복사하지 않는다. UI 실행 계정과 관리자가 아래 항목을 검토한다.

- 기존 UI 설정을 비공개 경로에 백업했는가?
- 실제 운영 callback, bind 주소, 포트, Kafka 인증/TLS 연결 설정을 보존했는가?
- UI 시작 서비스에서 secret 환경변수와 CA truststore를 전달하는가?
- 감시 프로세스와 충돌 없이 **UI만** 재시작하는 승인된 방법이 있는가?
- 조회자/관리자/역할 없는 사용자 시험 및 UI 로그인 불가 시 원복 담당자가 정해졌는가?

현재 통합 stack 스크립트는 Kafka까지 관리하므로 여기서 일괄 재시작 명령을 제공하지 않는다. 운영 반영은 해당 UI 프로세스 관리자에 맞춰 수행한다. 복구는 백업한 원본 UI 설정 및 시작 환경을 복원한 뒤 UI만 재시작한다. Kafka broker나 producer/consumer 재시작은 이 작업에 포함하지 않는다.

검증용 전경 UI를 Ctrl+C로 종료한 뒤 같은 터미널에서 secret을 해제한다. 데이터와 로그는 삭제하지 않고 비공개 보존한다.

```bash
unset UI_OAUTH_CLIENT_SECRET
```
