# 처음부터 설치하는 Keycloak + Kafka UI 다중 계정·권한 가이드

확인일: 2026-10-15. Linux/Red Hat 직접 설치, Docker 미사용. UI는 **Provectus v0.7.2**, Keycloak 설치 예제는 공식 사이트 확인 버전 **26.8.1 / JDK 25** 기준이다. Kafka 3.9.1 KRaft의 broker 설정은 변경하지 않는다. Keycloak용 JDK와 UI용 JDK 17은 분리한다.

**이 문서는 설치·연동 절차이며 실제 Keycloak 서버를 설치해 로그인 시험까지 통과한 결과가 아니다.** 문서 안 생성 코드·쉘 구문만 별도 검사했다. 특히 기존 서비스에 바로 적용하지 말고 아래 별도 UI로 합격 기준을 확인한다. 설치 버전을 바꾸면 해당 버전 JDK·DB 지원표와 보안 공지를 다시 확인한다.

## 무엇을 설치하고 어디서 계정을 만드는가

- Keycloak: 사용자 이름·암호를 저장하고 로그인 처리. 관리자 콘솔에서 사용자 여러 명 생성.
- Kafka UI: Keycloak 로그인 결과를 받아 사용자별 `rbac.roles` 적용.
- Kafka broker: UI 연결 계정으로 요청을 받음. 웹 사용자별 Kafka ACL을 자동 발급하지 않음.

먼저 같은 Linux 호스트에 Keycloak과 **별도 검증 UI**를 띄운다. 브라우저는 로컬 또는 SSH 터널로 접근한다. 기존 UI는 그대로 유지한다.

| 용도 | 로컬 주소 | 비고 |
|---|---|---|
| Keycloak | `http://localhost:18090` | loopback 전용, 개발 검증용 |
| 별도 Kafka UI | `http://localhost:18081` | 기존 UI와 다른 포트 |
| 기존 Kafka | 직접 입력 | 기존 인증/설정 변경 없음 |

이 로컬 검증 단계는 외부 공개·상시 운영용이 아니다. 운영 전환은 아래 HTTPS·PostgreSQL 절차를 따른다.

## Step 0. 반입 준비와 자원 확인

인터넷 가능 구간에서 승인된 아티팩트를 다운로드하고 해시·서명 및 보안 공지를 확인한 뒤 반입한다. 기존 설치 디렉터리를 덮어쓰지 않는다.

- [Keycloak 26.8.1 ZIP](https://github.com/keycloak/keycloak/releases/tag/26.8.1)
- 승인된 Linux 아키텍처용 JDK 25 배포본(Keycloak 전용)
- 기존 Provectus `kafka-ui-api-v0.7.2.jar`, UI용 JDK 17
- Bash, Python 3, unzip, curl, SSH 클라이언트
- 운영 전환용: PostgreSQL 지원 버전, DNS 2개, SAN이 맞는 TLS 인증서·키·CA, systemd 관리 권한

아래는 기존 설치 계정의 Bash에서 실행한다. 명령을 실행하기 전에 CPU·메모리·디스크 여유를 확인한다. 예산으로 추가 메모리 2~3 GiB 이상을 검토하되 실제 필요량을 보증하지 않는다. 여유가 부족하면 별도 호스트에서 수행한다.

```bash
bash
set -euo pipefail
umask 077
free -m
df -h "$HOME"
read -r -p '반입한 Keycloak ZIP 절대경로: ' KC_ZIP
read -r -p 'Keycloak 전용 JDK25 JAVA_HOME: ' KC_JAVA_HOME
read -r -p 'UI 전용 JDK17 JAVA_HOME: ' UI_JAVA_HOME
read -r -p '기존 Kafka UI v0.7.2 JAR 절대경로: ' UI_JAR
export KC_JAVA_HOME UI_JAVA_HOME UI_JAR
test -r "$KC_ZIP"
test -r "$UI_JAR"
"$KC_JAVA_HOME/bin/java" -version
"$UI_JAVA_HOME/bin/java" -version
sha256sum "$KC_ZIP" "$UI_JAR"
python3 - <<'PY'
import socket
for port in (18090, 18081):
    with socket.socket() as s:
        s.bind(('127.0.0.1', port))
print('검증 포트 사용 가능')
PY
LAB="$(mktemp -d "$HOME/ui-keycloak-lab.XXXXXXXX")"
export LAB
unzip -q "$KC_ZIP" -d "$LAB"
KC_HOME="$LAB/keycloak-26.8.1"
export KC_HOME
test -x "$KC_HOME/bin/kc.sh"
printf '비공개 작업 경로: %s\n' "$LAB"
```

출력된 해시를 승인된 반입 목록과 대조한다. 작업 경로에는 이후 secret·DB·로그가 들어가므로 Git이나 공유 폴더에 넣지 않는다.

## Step 1. Keycloak 검증 인스턴스 시작

같은 터미널에서 초기 관리자 이름·암호를 입력한다. 이 관리자는 Keycloak 설정용이며 Kafka UI 사용자가 아니다.

```bash
read -r -p 'Keycloak 초기 관리자 이름: ' KC_BOOTSTRAP_ADMIN_USERNAME
read -r -s -p 'Keycloak 초기 관리자 암호: ' KC_BOOTSTRAP_ADMIN_PASSWORD
printf '\n'
export KC_BOOTSTRAP_ADMIN_USERNAME KC_BOOTSTRAP_ADMIN_PASSWORD
JAVA_HOME="$KC_JAVA_HOME" JAVA_OPTS_KC_HEAP='-Xms256m -Xmx768m' \
  "$KC_HOME/bin/kc.sh" start-dev \
  --http-host=127.0.0.1 --http-port=18090 \
  --hostname=http://localhost:18090
```

이 명령은 전경에서 실행된다. 이 터미널을 유지하고 새 터미널을 연다. `start-dev`와 내장 개발 DB는 이 검증에서만 사용한다. 외부 인터페이스로 바인딩하지 않는다.

브라우저가 다른 PC에 있으면 **PC 터미널**에서 다음을 실행하고 유지한다. SSH 계정/호스트는 공개 문서에 적지 말고 입력한다.

```bash
read -r -p '테스트 Linux SSH 사용자@호스트: ' SSH_TARGET
read -r -p 'SSH 포트: ' SSH_PORT
ssh -N -o ExitOnForwardFailure=yes -p "$SSH_PORT" \
  -L 127.0.0.1:18090:127.0.0.1:18090 \
  -L 127.0.0.1:18081:127.0.0.1:18081 "$SSH_TARGET"
```

PC에서 두 로컬 포트가 비어 있어야 한다. 브라우저와 Linux UI 프로세스 모두 **동일한 `http://localhost:18090` issuer**를 사용한다. `localhost`와 `127.0.0.1`을 issuer/콜백에 섞지 않는다.

## Step 2. Realm 생성

1. 브라우저에서 `http://localhost:18090/admin/` 접속.
2. Step 1의 초기 관리자 계정으로 로그인.
3. Realm 선택 메뉴 → **Create realm** → Realm name `kafka-ui` → Create.
4. 이후 작업은 `master`가 아닌 **kafka-ui realm**에서 수행.
5. 로컬 HTTP 검증에서 `HTTPS required`가 나타나면 kafka-ui realm의 Realm settings → Login → Require SSL을 로컬 검증 범위에서 확인한다. loopback 외 공개를 위해 SSL 요구를 끄지 않는다.

## Step 3. UI용 Client 등록

Clients → Create client에서 다음과 같이 설정한다. 버전에 따라 화면 명칭·탭 위치가 조금 달라질 수 있다.

| 항목 | 값 |
|---|---|
| Client type | OpenID Connect |
| Client ID | `kafka-ui` |
| Client authentication | On(confidential) |
| Authorization | Off |
| Standard flow | On |
| Implicit flow | Off |
| Direct access grants | Off |
| Service accounts roles | Off |
| Root URL / Home URL | `http://localhost:18081` |
| Valid redirect URIs | `http://localhost:18081/login/oauth2/code/keycloak` |
| Web origins | `http://localhost:18081` |

Save 후 Credentials 탭에서 Client secret을 확인한다. **채팅·스크린샷·Git에 붙이지 말고** 이후 숨김 입력에만 사용한다. callback wildcard `*`를 허용하지 않는다. PKCE가 필수인 정책에서는 이 UI 버전이 실제로 호환되는지 먼저 시험한다.

Client scopes 탭에서 기본 `profile` scope가 배정되어 있는지 확인한다. 이 가이드의 계정 매핑은 `sub`를 쓰므로 roles claim mapper는 필요 없다.

## Step 4. 사용자 3명 생성

Users → Create new user에서 다음 예시처럼 생성한다. 사용자 이름은 원하는 값으로 바꿀 수 있다.

| 사용자 예시 | 목적 | Keycloak 관리자 권한 |
|---|---|---|
| `ui-viewer` | 조회만 | 부여하지 않음 |
| `ui-config-admin` | 토픽 설정 변경 | 부여하지 않음 |
| `ui-no-role` | 거부 시험 | 부여하지 않음 |

각 사용자에 대해 다음 순서로 반복한다.

1. Enabled 상태로 생성하고 필요한 profile 필드를 입력한다.
2. Credentials → Set password → 서로 다른 강한 암호 설정.
3. 로컬 시험에서는 Temporary Off로 두거나 최초 로그인 때 암호를 변경한다. 운영 정책에서는 만료·MFA 정책을 별도로 적용한다.
4. 사용자 상세 화면의 **ID(UUID)**를 비공개로 기록한다. 이것이 OIDC `sub`와 대응하는 값이다. 아래 UI 역할에 표시용 username이 아니라 이 ID를 입력한다.
5. UI 사용자를 Keycloak `realm-management` 관리자나 master 관리자 역할에 배정하지 않는다.

비밀번호는 YAML에 쓰지 않는다. Keycloak이 관리한다. `ui-no-role` 사용자는 UI YAML 역할 목록에 넣지 않는다.

## Step 5. UI 설정 생성

Linux의 **새 터미널**에서 아래를 실행한다. Step 0에 출력한 작업 경로와 JDK/JAR 경로를 다시 입력한다.

```bash
bash
set -euo pipefail
umask 077
read -r -p 'Step 0의 비공개 작업 경로: ' LAB
read -r -p 'UI JDK17 JAVA_HOME: ' UI_JAVA_HOME
read -r -p 'Kafka UI v0.7.2 JAR 절대경로: ' UI_JAR
read -r -p 'Kafka bootstrap(host:port,host:port): ' UI_BOOTSTRAP
read -r -p 'UI에 표시할 cluster 이름: ' UI_CLUSTER
read -r -p '조회 사용자 ID(UUID): ' UI_VIEWER_SUB
read -r -p '토픽 설정 관리자 ID(UUID): ' UI_ADMIN_SUB
read -r -s -p 'Step 3의 Client secret: ' UI_OAUTH_CLIENT_SECRET
printf '\n'
export LAB UI_BOOTSTRAP UI_CLUSTER UI_VIEWER_SUB UI_ADMIN_SUB
export UI_OAUTH_CLIENT_SECRET
test -d "$LAB"
test -x "$UI_JAVA_HOME/bin/java"
test -r "$UI_JAR"
python3 - <<'PY'
import json, os
from pathlib import Path
for key in ('UI_BOOTSTRAP','UI_CLUSTER','UI_VIEWER_SUB','UI_ADMIN_SUB',
            'UI_OAUTH_CLIENT_SECRET'):
    if not os.environ.get(key, '').strip():
        raise SystemExit('빈 입력 금지: '+key)
if os.environ['UI_VIEWER_SUB'] == os.environ['UI_ADMIN_SUB']:
    raise SystemExit('두 사용자 ID가 같음')
roles=[]
for name, key, actions in (
    ('viewer','UI_VIEWER_SUB',['view']),
    ('topic-config-admin','UI_ADMIN_SUB',['view','edit'])):
    roles.append({
        'name':name, 'clusters':[os.environ['UI_CLUSTER']],
        'subjects':[{'provider':'oauth','type':'user',
                     'value':os.environ[key]}],
        'permissions':[
            {'resource':'clusterconfig','actions':['view']},
            {'resource':'topic','value':'.*','actions':actions},
            {'resource':'consumer','value':'.*','actions':['view']}]
    })
config={
    'server':{'address':'127.0.0.1','port':18081},
    'auth':{'type':'OAUTH2','oauth2':{'client':{'keycloak':{
        'provider':'oauth', 'clientId':'kafka-ui',
        'clientSecret':'${UI_OAUTH_CLIENT_SECRET}',
        'clientName':'Keycloak',
        'authorizationGrantType':'authorization_code',
        'redirectUri':'http://localhost:18081/login/oauth2/code/keycloak',
        'scope':['openid','profile'],
        'issuerUri':'http://localhost:18090/realms/kafka-ui',
        'userNameAttribute':'sub',
        'customParams':{'type':'oauth','roles-field':'roles'}
    }}}},
    'kafka':{'clusters':[{'name':os.environ['UI_CLUSTER'],
                          'bootstrapServers':os.environ['UI_BOOTSTRAP']}]},
    'dynamic':{'config':{'enabled':False}},
    'rbac':{'roles':roles}
}
p=Path(os.environ['LAB'])/'ui-keycloak.yml'
if p.exists():
    raise SystemExit('기존 파일 존재: 덮어쓰기 중단')
p.write_text(json.dumps(config, ensure_ascii=False, indent=2)+'\n')
p.chmod(0o600)
print('UI 설정 생성 완료; secret 값은 파일에 저장하지 않음')
PY
```

JSON은 YAML 부분집합이다. `.yml` 파일 안에 JSON 구조로 저장하므로 별도 YAML 패키지가 필요 없다. 이 생성 예제는 **PLAINTEXT Kafka 연결** 기준이다. Kafka가 SASL/TLS라면 기존의 승인된 클라이언트 보안 속성을 별도 추가해야 한다.

`roles-field: roles`는 v0.7.2 일반 OAuth 권한 추출기를 활성화하기 위해 유지한다. 이 가이드에서는 user/sub 직접 매핑을 사용하므로 실제 `roles` claim이 없어도 된다. Keycloak의 기본 중첩 `realm_access.roles`를 자동으로 평면 `roles`로 해석한다고 가정하지 않는다.

`.*`는 모든 토픽/그룹을 뜻한다. 필요한 경우 **실행 전** 비공개 설정에서 해당 역할의 `topic.value`, `consumer.value`를 검토된 정규식으로 제한한다. UI 표시 cluster 이름과 `roles[].clusters`가 같아야 한다.

## Step 6. 별도 UI 시작

Step 5 터미널에서 실행한다. 기존 UI는 재시작하지 않는다.

```bash
"$UI_JAVA_HOME/bin/java" -Xms128m -Xmx512m \
  "-Dspring.config.additional-location=file:$LAB/ui-keycloak.yml" \
  -jar "$UI_JAR" > "$LAB/ui-keycloak.log" 2>&1
```

전경 프로세스다. 별도 브라우저에서 `http://localhost:18081` 접속 → Keycloak 선택 → 해당 사용자로 로그인한다. discovery 실패 시 새 터미널에서 다음을 확인한다.

```bash
curl --fail --silent --show-error \
  http://localhost:18090/realms/kafka-ui/.well-known/openid-configuration \
  | python3 -m json.tool
```

issuer가 입력한 주소와 일치해야 한다. 이 응답에는 client secret이나 사용자 암호가 없다. 원본 UI 로그는 사용자 정보가 들어갈 수 있으므로 비공개로 보존한다.

## Step 7. 계정별 합격 기준

항상 로그아웃하거나 계정별 새 비공개 창을 사용한다. Keycloak SSO 세션이 남아 있으면 이전 사용자가 자동 로그인할 수 있다.

| 시험 | 조회자 | 토픽 설정 관리자 | 역할 없는 사용자 |
|---|---|---|---|
| 토픽·그룹 현황 조회 | 허용 | 허용 | 거부 |
| 토픽 Properties 수정 | 거부 | 허용 범위 내 허용 | 거부 |
| 토픽 생성·삭제 | 거부 | 거부 | 거부 |
| broker 설정 변경 | 거부 | 거부 | 거부 |
| 메시지 읽기·전송 | 거부 | 거부 | 거부 |
| ACL 편집 | 거부 | 거부 | 거부 |

1. 관리자 승인을 받은 전용 테스트 토픽을 **Kafka 관리자**가 미리 생성한다. 이 예제의 UI 설정 관리자는 토픽 생성 권한이 없다.
2. 조회자 계정에서 Properties 조회가 되고 변경이 거부되는지 확인한다.
3. 설정 관리자 계정에서 테스트 토픽의 설정 하나를 변경하고, Kafka CLI의 실제 설정과 대조한 뒤 원래 값으로 돌린다. 운영 토픽으로 시험하지 않는다.
4. 브라우저 Network에서 쓰기 API 응답을 확인한다. 조회자 세션에서 대응하는 요청을 수행해 서버가 거부하는지도 확인한다. 버튼 숨김만으로 판정하지 않는다. 쿠키나 토큰을 Git·보고서에 넣지 않는다.
5. 역할 없는 계정은 Keycloak 인증 자체가 성공할 수 있지만 UI 관리 데이터와 작업 권한이 없어야 한다.
6. Keycloak 암호 오류 시 거부, 로그아웃 및 권한 제거 후 새 로그인 시 회수 여부 확인.

`topic edit`는 토픽 설정 하나만 허용하는 키별 권한이 아니다. 범위를 더 좁히려면 UI 외 별도 승인 작업 계층이 필요하다. broker Properties 편집은 `clusterconfig edit`가 필요하며 이 예제에는 없다. 생성은 `topic create`, 삭제는 `topic delete`, 메시지 조회는 `messages_read`를 필요할 때만 추가한다.

Kafka의 UI 연결 계정 권한이 부족하면 웹 역할이 허용해도 요청은 실패한다. UI 로그인·RBAC가 broker 직접 접속을 막아 주지는 않는다.

## Step 8. 검증 인스턴스 종료와 보존

UI와 Keycloak을 실행한 각각의 터미널에서 **Ctrl+C**로 해당 전경 프로세스만 종료한다. 기존 stack stop/start는 실행하지 않는다. 종료 후 각 셸에서 비밀 환경변수를 해제한다.

```bash
unset UI_OAUTH_CLIENT_SECRET KC_BOOTSTRAP_ADMIN_PASSWORD
```

작업 경로와 Keycloak 개발 DB·로그는 비공개로 보존한다. 여러 프로세스를 한꺼번에 종료하는 명령이나 기존 데이터 삭제는 사용하지 않는다. 다음 기동에는 bootstrap 관리자 환경변수를 계속 넣지 말고 기존 계정으로 관리한다.

## Step 9. 운영용 Keycloak: PostgreSQL + 직접 HTTPS

개발 DB나 `start-dev`를 그대로 운영에 쓰지 않는다. 여기서는 단일 Keycloak + 외부 PostgreSQL + 직접 TLS 방식을 설명한다. HA는 별도 설계 사항이다. 운영 호스트의 승인된 유지보수 절차로 진행하며, 아래 단계는 문서 작성 중 실행하지 않았다.

### 9-1. DB와 DNS·인증서 준비

DB 관리자는 선택 Keycloak 버전이 지원하는 PostgreSQL을 준비한다. 승인된 패키지 저장소나 검증된 오프라인 패키지를 사용한다. 기존 PostgreSQL을 무단 초기화하지 않는다. DB 관리자 psql 세션에서 신규 전용 DB/역할을 만든다.

```sql
CREATE ROLE keycloak_app LOGIN;
\password keycloak_app
CREATE DATABASE keycloak OWNER keycloak_app;
```

`\password`의 비노출 프롬프트로 암호를 설정한다. 이미 이름이 있으면 중단하고 소유권/내용을 확인한다. PostgreSQL `pg_hba.conf`에는 Keycloak 호스트만 SCRAM 인증으로 허용하고 방화벽도 제한한다. 원격 DB는 TLS와 `verify-full`에 필요한 CA·DNS를 준비한다. 초기 로컬 DB부터 운영 원격 DB로 단순 복사해도 된다고 가정하지 않는다.

DNS 예시 `sso.example.invalid`, `ui.example.invalid`을 실제 승인된 이름으로 바꾼다. 브라우저와 UI 호스트에서 둘 다 해석·접속되어야 한다. Keycloak 인증서 SAN은 SSO 호스트명, UI 인증서 SAN은 UI 호스트명과 일치해야 한다. CA를 브라우저와 각 JVM truststore에 배포한다.

### 9-2. 전용 서비스 계정과 설치

아래 관리 명령은 Linux 관리자가 수행한다. **동일 이름/경로가 이미 있다면 중단**하고 설치 계획을 조정한다. 반입한 ZIP 및 인증서는 승인된 파일을 사용한다.

```bash
test ! -e /opt/keycloak
test ! -e /opt/keycloak-26.8.1
if getent passwd keycloak >/dev/null; then echo "기존 계정 존재: 중단"; exit 1; fi
sudo useradd --system --no-create-home --home-dir /opt/keycloak --shell /sbin/nologin keycloak
read -r -p '승인된 Keycloak ZIP 절대경로: ' KC_ZIP
sudo unzip -q "$KC_ZIP" -d /opt
sudo ln -s /opt/keycloak-26.8.1 /opt/keycloak
sudo chown -R keycloak:keycloak /opt/keycloak-26.8.1
sudo install -d -m 750 -o root -g keycloak /etc/keycloak
sudo install -d -m 750 -o keycloak -g keycloak /etc/keycloak/tls
```

관리자는 승인된 TLS certificate chain PEM, private key PEM, DB CA PEM을 `/etc/keycloak/tls/`에 각각 `server.crt`, `server.key`, `db-ca.crt`로 배포한다. 키는 `keycloak`만 읽을 수 있게 `0600`, 인증서는 `0640`으로 제한한다. 개인키 내용을 명령행이나 문서에 출력하지 않는다.

`/etc/keycloak/keycloak.env`를 `sudoedit`로 작성한다. 파일은 root 소유·0600으로 만든 후 편집하며 아래 placeholder를 실제 값으로 바꾼다. 비밀 값에 포함된 문자에 대해 systemd EnvironmentFile quoting 규칙을 따른다. 이 파일을 shell로 source하지 않는다.

```bash
test ! -e /etc/keycloak/keycloak.env
sudo install -m 600 -o root -g root /dev/null /etc/keycloak/keycloak.env
sudoedit /etc/keycloak/keycloak.env
```

```ini
JAVA_HOME=/opt/jdk-keycloak
KC_DB=postgres
KC_DB_URL=jdbc:postgresql://db.example.invalid:5432/keycloak?sslmode=verify-full&sslrootcert=/etc/keycloak/tls/db-ca.crt
KC_DB_USERNAME=keycloak_app
KC_DB_PASSWORD="<DB_PASSWORD>"
KC_HOSTNAME=https://sso.example.invalid:8443
KC_HTTPS_PORT=8443
KC_HTTPS_CERTIFICATE_FILE=/etc/keycloak/tls/server.crt
KC_HTTPS_CERTIFICATE_KEY_FILE=/etc/keycloak/tls/server.key
KC_HTTP_ENABLED=false
KC_CACHE=local
JAVA_OPTS_KC_HEAP=-Xms512m -Xmx1024m
KC_BOOTSTRAP_ADMIN_USERNAME=<INITIAL_ADMIN_NAME>
KC_BOOTSTRAP_ADMIN_PASSWORD="<INITIAL_ADMIN_PASSWORD>"
```

`/opt/jdk-keycloak`에는 사전에 승인된 JDK 25를 설치한다. 단일 노드 예제이므로 `cache=local`이며 HA 구성으로 주장하지 않는다. 방화벽에서는 승인된 사용자/관리 네트워크만 8443 접근을 허용한다. 인터넷에 관리 콘솔을 노출하지 않는다.

### 9-3. systemd 등록

`sudoedit /etc/systemd/system/keycloak.service`로 다음을 저장한다.

```ini
[Unit]
Description=Keycloak identity service
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=keycloak
Group=keycloak
WorkingDirectory=/opt/keycloak
EnvironmentFile=/etc/keycloak/keycloak.env
ExecStart=/opt/keycloak/bin/kc.sh start
Restart=on-failure
RestartSec=10
TimeoutStartSec=300
TimeoutStopSec=90
UMask=0077
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now keycloak.service
sudo systemctl status keycloak.service --no-pager
```

위 명령은 신규 Keycloak 서비스용이며 기존 Kafka/UI 서비스를 재시작하지 않는다.

최초 기동에는 빌드가 포함될 수 있으므로 정상 기동을 확인한다. SSO의 `/admin/`에서 초기 관리자로 로그인한 뒤 정규 관리 계정을 생성하고 MFA를 설정한다. 정규 계정 로그인을 확인한 후 임시 관리자를 폐기하고 env에서 bootstrap 항목을 제외한다. 다음에 계획한 Keycloak 재시작 때 반영한다. 관리자 계정을 모두 없애지 않는다.

DB 정기 백업과 복원 훈련, TLS 만료 감시, 비밀 보호와 패치 정책을 준비한다. DB 장애나 Keycloak 중단 시 신규 로그인이 불가능해지므로 기존 UI 로그인 설정으로 되돌리는 방법도 보관한다.

## Step 10. 운영용 Realm·Client·UI에 반영

1. 운영 DB의 Keycloak에 Step 2~4처럼 realm/client/사용자를 만든다. 검증 DB와 사용자 ID 및 secret이 다르므로 다시 확인한다.
2. client callback은 `https://ui.example.invalid:18443/login/oauth2/code/keycloak` 하나로 제한하고 Root/Home/Web origins도 해당 origin으로 바꾼다.
3. 기존 UI의 Kafka 연결 설정을 비공개로 백업한다. 검증 파일로 기존 전체 설정을 덮어쓰지 않는다.
4. UI의 `auth.type`을 OAUTH2로 바꾸고 `spring.security.user`를 제외한다. 검증 auth/rbac 블록의 issuer는 `https://sso.example.invalid:8443/realms/kafka-ui`, callback은 위 URL, subject는 운영 사용자 ID로 교체한다. `scope: [openid, profile]`은 유지한다.
5. UI의 직접 HTTPS 설정 예시는 아래와 같다. 기존 TLS reverse proxy를 사용하는 경우 그 HTTPS origin/forwarded 설정에 맞게 구성하며 직접 TLS 예시와 혼용하지 않는다.

```yaml
server:
  address: "<APPROVED_UI_BIND_ADDRESS>"
  port: 18443
  ssl:
    enabled: true
    key-store: "file:/etc/kafka-ui/tls/ui.p12"
    key-store-type: PKCS12
    key-store-password: "${UI_TLS_KEYSTORE_PASSWORD}"
```

PKCS12는 승인된 인증서·키에서 생성하고 암호는 프롬프트로 입력한다. 아래 파일은 사전 배포한 인증서·키다.

```bash
umask 077
openssl pkcs12 -export -name kafka-ui \
  -in ui-server.crt -inkey ui-server.key -certfile ca-chain.crt \
  -out ui.p12
```

UI 실행 계정만 읽을 수 있는 `/etc/kafka-ui/tls/ui.p12`에 배포한다. UI JVM이 Keycloak CA를 신뢰하지 않으면 전용 truststore에 CA를 import한다. 공용 JDK cacerts를 무단 수정하거나 HTTPS hostname verification을 끄지 않는다. 아래에 별도 truststore 전달 예시를 제공한다.

UI가 SSO CA를 신뢰하지 않으면 **UI만 쓰는 전용 JDK17**의 truststore에 승인된 CA를 넣는다. 공용 JDK나 Kafka용 JDK를 변경하지 않는다. 아래 `UI_JAVA_HOME`이 전용 복사본인지 관리자가 확인한 후 실행한다. 배포본이 cacerts를 시스템 경로에 symlink한 경우 대상까지 확인하고 공용 파일이면 중단한다.

```bash
readlink -f "$UI_JAVA_HOME/lib/security/cacerts"
# 관리자가 전용 파일임을 확인한 뒤 승인된 위치에 원본을 백업한다.
"$UI_JAVA_HOME/bin/keytool" -importcert -alias approved-sso-ca \
  -file sso-ca.crt -keystore "$UI_JAVA_HOME/lib/security/cacerts"
```

스토어 암호는 프롬프트 입력한다. fingerprint를 승인 목록과 비교한다. UI를 해당 JDK로 재기동해 HTTPS discovery가 성공하는지 확인한다. CA 만료·교체도 동일한 관리 절차로 수행한다.

6. UI가 systemd 관리라면 기존 unit에 EnvironmentFile 참조를 추가한다. 신규 서비스 예시는 아래와 같다. `/opt/jdk-ui`는 JDK17이며 경로는 실제 배치에 맞춘다. 기존 stack 감시와 중복 기동하지 않는다.

```ini
[Unit]
Description=Kafka UI with Keycloak login
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=kafkaui
Group=kafkaui
EnvironmentFile=/etc/kafka-ui/ui.env
ExecStart=/opt/jdk-ui/bin/java -Xms128m -Xmx512m -Dspring.config.additional-location=file:/etc/kafka-ui/ui.yml -jar /opt/kafka-ui/kafka-ui-api-v0.7.2.jar
Restart=on-failure
UMask=0077
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

위 내용은 신규 서비스 예시이며 기존 UI에 무조건 적용하는 명령이 아니다. 관리자가 `kafkaui` 서비스 계정, JAR 및 설정 디렉터리를 준비하고 기존 관리 방식에서의 전환을 계획한다.

신규 UI 전용 호스트에서는 아래처럼 계정·파일을 준비할 수 있다. 기존 계정/디렉터리가 있으면 중단하고 승인된 배치 경로를 사용한다. Step 5 설정에서 운영 주소/ID를 바꾼 파일을 반입하고 승인된 JAR를 배치한다.

```bash
if getent passwd kafkaui >/dev/null; then echo '기존 계정: 중단'; exit 1; fi
test ! -e /opt/kafka-ui
test ! -e /etc/kafka-ui
sudo useradd --system --no-create-home --shell /sbin/nologin kafkaui
sudo install -d -m 750 -o root -g kafkaui /opt/kafka-ui /etc/kafka-ui
sudo install -d -m 750 -o root -g kafkaui /etc/kafka-ui/tls
read -r -p '승인된 UI JAR 절대경로: ' UI_JAR_SOURCE
read -r -p '검토한 운영 UI 설정 절대경로: ' UI_CONFIG_SOURCE
sudo install -m 640 -o root -g kafkaui "$UI_JAR_SOURCE" /opt/kafka-ui/kafka-ui-api-v0.7.2.jar
sudo install -m 640 -o root -g kafkaui "$UI_CONFIG_SOURCE" /etc/kafka-ui/ui.yml
sudo install -m 640 -o root -g kafkaui ui.p12 /etc/kafka-ui/tls/ui.p12
sudo install -m 600 -o root -g root /dev/null /etc/kafka-ui/ui.env
sudoedit /etc/kafka-ui/ui.env
```

`/etc/kafka-ui/ui.env`는 root 소유 0600으로 관리한다. systemd 이외 방식도 동등하게 제한된 비밀 전달 방식을 사용한다.

```ini
UI_OAUTH_CLIENT_SECRET="<PRODUCTION_CLIENT_SECRET>"
UI_TLS_KEYSTORE_PASSWORD="<UI_PKCS12_PASSWORD>"
```

신규 unit은 `/etc/systemd/system/kafka-ui.service`로 저장한다. 신규 설치이며 기존 감시와 충돌하지 않는 것을 확인한 후에만 시작한다.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now kafka-ui.service
sudo systemctl status kafka-ui.service --no-pager
```

7. 계획한 시점에 UI만 재시작하고 Step 7의 허용/거부를 재검증한다. Kafka broker와 producer/consumer는 재시작하지 않는다. UI 세션 재로그인은 발생할 수 있다.
8. 로그인 실패 시 백업한 UI 설정 및 시작 환경을 복원하고 UI만 재시작한다. 인증을 끄고 공개하는 방법으로 복구하지 않는다.

Keycloak에 사용자만 추가하면 UI 권한은 생기지 않는다. 사용자 ID를 UI YAML의 적절한 role에 추가하고 설정 반영 후 새 세션에서 확인한다. 빈번한 계정 변경에 대응하려면 Keycloak 역할을 평면 claim으로 매핑하는 방식을 별도로 설계·검증한다.

## 문제 발생 시 확인표

| 症状 | 確認点 |
|---|---|
| 로그인 화면 없음 | 읽은 파일, auth.type, 기존 cookie, UI 재시작 |
| invalid redirect_uri | Keycloak 등록 URL과 UI redirectUri의 완전 일치, port, scheme |
| invalid_client | client authentication On, client ID와 secret, 환경변수 전달 |
| discovery/issuer 오류 | UI에서 SSO 접근성, DNS, issuer 일치, CA, 시각 동기화 |
| 로그인 성공, 데이터 없음 | sub UUID, cluster 이름, provider oauth, roles-field 설정 |
| 조회자도 변경 가능 | 다른 역할 중복, 기존 세션, 서버 API 거부 확인 |
| 설정 관리자 변경 실패 | topic edit 매핑, topic 정규식, Kafka 연결 계정 ACL |
| OIDC userinfo 오류 | openid scope, profile, client scope, Keycloak 및 UI 로그 |

## 검증 범위와 공식 출처

문서 검사 결과: Bash 코드 블록 구문 검사, Python 코드 구문 검사 및 합성 입력으로 UI 설정 생성 실행, 파일 권한 0600, client secret 비저장, 조회자/관리자 권한 분리, 기존 파일 덮어쓰기 거부를 확인했다. 실제 IdP와 연결하지 않은 검사다.

문서 작성 시 기존 NAS의 Kafka/UI/Keycloak 설정·서비스를 변경하지 않았다. 실제 설치/인증/REST 권한 차단/HTTPS/DB/systemd 시험은 **미수행**이다. 사용자 환경에서 Step 7을 통과하기 전 운영 반영하지 않는다.

- [Keycloak ZIP 시작 가이드](https://www.keycloak.org/getting-started/getting-started-zip): 배포본/JDK 확인, 초기 설치 흐름.
- [Keycloak 설정 목록](https://www.keycloak.org/server/all-config): bootstrap, HTTP/HTTPS, DB 옵션.
- [Keycloak hostname](https://www.keycloak.org/server/hostname), [TLS](https://www.keycloak.org/server/enabletls), [DB](https://www.keycloak.org/server/db).
- [Keycloak 관리자 문서](https://www.keycloak.org/docs/latest/server_admin/index.html): realm/client/user 관리.
- [UI v0.7.2 OAuth 설정](https://github.com/provectus/kafka-ui/blob/v0.7.2/kafka-ui-api/src/main/java/com/provectus/kafka/ui/config/auth/OAuthProperties.java), [OIDC 처리](https://github.com/provectus/kafka-ui/blob/v0.7.2/kafka-ui-api/src/main/java/com/provectus/kafka/ui/config/auth/OAuthSecurityConfig.java), [user/role 추출기](https://github.com/provectus/kafka-ui/blob/v0.7.2/kafka-ui-api/src/main/java/com/provectus/kafka/ui/service/rbac/extractor/OauthAuthorityExtractor.java).
- [Spring DefaultOidcUser](https://docs.spring.io/spring-security/site/docs/current/api/org/springframework/security/oauth2/core/oidc/user/DefaultOidcUser.html): DefaultOAuth2User 상속. 기존 README의 openid 일괄 금지 설명을 정정한 근거. 실제 연동 성공 증거를 뜻하지는 않는다.

- [Spring Security 6.1.0 DefaultOidcUser 소스](https://github.com/spring-projects/spring-security/blob/6.1.0/oauth2/oauth2-core/src/main/java/org/springframework/security/oauth2/core/oidc/user/DefaultOidcUser.java): 6.x에서도 동일한 상속 구조 확인.
