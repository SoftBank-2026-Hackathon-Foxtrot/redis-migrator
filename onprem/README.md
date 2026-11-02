# 온프레미스 Valkey

Docker만 있는 Linux 서버에 Valkey 8.0을 띄운다. 이전 도구 쪽은 바뀌지 않고, 엔드포인트가 하나 늘 뿐이다.

## 실행

```bash
cp .env.example .env    # 비밀번호와 VPN 주소를 채운다
docker compose up -d
```

## 재시작해도 남는 것

| 항목 | 위치 |
|---|---|
| 데이터 | AOF (`appendfsync everysec`), 볼륨 `valkey-data` |
| keyspace 알림 | `valkey.conf` |
| 앱 사용자 쓰기 차단 | `/data/users.acl`. 제어부가 권한을 바꿀 때마다 `ACL SAVE`로 저장 |
| 메모리 제한 | `VALKEY_MAXMEMORY`. 축출은 하지 않는다(`noeviction`) |

`everysec`이라 전원이 갑자기 나가면 최근 약 1초의 쓰기는 잃을 수 있다.

비밀번호는 `users.acl`이 없을 때, 즉 첫 부팅에만 읽는다. 나중에 `.env`를 바꿔도 반영되지 않으니 `ACL SETUSER`로 바꾼다.

## 이전 도구에 연결

프록시에는 앱 사용자, Sync Worker와 statemig에는 관리자 계정을 준다. `<host>`는 VPN 주소다.

```
# proxy
STATEMIG_ENDPOINTS=...,onprem=redis://app:<앱 비밀번호>@<host>:6379
# syncworker, statemig
STATEMIG_ENDPOINTS=...,onprem=redis://default:<관리자 비밀번호>@<host>:6379
```

그다음 `POST /migrations {"target":"onprem"}`로 시작한다. 사전 확인의 `warnings`에 재시작 위험이 나오지 않아야 한다.

## 검증

`python demo/onprem_check.py`는 로컬에서 부하를 건 채 다음을 확인한다. VPN 구간과 실제 관리형 Redis는 포함하지 않는다.

- aws → onprem 컷오버와 정상 롤백 (앱 비밀번호가 다른 경우)
- 차단된 상태와 활성 상태에서 각각 강제 종료 후 데이터, 차단, 알림, 역방향 동기화 유지
