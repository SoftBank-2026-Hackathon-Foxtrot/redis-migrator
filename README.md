# redis-migrator

Redis를 AWS, GCP, 온프레미스 사이에서 이전한다. 앱은 프록시 주소만 알고, 전환할 때 쓰기를 잠깐 멈춘 뒤 프록시가 대상 Redis를 바꾼다.

```text
앱 → Redis 프록시 → ElastiCache / Memorystore / 온프레 Valkey
           ↑                     ↑
       statemig  →  Sync Worker (양쪽 동기화)
```

| 구성 요소 | 코드 | 포트 | 역할 |
|---|---|---|---|
| Redis 프록시 | `proxy/cmd/proxy` | RESP `6379`, 관리 `7000` | 앱의 고정 주소. PAUSE, SWITCH, RESUME |
| Sync Worker | `proxy/cmd/syncworker` | 관리 `7100` | keyspace 알림과 스캔으로 복사, 역방향 동기화, 값 비교 |
| statemig | `statemig/` | `8090` | 전환·롤백 단계 실행. 오케스트레이터가 호출 |

`dataplane/`은 프록시와 Sync Worker의 Python 버전이다. 기본은 Go다. 비교는 [PERFORMANCE.md](PERFORMANCE.md)에 있다.

## 공통 API

오케스트레이터가 호출하는 API다. statemig `:8090`에서 제공하며 `Authorization: Bearer <STATEMIG_ADMIN_TOKEN>`이 필요하다.

공통 스펙이 확정되기 전 임시안이다. 경로와 필드는 스펙에 맞춰 바뀔 수 있다.

| 메서드 | 경로 | 본문 | 동작 |
|---|---|---|---|
| POST | `/v1/prepare` | `{"target":"gcp","budget_ms":1800,"mode":"delta"}` | 사전 점검 후 동기화 시작 |
| POST | `/v1/execute` | `{"budget_ms":1800}` | 컷오버 |
| POST | `/v1/rollback` | `{"mode":"normal","budget_ms":1800}` | 원본으로 복귀. 대상이 죽었으면 `"mode":"failure"` |
| POST | `/v1/commit` | 없음 | 역방향 동기화를 끝내고 이전 확정 |
| GET | `/v1/status` | 없음 | 현재 단계와 결과 조회 |

POST는 바로 `202`를 돌려주고 작업은 뒤에서 돈다. 결과는 `/v1/status`로 확인한다.

```json
{
  "resource": "redis",
  "phase": "executed",
  "ready": false,
  "source": "aws",
  "target": "gcp",
  "active": "gcp",
  "operation": {"name": "execute", "state": "succeeded", "error": null, "result": {}},
  "last_cutover": {"paused_ms": 38.9, "total_ms": 42.1},
  "events": []
}
```

| `phase` | 뜻 |
|---|---|
| `idle` | 진행 중인 이전 없음 |
| `preparing` | 사전 점검 또는 동기화 중 |
| `ready` | 컷오버 가능 |
| `executing` | 컷오버 또는 큐 비우기 중 |
| `executed` | 전환 완료. 롤백이나 commit 가능 |
| `rolling_back`, `rolled_back` | 롤백 중, 완료 |
| `committing`, `committed` | 확정 중, 완료 |
| `aborted` | 컷오버 실패로 원본에 복귀 |
| `failed` | 사전 점검 실패 |

| 응답 | 경우 |
|---|---|
| `400` | 없는 대상이거나 이미 활성인 대상 |
| `401` | 토큰 없음 |
| `404` | 준비된 이전 없음 |
| `409` | 다른 단계가 실행 중 |
| `422` | 본문 형식 오류 |

단계 실패는 `operation.state`가 `failed`가 되고 `operation.error`에 이유가 남는다. 정상 롤백은 큐를 다 비운 `executed`부터 가능하다.

호출 순서:

```text
prepare → ready 대기 → execute → executed 대기 → commit
                                              └→ rollback
```

## 내부 API

공통 API 아래에서 쓰는 API다. 모두 같은 토큰이 필요하다.

| 서비스 | 경로 |
|---|---|
| statemig | `POST /migrations`, `GET /migrations/current/ready`, `POST /migrations/current/{cutover,rollback,commit}`, `GET /status` |
| 프록시 | `GET /status`, `POST /pause`, `POST /switch`, `POST /resume`, `POST /drain-done` |
| Sync Worker | `GET /status`, `POST /start`, `POST /precompare`, `POST /reconcile`, `POST /verify`, `POST /reverse`, `POST /stop` |

## 배포 위치

프록시, Sync Worker, statemig는 AWS 관리 VPC에 둔다. 프록시와 statemig는 `control.json`을 공유하므로 같은 호스트에 둔다.

환경 간 연결은 Tailscale로 한다. ElastiCache와 Memorystore에는 Tailscale을 설치할 수 없어서 각 VPC의 라우터나 피어링을 거친다.

## 실행

```bash
cd demo && docker compose up -d --build
```

로컬 관리 토큰은 `demo-admin-token`이고, statemig는 `localhost:18090`이다.

## 검사

스택을 띄우고 호스트에서 실행한다. `httpx`와 `redis`가 필요하다.

| 명령 | 확인 내용 |
|---|---|
| `python demo/adapter_check.py` | 공통 API로 prepare, execute, rollback, commit |
| `python demo/scenario.py --mode normal` | 부하 중 컷오버, 롤백, commit |
| `python demo/scenario.py --mode failure` | 대상 Redis 강제 종료 후 롤백 |
| `python demo/faults.py` | 실패 경로와 권한 제한 |
| `python demo/onprem_check.py` | 온프레 Valkey 전환과 롤백 |
| `python demo/chain_check.py` | aws → 온프레 → gcp 연속 이전 |
| `python demo/aws_check.py` | 실제 ElastiCache. 같은 VPC의 호스트에서 실행 |

`demo/app`은 검사에서 부하를 거는 앱이다. 실제 공구앱은 [demo 레포](https://github.com/SoftBank-2026-Hackathon-Foxtrot/demo)에 있다.

## 아직 안 된 것

- AWS 관리 VPC 배포와 Tailscale 구간 검증
- Memorystore 검증
- 오케스트레이터 연결
- 공통 스펙 확정 후 API 조정

구현이 설계와 다른 점은 [DESIGN.md](DESIGN.md), 온프레 구성은 [onprem/README.md](onprem/README.md)에 있다.
