# redis-migrator

Redis를 AWS, GCP, 온프레미스 사이에서 이전한다. 앱은 프록시 주소만 알고, 전환할 때 쓰기를 잠깐 멈춘 뒤 프록시가 대상 Redis를 바꾼다.

```text
앱 → Redis 프록시 → ElastiCache / Memorystore / 온프레 Valkey
           ↑                     ↑
       statemig  →  Sync Worker (양쪽 동기화)
```

| 구성 요소 | 코드 | 포트 | 역할 |
|---|---|---|---|
| Redis 프록시 (Go) | `proxy/cmd/proxy` | RESP `6379`, 관리 `7000` | 앱의 고정 주소. PAUSE, SWITCH, RESUME |
| Sync Worker (Go) | `proxy/cmd/syncworker` | 관리 `7100` | keyspace 알림과 스캔으로 복사, 역방향 동기화, 값 비교 |
| statemig (FastAPI) | `statemig/` | `8090` | 전환·롤백 단계 실행. 오케스트레이터가 호출 |

## 지원 범위

- 옮기는 키: `session:`, `cache:`, `q:delayed`, 실패 큐 `q:join:dead`, `q:notify:dead`. `rl:`은 옮기지 않는다
- 작업 큐 `q:join`, `q:notify`는 복사하지 않고, 원본을 다 소비한 뒤 소비자를 옮긴다
- Pub/Sub 구독(`SUBSCRIBE`, `PSUBSCRIBE`)은 전환해도 연결이 유지된다. 정상 전환과 정상 롤백에서는 메시지 유실·중복이 없다. `MONITOR`와 `SSUBSCRIBE`는 끊는다
- 여러 db는 번호 그대로 옮긴다
- 사전 점검에서 거부: Cluster 모드, 반대쪽에 없는 db 번호, db 0 밖의 큐, `maxmemory`가 있는데 `noeviction`이 아닌 경우

관리형 Redis는 CONFIG가 막혀 있으므로 keyspace 알림(`KEA`)과 `maxmemory-policy noeviction`을 파라미터 그룹이나 인스턴스 설정으로 켜 둬야 한다. ACL도 막혀 있어서 원본 쓰기 차단은 자격 증명 분리로만 동작한다.

## 공통 API

오케스트레이터가 호출하는 API다. [마이그레이션 공통 Design Doc](https://app.notion.com/p/40d8bee9ada4815e9e94ee454be82d4a) 초안 0.2의 형식을 따른다. `:8090`에서 제공하며 `Authorization: Bearer <STATEMIG_ADMIN_TOKEN>`이 필요하다. POST에는 `Idempotency-Key`도 필요하다.

| 메서드 | 경로 | 동작 |
|---|---|---|
| GET | `/v1/capabilities` | 지원 프로필과 제약 |
| POST | `/v1/migrations` | 계획 등록. 외부 변경 없음 |
| GET | `/v1/migrations/{id}` | 현재 상태와 진행 판정 |
| POST | `/v1/migrations/{id}/actions` | 명령 접수. `202`와 `operationId` |
| GET | `/v1/operations/{id}` | 명령 결과 |

```json
{"workflowId":"wf-001","resourceId":"redis-main","kind":"REDIS","profile":"redis-proxy-sync-v1",
 "sourceRef":"resource:aws","targetRef":"resource:gcp","bindingRef":"binding:redis-proxy",
 "policy":{"rollbackMode":"REVERSE_REQUIRED","maxQuiesceSeconds":1.8,"readyTimeoutSeconds":600}}
```

```json
{"action":"CUTOVER","expectedRevision":8,"transitionId":"tx-001","quiesceRef":"quiesce:redis-main:tx-001:1"}
```

| 명령 | Redis에서 하는 일 |
|---|---|
| `PREPARE` | 사전 점검 |
| `START_SYNC` | 복사와 변경 추적 시작. `readyTimeoutSeconds`까지 `READY`가 안 되면 취소 |
| `QUIESCE` | **확인만 한다.** `ready`와 키 수. 쓰기는 멈추지 않는다 |
| `CUTOVER` | **쓰기 정지, 전환, 재개까지 한 번에.** `maxQuiesceSeconds`가 쓰기 정지 예산 |
| `RESUME` | **이미 재개된 위치를 확인만 한다.** `expectedLocation`은 `TARGET` 또는 `SOURCE` |
| `ROLLBACK` | 원본 복귀, 재개까지 포함. `"mode":"failure"`는 대상 장애 시의 유실 감수 복귀 |
| `CANCEL` | 컷오버 전 취소. 대상 데이터는 남긴다 |
| `RECONCILE` | 기록과 실제 상태 대조. 외부 변경 없음 |
| `FINALIZE` | 역방향 또는 정방향 동기화 중지. 원본은 지우지 않는다 |

```text
CREATED → PREPARED → SYNCING ↔ READY → QUIESCED → SWITCHED → ACTIVE → COMPLETED
ACTIVE → QUIESCED → ROLLED_BACK_HELD → RESTORED → COMPLETED
```

| 응답 | 코드 |
|---|---|
| `400` | `INVALID_REQUEST`: 필수 값 누락 |
| `401` | 토큰 없음 |
| `404` | `NOT_FOUND` |
| `409` | `REVISION_CONFLICT`, `IDEMPOTENCY_CONFLICT`, `RESOURCE_BUSY`, `INVALID_STATE`, `UNKNOWN_STATE` |
| `422` | `UNSUPPORTED_PROFILE`, `PRECONDITION_FAILED` |

### 공통 문서와 다른 점

전환 동작은 팀 합의 전까지 바꾸지 않았다.

- `QUIESCE`와 `RESUME`은 확인만 한다. 쓰기 정지와 재개는 `CUTOVER`, `ROLLBACK` 안에서 한다.
- `ROLLBACK`의 `mode=failure`는 대상 장애 때 유실을 감수하고 원본으로 돌아간다. 공통 문서는 이 복귀를 금지한다.
- 어댑터가 1초마다 스스로 복구한다. 그 결과는 상태 조회에 드러나지만 `UNKNOWN`에서 멈춰 기다리지는 않는다. 어댑터가 명령 도중 재시작한 경우에만 그 명령이 `UNKNOWN`이 되어 `RECONCILE`을 요구한다.
- 쓰기 정지 예산을 넘으면 전환 전에 취소하고 원본에서 재개한다.

아직 구현하지 않은 것:

- `quiesceRef`는 값이 있는지만 본다. 검증하려면 공유 관리 DB가 필요하다.
- `readiness.advance`는 `PASS`가 되지 않는다. `WORKLOAD_RUNNING`, `BUSINESS_PROBE`는 오케스트레이터의 근거가 필요해 `UNKNOWN`이다.
- 계획과 명령 기록은 관리 DB 대신 `common.json`에 저장한다. 검사 근거는 저장하지 않고 조회 때 계산한다.
- `sourceRef`는 `resource:<STATEMIG_ENDPOINTS의 이름>` 형식이다. 비밀 저장소 참조는 없다.

## 내부 API

공통 API 아래에서 쓰는 API다. 모두 같은 토큰이 필요하다.

| 서비스 | 경로 |
|---|---|
| statemig | `POST /migrations`, `GET /migrations/current/ready`, `POST /migrations/current/{cutover,rollback,commit}`, `GET /status`, `/internal/{prepare,execute,rollback,commit,abort,status}` |
| 프록시 | `GET /status`, `POST /pause`, `POST /presubscribe`, `POST /switch`, `POST /resume`, `POST /drain-done` |
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
| `python demo/adapter_check.py` | 공통 API의 전체 명령, 재시도, 거부 |
| `python demo/scenario.py --mode normal` | 부하 중 컷오버, 롤백, commit |
| `python demo/scenario.py --mode failure` | 대상 Redis 강제 종료 후 롤백 |
| `python demo/faults.py` | 실패 경로와 권한 제한 |
| `python demo/recovery_check.py` | Redis가 빈 채로 재시작, 프록시·Sync Worker·statemig가 전환 도중 사망 |
| `python demo/onprem_check.py` | 온프레 Valkey 전환과 롤백 |
| `python demo/chain_check.py` | aws → 온프레 → gcp 연속 이전 |
| `python demo/multidb_check.py` | 여러 db의 번호 유지, 실패 큐 |
| `python demo/topology_check.py` | 사전 점검의 거부 조건 (자체 컨테이너) |
| `python demo/loss_report_check.py` | 장애 롤백의 유실 범위 보고 |
| `python demo/pubsub_check.py` | 전환 중 구독 유지, 메시지 유실·중복 |
| `cd proxy && go test ./...` | 멈춘 구독자, 구독 연결의 명령 제한 |
| `python demo/aws_check.py` | 실제 ElastiCache. 같은 VPC의 호스트에서 실행 |

`demo/app`은 검사에서 부하를 거는 앱이다. 실제 공구앱은 [demo 레포](https://github.com/SoftBank-2026-Hackathon-Foxtrot/demo)에 있다.

## 실환경 검증 (2026-11-03)

ElastiCache for Valkey 8.0, Memorystore for Redis 7.2, 온프레 Valkey 8.0을 Tailscale로 잇고 부하 중에 옮겼다. 상세는 [검증 결과](https://app.notion.com/p/40d8bee9ada48102a0ccfb62720fe22f)에 있다.

- 컷오버, 정상 롤백, 연속 이전: 쓰기 정지 210~676ms. 채팅(Pub/Sub) 유실·중복, WebSocket 끊김, 로그인 실패 모두 0건
- Valkey 8.0과 Redis 7.2 사이의 `DUMP`/`RESTORE`는 양방향으로 동작했다
- 장애 롤백: 쓰기 정지 0.5~4.1초. 간헐적인 약 3.6초 지연은 원인을 찾지 못했고, 이때는 예산 1800ms를 넘는다
- 구간별 1~2회, 키 7600개, 구독 3개 이하 규모다. DB·오브젝트·트래픽 계층과의 통합과 오케스트레이터는 포함하지 않았다

## 아직 안 된 것

- 장애 롤백의 간헐적 3.6초 지연 원인
- 오케스트레이터 연결
- 공통 스펙 확정 후 API 조정

구현이 설계와 다른 점은 [DESIGN.md](DESIGN.md), 온프레 구성은 [onprem/README.md](onprem/README.md)에 있다.
