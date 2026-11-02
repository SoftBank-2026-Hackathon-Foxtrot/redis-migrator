# redis-migrator

치이카와 공구앱이 쓰는 Redis(세션·캐시·rate limit·작업 큐)를 다른 환경으로 무중단 이전하는 도구다. 설계는 Notion의 "Redis·큐 무중단 이전 핵심 설계 (통합)" 문서를 따른다. 구현하면서 설계와 달라진 점은 [DESIGN.md](DESIGN.md)에 있다.

| 디렉터리 | 역할 |
|---|---|
| `proxy/` (Go, 기본) | 중앙 RESP 프록시(`cmd/proxy`, RESP `:6379`, admin API `:7000`)와 Sync Worker(`cmd/syncworker`, admin API `:7100`). keyspace 알림 + 양방향 스캔 복사, 역방향 동기화, VERIFY |
| `dataplane/` (Python) | 같은 프록시와 Sync Worker의 Python 버전. `DATAPLANE=python`으로 쓴다. 비교는 [PERFORMANCE.md](PERFORMANCE.md) |
| `statemig/` (Python) | 제어부. 컷오버·롤백 단계, 제어 레코드 기록, ACL fence |
| `onprem/` | 온프레미스용 Valkey 8.0 배포 구성. [onprem/README.md](onprem/README.md) |
| `demo/app` | 공구앱(FastAPI)과 큐 worker. 원본 데이터는 SQLite(RDB 대용) |
| `demo/scenario.py` | 부하를 건 채로 설계의 완료 기준을 검사한다 (호스트에서 실행) |
| `demo/faults.py` | 실패 처리와 권한 제한을 일부러 일으켜 검사한다 (호스트에서 실행) |
| `demo/onprem_check.py` | 온프레 Valkey로 전환·롤백하고 강제 종료까지 검사한다 (호스트에서 실행) |
| `demo/aws_check.py` | 실제 ElastiCache를 원본으로 전환·롤백을 검사한다 (같은 VPC의 호스트에서 실행) |
| `demo/chain_check.py` | aws → 온프레 → gcp를 한 번에 이전하고 마지막 구간의 롤백까지 검사한다 (호스트에서 실행) |
| `demo/console.py`, `demo/loadgen.py` | 화면 공유용 데모 콘솔과 그 부하 생성기 |
| `demo/bench.py`, `dataplane/latency.py` | 환경 간 지연을 흉내 내 쓰기 정지 시간을 잰다. 결과는 `demo/bench-results.jsonl` |
| `demo/compare_dataplanes.py` | Go와 Python을 번갈아 측정한다. 결과는 `demo/compare-results.jsonl` |

## 로컬 실행

```bash
cd demo && docker compose up -d --build
```

Python 버전은 compose 파일을 더한다. 검사 스크립트와 콘솔에는 `DATAPLANE=python` 환경 변수를 주면 된다.

```bash
cd demo && docker compose -f docker-compose.yml -f python.compose.yml up -d --build
```

시나리오는 호스트에서 돌린다. 장애 시나리오가 `docker compose kill`을 쓰기 때문이다. `httpx`와 `redis`가 있는 Python이 필요하다.

```bash
python demo/scenario.py --mode normal --users 12 --think-ms 150
```

```bash
python demo/scenario.py --mode failure --users 12 --think-ms 150
```

- `normal`: aws → gcp 컷오버, 정상 롤백, 다시 컷오버, commit
- `failure`: 컷오버 뒤 gcp Redis 컨테이너를 kill하고 장애 롤백
- 결과 상세는 `demo/result-<mode>.json`에 남는다. 시나리오마다 `docker compose down -v`로 초기화한 뒤 돌린다.

실패 경로는 일부러 일으켜 확인한다. 스택을 새로 띄우고 항목별로 검사한다.

```bash
python demo/faults.py
```

## 제어 API (statemig, `:18090`)

모든 관리 API(statemig, 프록시, Sync Worker)는 `Authorization: Bearer <STATEMIG_ADMIN_TOKEN>`이 있어야 호출된다. 로컬 값은 `demo-admin-token`이다.

| 메서드 | 경로 | 설명 |
|---|---|---|
| POST | `/migrations` `{"target":"gcp","budget_ms":1800,"mode":"delta"}` | precheck(알림·권한 실측, 예상 시간), 대상 큐 group 생성, Sync Worker 시작. `delta`(기본)는 쓰기를 멈추기 전에 전체 비교, `full`은 쓰기 정지 안에서 전체 복사·비교 |
| GET | `/migrations/current/ready` | 동기화 backlog가 작아졌는지 |
| POST | `/migrations/current/cutover` `{"budget_ms":1800}` | PAUSE → FINAL RECONCILE → VERIFY → SWITCH → FENCE → REVERSE SYNC → RESUME, 이후 큐 drain |
| POST | `/migrations/current/rollback` `{"mode":"normal"\|"failure"}` | 정상 롤백(대상 생존) / 장애 롤백(대상 소실) |
| POST | `/migrations/current/commit` | 역방향 동기화 중지 |
| GET | `/status` | 프록시, Sync Worker, 제어 레코드, 이벤트 |

## 데모 콘솔

버튼으로 한 단계씩 실행하고 프록시, Sync Worker, 양쪽 Redis를 실시간으로 본다. 진행 순서는 [demo/DEMO.md](demo/DEMO.md)에 있다.

```bash
python demo/console.py
```

http://localhost:18888 에서 **초기화**부터 누른다. 부하는 compose 안의 `loadgen` 컨테이너가 만든다.
