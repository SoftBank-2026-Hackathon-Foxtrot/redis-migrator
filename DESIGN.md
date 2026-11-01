# 구현 메모: 설계와 다른 점

기준 설계는 Notion의 "Redis·큐 무중단 이전 핵심 설계 (통합)"이다. 여기에는 지금도 설계와 다른 것만 적는다.

| 항목 | 설계 | 구현 | 이유 |
|---|---|---|---|
| 전환 단계 위치 | Migration Worker 안의 단계 | 별도 서비스 `statemig` | Migration Worker와 아직 연결하지 않음 |
| 관리 DB | Migration Worker의 관리 DB | 공유 볼륨의 JSON 파일 2개 (`control.json`, `migration.json`), 원자적 교체 | 로컬 대용 |
| DB 컷오버와 함께 전환·취소 | 같은 쓰기 정지 구간, 실패 시 함께 취소 | Redis만 전환 | PgBouncer·Migration Worker와 미연결 |
| 앱이 쓰는 비밀번호 | 시크릿 저장소 참조 | compose 환경 변수 | 로컬 대용 |
| 관리형 Redis의 ACL 변경 API | 회수가 안 되면 자격 증명 격리 | 격리로만 동작. ElastiCache `ModifyUser` 같은 API 호출은 없음 | 반영이 비동기라 쓰기 정지 안에서 기다릴 수 없고, 실제 인스턴스로 시험하지 못함 |

## 로컬 검증 범위

- Redis: Valkey 8 두 대(aws, gcp 역할). 실제 ElastiCache / Memorystore에서는 돌려보지 않았다. "CONFIG·ACL이 막힌 Redis"는 ACL 규칙으로 흉내 낸 것이다.
- 원본 데이터: SQLite(RDB 대용). DB 컷오버는 하지 않았고, Redis 전환만 검증했다.
- 장애 롤백 때 역방향 backlog는 매번 0이었다. "역방향 미처리분만큼 유실"의 유실이 실제로 생기는 경우는 재현하지 못했다.
- 쓰기 정지 측정(`demo/bench.py`)은 지연만 흉내 낸 것이다. 대역폭 제한이 없고, 값이 작은 세션 키이며, 변경량이 적은 부하에서 키 수별 1회씩 쟀다.
- `demo/faults.py`로 B 방식의 약점을 확인했다. 대상 값 하나를 알림 없이 바꾸면 샘플에 안 걸릴 때 그대로 전환된다. 이 위험은 감수하기로 했다. `mode: "full"`은 비교용으로 남겨 두었다.
- 크래시 복구 시험은 전환 도중 상태를 파일과 프록시에 직접 만들어 놓고 제어부를 재시작한 것이다. 실제 프로세스를 전환 도중에 죽인 것은 아니다.
- 호스트(Windows) → Docker 포트 포워딩이 약 96 rps에서 포화된다. 데모 콘솔의 부하는 그래서 compose 안의 `loadgen`이 만든다. `scenario.py`, `faults.py`는 호스트에서 돌아 지연이 더 크게 나온다.
