# 구현 메모: 설계와 다른 점

기준 설계는 Notion의 "Redis·큐 무중단 이전 핵심 설계 (통합)"이다. 여기에는 지금도 설계와 다른 것만 적는다.

| 항목 | 설계 | 구현 | 이유 |
|---|---|---|---|
| 전환 단계 위치 | Migration Worker 안의 단계 | 별도 서비스 `statemig` | Migration Worker와 아직 연결하지 않음 |
| 관리 DB | Migration Worker의 관리 DB | 공유 볼륨의 JSON 파일 2개 (`control.json`, `migration.json`), 원자적 교체 | 로컬 대용 |
| DB 컷오버와 함께 전환·취소 | 같은 쓰기 정지 구간, 실패 시 함께 취소 | Redis만 전환 | PgBouncer·Migration Worker와 미연결 |
| 앱이 쓰는 비밀번호 | 시크릿 저장소 참조 | compose 환경 변수 | 로컬 대용 |
| 프록시 구현 | 미정 | Go | 한 프로세스에서 여러 코어를 쓴다 |
| 관리형 Redis의 ACL 변경 API | 회수가 안 되면 자격 증명 격리 | 격리로만 동작. ElastiCache `ModifyUser` 같은 API 호출은 없음 | 반영이 비동기라 쓰기 정지 안에서 기다릴 수 없고, 실제 인스턴스로 시험하지 못함 |

## 로컬 검증 범위

- Redis: Valkey 8 두 대(aws, gcp 역할). "CONFIG·ACL이 막힌 Redis"는 ACL 규칙으로 흉내 낸 것이다.
- 실제 ElastiCache for Valkey 8.0은 `demo/aws_check.py`로 2026-11-02에 1회 돌렸다(`cache.t4g.micro`, 같은 VPC의 EC2, TLS, 8/8 통과). `CONFIG`와 `ACL`이 막혀 쓰기 차단은 자격 증명 격리로 동작했고, 앱 사용자는 원본에 직접 쓸 수 있었다. 알림은 파라미터 그룹으로 켰다. Memorystore와 환경 간 VPN은 돌려보지 않았다.
- 원본 데이터: SQLite(RDB 대용). DB 컷오버는 하지 않았고, Redis 전환만 검증했다.
- 장애 롤백 때 역방향 backlog는 매번 0이었다. "역방향 미처리분만큼 유실"의 유실이 실제로 생기는 경우는 재현하지 못했다.
- 쓰기 정지 측정(`demo/bench.py`)은 지연만 흉내 낸 것이다. 대역폭 제한이 없고, 값이 작은 세션 키이며, 변경량이 적은 부하에서 키 수별 1회씩 쟀다.
- `demo/faults.py`로 B 방식의 약점을 확인했다. 대상 값 하나를 알림 없이 바꾸면 샘플에 안 걸릴 때 그대로 전환된다. 이 위험은 감수하기로 했다. `mode: "full"`은 비교용으로 남겨 두었다.
- 온프레 Valkey(`onprem/`)는 로컬 Docker 네트워크에서만 검증했다(`demo/onprem_check.py`). VPN 구간은 없다. 활성 온프레를 강제 종료하면 그동안의 요청은 실패하고, `everysec` 범위의 쓰기 유실은 재현하지 못했다.
- aws → 온프레 → gcp 연속 이전(`demo/chain_check.py`)도 로컬 컨테이너 세 대에서만 돌렸다. 구간별로 commit한 경로와 마지막 구간의 정상 롤백만 다루고, 연속 이전 중 장애 롤백은 시험하지 않았다.
- 크래시 복구 시험은 전환 도중 상태를 파일과 프록시에 직접 만들어 놓고 제어부를 재시작한 것이다. 실제 프로세스를 전환 도중에 죽인 것은 아니다.
- 호스트(Windows) → Docker 포트 포워딩이 약 96 rps에서 포화된다. 데모 콘솔의 부하는 그래서 compose 안의 `loadgen`이 만든다. `scenario.py`, `faults.py`는 호스트에서 돌아 지연이 더 크게 나온다.
