# 마이그레이션 관측

공통 상태 API의 `details.redis.sync`에 backlog, copied, oldestDirtyAgeMs, 정/역방향, 실행 epoch와 비교 요약을 추가했습니다. `details.redis.proxy`는 pause, in-flight writes, 열린 transaction, 마지막 pause와 pub/sub 잔존을 제공합니다. 작업에는 startedAt/finishedAt이 추가됩니다.

copied는 재복사를 포함한 처리 건수이며 전체 키 진행률이 아닙니다. lag_ms는 heartbeat/알림 계열 값이며 전체 복제 지연의 상한이 아닙니다. worker 단절 시 backlog/관측 시각은 null입니다. 원시 key 목록과 오류 원문은 노출하지 않습니다. 비교 방식(values/buckets)과 유효 여부를 함께 확인합니다.

컨트롤 플레인의 등록된 어댑터 경로로 5초마다 수집합니다. 별도 exporter나 외부 전송 설정은 필요하지 않습니다. Python 상태 API와 Go syncworker를 함께 재빌드합니다.

검사: `python -m unittest discover -s tests -v`, `cd proxy && go test ./...`.
