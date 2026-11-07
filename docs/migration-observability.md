# Redis OTEL 관측

`statemig/api.py`가 독립적으로 5초마다 CommonApi의 migration 상태와 Go proxy/syncworker의 실제 카운터를 읽어 공식 Python SDK로 내보냅니다. `statemig/common.py`는 실제 비동기 작업을 span으로 감싸며 접수 HTTP의 W3C context를 이어갑니다. Python 서비스와 기존 Go data plane을 함께 배포합니다.

필수 수치: 정·역방향 backlog/copied/oldestDirtyAgeMs/lag_ms, proxy의 in-flight writes/open transaction/last pause/pubsub 잔존. `copied`는 재복사 포함 처리 수이며 전체 키 진행률이 아닙니다. `lag_ms`는 알림/heartbeat 기반으로 전체 복제 지연의 상한이 아닙니다. worker 단절 시 없는 수치는 null이며 상태 신선도를 함께 봅니다. 원시 키와 오류 원문은 제외합니다.

검사: `python -m unittest discover -s tests -v`, `cd proxy && go test ./...`. 기존 직접 `/status` API는 제어·진단용으로 유지합니다.

## OTEL 실행 설정

공식 OpenTelemetry SDK가 OTLP HTTP/protobuf로 로그·메트릭·트레이스를 내보냅니다. 아래 값을 **프로세스 환경**에 주입합니다. endpoint가 없거나 `OTEL_SDK_DISABLED=true`이면 전송하지 않습니다. `.env` 파일을 만드는 것만으로 Python 프로세스 환경이 바뀌지는 않습니다.

```dotenv
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.internal:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<module-ingest-token>
```

Collector는 IaC의 `observability/compose.yaml`로 준비합니다. Collector → CP에는 별도 `OBSERVATION_TOKEN`을 씁니다. 모듈에 CP 수신 토큰을 주지 않습니다. 운영 주소는 사설망/VPN에서 접근하도록 구성하고, TLS를 사용하는 경우 SDK의 `OTEL_EXPORTER_OTLP_CERTIFICATE`와 HTTPS endpoint를 설정합니다. 앱 환경 값·DSN·Terraform output·원시 키·오류 원문은 전송하지 않습니다.

진행 상태는 OTEL 구조화 로그 → Collector → CP의 `OBS_LATEST`/24시간 이력, 수치는 Prometheus, 작업 span은 Tempo에 저장합니다. 플랫폼은 CP API만 조회합니다. 제어 명령·승인·멱등성·판정 근거는 기존 저장소가 원본이며 OTEL 전송 성공을 전환 조건으로 사용하지 않습니다. 관측이 끊겨도 이전 값을 0이나 성공으로 바꾸지 않습니다.

Python `telemetry.py`/`telemetry_contract.py`는 독립 이미지에 포함한 공통 계약 v1 사본입니다. 계약을 바꾸면 CP와 모든 Python 발행자의 사본 및 SDK 계약 테스트를 함께 갱신합니다. metric에는 operation/transition ID·키·URL을 label로 넣지 않으며, 값 캐시는 최대 2048 series/20초입니다. workflow ID는 마이그레이션별 화면 조회를 위한 제한된 차원입니다.
