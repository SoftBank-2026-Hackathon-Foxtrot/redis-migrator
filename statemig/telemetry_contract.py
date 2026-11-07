"""An explicit public allowlist: never forward adapter payloads, plans or credentials."""

from __future__ import annotations

from typing import Any


def fields(names: str) -> dict[str, Any]:
    return dict.fromkeys(names.split(), True)


DECISION = fields("result reasons")
VERIFICATION = fields(
    "method scope result evidenceKind observedAt transitionId operationId checkedCount "
    "mismatchCount committedThroughLsn durableOriginLsn barrierId valid comparedBuckets "
    "mismatchedBuckets matched missing different extra"
)
CHECK = {
    **fields("code stage required evidenceRef observedAt validUntil transitionId topologyRevision"),
    "decision": DECISION,
}
READINESS = {key: DECISION for key in ("cutover", "resume", "rollback", "advance")}
FLOW = fields("state backlog lagSeconds")
OBJECT_DECISION = {
    **DECISION,
    "checks": [{**fields("name ok"), "verification": VERIFICATION}],
}
DETAILS = {
    "database": {
        **fields(
            "observedAt observationAvailable proxyPaused sourceWriterSessions "
            "targetWriterSessions sourcePreparedTransactions targetPreparedTransactions "
            "targetReachable accessEvidence"
        ),
        "verification": VERIFICATION,
        "reverseReplication": fields("status state lagBytes lagSeconds backlog observedAt"),
    },
    "redis": {
        **fields("dataRoute producerRoute consumerRoute sourceConsumerAccess drainStatus"),
        "sync": {
            **fields(
                "running halted backlog lag_ms changed_keys not_ready error observedAt "
                "direction copied oldestDirtyAgeMs epoch"
            ),
            "verification": VERIFICATION,
        },
        "proxy": fields(
            "gen paused inflight_writes open_tx last_pause_ms pubsub_sessions "
            "pubsub_moved pubsub_cut pubsub_dropped observedAt"
        ),
    },
    "object": {
        **fields(
            "appsLive lastSuccessfulSyncAt lastVerificationAt lastProgressAt "
            "initialCopyObservedAt forwardObservedAt reverseObservedAt"
        ),
        "apps": fields("generation live acked waiting recovering oldestReportAt observedAt"),
        "readiness": {key: OBJECT_DECISION for key in ("cutover", "rollback", "advance")},
    },
    "compute": {
        **fields("targetRef imageId configVersion containerId probePort"),
        "check": True,
    },
}
STATUS = {
    **fields(
        "migrationId workflowId resourceId kind revision lifecycle health phase "
        "activeOperationId transitionId topologyRevision lastSuccessfulSyncAt "
        "lastVerificationAt lastProgressAt"
    ),
    "observed": fields("observedAt route businessWriteAccess targetWritesPossible"),
    "progress": fields("completed total unit bytes totalBytes"),
    "readiness": READINESS,
    "rollback": fields("method reasons"),
    "checks": [CHECK],
    "details": DETAILS,
    "replication": {
        "forward": FLOW,
        "reverse": FLOW,
        "initialCopy": fields("completed total unit bytes totalBytes"),
    },
    "routing": fields("read write generation"),
}
OPERATION = fields(
    "operationId migrationId action status requestedAt startedAt finishedAt phase confirmedChanges"
)
PREPARATION = {
    **fields(
        "migrationId workflowId stage status observedAt requestedAt startedAt finishedAt "
        "jobId targetRef vmId cloud imageId configVersion containerId"
    ),
    "changes": fields("create update delete replace"),
    "check": True,
    "operations": [OPERATION],
}


def project(value: Any, spec: Any) -> Any:
    if value is None:
        return None
    if spec is True:
        if isinstance(value, str):
            return value[:512]
        if isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, list):
            return [
                project(item, True) for item in value[:100] if not isinstance(item, (dict, list))
            ]
        return None
    if isinstance(spec, list):
        return [project(item, spec[0]) for item in value[:100]] if isinstance(value, list) else []
    if not isinstance(value, dict):
        return {}
    return {key: project(value[key], child) for key, child in spec.items() if key in value}


APP = {
    **fields(
        "workflowId desired generation heldSince requestedAt releasedAt "
        "redisRoute redisRouteGeneration"
    ),
    "observed": fields("observedAt"),
    "actors": [fields("actorId state inFlight activeWebSockets reported expected observedAt")],
}
PUBLIC = {
    **fields(
        "workflowId scope windowStart windowEnd total failed timeouts unknownLocation "
        "knownLocationSamples enoughSamples p95Ms missingSamples maxSampleGapSeconds lastSampleAt"
    ),
    "observed": fields("observedAt"),
    "failureIntervals": [fields("firstFailureAt lastFailureAt recoveredAt")],
}
TRAFFIC = {
    **fields(
        "workflowId migrationId operationId status action requestedAt startedAt "
        "finishedAt configVerified"
    ),
    "observed": fields("observedAt"),
}


def snapshot_projection(kind: str, data: dict[str, Any]) -> dict[str, Any]:
    spec = {
        "DATABASE": STATUS,
        "REDIS": STATUS,
        "OBJECT_STORAGE": STATUS,
        "INFRA": PREPARATION,
        "COMPUTE": PREPARATION,
        "APP": APP,
        "PUBLIC": PUBLIC,
        "TRAFFIC": TRAFFIC,
    }[kind]
    result: dict[str, Any] = project(data, spec)
    if kind in {"INFRA", "COMPUTE"}:
        result["observed"] = {"observedAt": data.get("observedAt")}
    if kind in {"PUBLIC", "TRAFFIC"}:
        for key in (
            ("locations", "observedPercent") if kind == "PUBLIC" else ("requested", "applied")
        ):
            values = data.get(key)
            result[key] = (
                {
                    str(k)[:64]: v
                    for k, v in list(values.items())[:16]
                    if isinstance(v, (int, float)) and not isinstance(v, bool)
                }
                if isinstance(values, dict)
                else None
            )
    return result
