"""Render one Redis migration task behind the shared IaC subnet routers.

Run: python deploy/render_ecs.py --config deploy/ecs.example.json --output-dir /tmp/redis-ecs
The host subnets need routes to both original Redis endpoints through the shared
cloud routers. Include cache/PSA ranges, return routes and TCP 6379 (6378 for
Memorystore Redis TLS). Approve Tailnet routes and grants before starting a move.
Memorystore PRIVATE_SERVICE_ACCESS also needs producer-side custom route import;
consumer-side export alone is insufficient. No cloud resources are changed here.
"""

import argparse
import json
import re
from pathlib import Path


def _required(config: dict, key: str) -> str:
    value = config.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a nonempty string")
    return value


def _arn(config: dict, key: str, service: str) -> str:
    value = _required(config, key)
    if not re.fullmatch(rf"arn:aws:{service}:[a-z0-9-]*:[0-9]{{12}}:.+", value):
        raise ValueError(f"{key} must be a {service} ARN")
    return value


def _ids(config: dict, key: str, prefix: str) -> list[str]:
    values = config.get(key)
    if not isinstance(values, list) or not values or any(
        not isinstance(v, str) or not re.fullmatch(rf"{prefix}-[a-f0-9]+", v) for v in values
    ):
        raise ValueError(f"{key} must contain existing {prefix} IDs")
    return values


def render(config: dict) -> tuple[dict, dict]:
    """Use existing private URLs from secret references, without rewriting hosts or ports."""
    if not isinstance(config, dict):
        raise ValueError("config must be a JSON object")
    family = _required(config, "family")
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,255}", family):
        raise ValueError("family must be an ECS task family name")
    region = _required(config, "region")
    go_image, api_image = _required(config, "goImage"), _required(config, "apiImage")
    execution_role = _arn(config, "executionRoleArn", "iam")
    task_role = _arn(config, "taskRoleArn", "iam")
    secret_config = config.get("secrets")
    if not isinstance(secret_config, dict):
        raise ValueError("secrets must map names to existing Secrets Manager ARNs")
    secrets = {key: _arn(secret_config, key, "secretsmanager")
               for key in ("proxyEndpoints", "adminEndpoints", "adminToken", "clients")}
    active, app_user = _required(config, "initialBackend"), _required(config, "appUser")
    fs_id = _ids({"id": [config.get("fileSystemId")]}, "id", "fs")[0]
    ap_id = _ids({"id": [config.get("accessPointId")]}, "id", "fsap")[0]

    def container(name: str, image: str, port: int, endpoint_secret: str, env: dict) -> dict:
        check = ["CMD-SHELL", "wget -q -O /dev/null --header \"Authorization: Bearer "
                 "$STATEMIG_ADMIN_TOKEN\" http://127.0.0.1:" + str(port) + "/status"]
        return {
            "name": name, "image": image, "essential": True,
            "environment": [{"name": k, "value": v} for k, v in env.items()],
            "secrets": [{"name": "STATEMIG_ENDPOINTS", "valueFrom": secrets[endpoint_secret]},
                        {"name": "STATEMIG_ADMIN_TOKEN", "valueFrom": secrets["adminToken"]}],
            "logConfiguration": {"logDriver": "awslogs", "options": {
                "awslogs-group": _required(config, "logGroup"), "awslogs-region": region,
                "awslogs-stream-prefix": name}},
            "healthCheck": {"command": check, "interval": 15, "timeout": 5,
                            "retries": 3, "startPeriod": 60},
        }

    state = [{"sourceVolume": "control", "containerPath": "/state", "readOnly": False}]
    proxy = container("proxy", go_image, 7000, "proxyEndpoints", {
        "STATEMIG_ACTIVE": active, "STATEMIG_CONTROL_FILE": "/state/control.json",
        "STATEMIG_LISTEN": ":6379", "STATEMIG_ADMIN": "127.0.0.1:7000"})
    proxy.update({"entryPoint": ["statemig-proxy"], "mountPoints": state,
                  "portMappings": [{"containerPort": 6379, "protocol": "tcp"}]})
    proxy["secrets"].append({"name": "STATEMIG_CLIENTS", "valueFrom": secrets["clients"]})
    worker = container("syncworker", go_image, 7100, "adminEndpoints", {
        "STATEMIG_ADMIN": "127.0.0.1:7100"})
    worker["entryPoint"] = ["statemig-syncworker"]
    api = container("statemig", api_image, 8090, "adminEndpoints", {
        "STATEMIG_PROXY": "http://127.0.0.1:7000",
        "STATEMIG_SYNCWORKER": "http://127.0.0.1:7100",
        "STATEMIG_CONTROL_FILE": "/state/control.json", "STATEMIG_APP_USER": app_user})
    api.update({"command": ["uvicorn", "statemig.api:app", "--host", "0.0.0.0", "--port", "8090",
                           "--timeout-keep-alive", "120"],
                "mountPoints": state, "portMappings": [{"containerPort": 8090, "protocol": "tcp"}],
                "dependsOn": [{"containerName": name, "condition": "HEALTHY"}
                              for name in ("proxy", "syncworker")]})
    api["healthCheck"]["command"] = ["CMD", "python", "-c",
        "import os,urllib.request as u;u.urlopen(u.Request('http://127.0.0.1:8090/status',"
        "headers={'Authorization':'Bearer '+os.environ['STATEMIG_ADMIN_TOKEN']}),timeout=3)"]
    task = {
        "family": family, "requiresCompatibilities": ["FARGATE"], "networkMode": "awsvpc",
        "cpu": "512", "memory": "1024", "executionRoleArn": execution_role, "taskRoleArn": task_role,
        "runtimePlatform": {"cpuArchitecture": "X86_64", "operatingSystemFamily": "LINUX"},
        "containerDefinitions": [proxy, worker, api],
        "volumes": [{"name": "control", "efsVolumeConfiguration": {
            "fileSystemId": fs_id, "rootDirectory": "/", "transitEncryption": "ENABLED",
            "authorizationConfig": {"accessPointId": ap_id, "iam": "ENABLED"}}}],
    }
    service = {
        "cluster": _required(config, "cluster"), "serviceName": _required(config, "serviceName"),
        "taskDefinition": family, "desiredCount": 1, "launchType": "FARGATE", "platformVersion": "1.4.0",
        "deploymentConfiguration": {"minimumHealthyPercent": 0, "maximumPercent": 100},
        "networkConfiguration": {"awsvpcConfiguration": {
            "subnets": _ids(config, "subnetIds", "subnet"),
            "securityGroups": _ids(config, "securityGroupIds", "sg"), "assignPublicIp": "DISABLED"}},
        # Existing private Cloud Map service with A records: apps keep this DNS name.
        "serviceRegistries": [{"registryArn": _arn(config, "serviceRegistryArn", "servicediscovery")}],
    }
    return task, service


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        task, service = render(json.loads(args.config.read_text()))
    except (ValueError, OSError) as error:
        parser.error(str(error))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for name, value in (("task-definition.json", task), ("service.json", service)):
        (args.output_dir / name).write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
