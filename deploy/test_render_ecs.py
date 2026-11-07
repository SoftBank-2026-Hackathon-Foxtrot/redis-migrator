import copy
import json
import unittest
from pathlib import Path

from deploy.render_ecs import render


class RenderEcsTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads(Path(__file__).with_name("ecs.example.json").read_text())

    def test_private_task_uses_original_endpoint_secrets_without_tunnel_container(self):
        task, service = render(self.config)
        containers = {c["name"]: c for c in task["containerDefinitions"]}
        self.assertEqual(set(containers), {"proxy", "syncworker", "statemig"})
        self.assertEqual(task["networkMode"], "awsvpc")
        for name, secret in (("proxy", "proxyEndpoints"), ("syncworker", "adminEndpoints"),
                             ("statemig", "adminEndpoints")):
            refs = {s["name"]: s["valueFrom"] for s in containers[name]["secrets"]}
            self.assertEqual(refs["STATEMIG_ENDPOINTS"], self.config["secrets"][secret])
            self.assertNotIn("linuxParameters", containers[name])
        network = service["networkConfiguration"]["awsvpcConfiguration"]
        self.assertEqual(network["assignPublicIp"], "DISABLED")
        self.assertEqual(network["subnets"], self.config["subnetIds"])

    def test_single_task_preserves_control_record_and_local_admin_addresses(self):
        task, service = render(self.config)
        self.assertEqual(service["desiredCount"], 1)
        self.assertEqual(service["deploymentConfiguration"],
                         {"minimumHealthyPercent": 0, "maximumPercent": 100})
        volume = task["volumes"][0]["efsVolumeConfiguration"]
        self.assertEqual(volume["authorizationConfig"],
                         {"accessPointId": self.config["accessPointId"], "iam": "ENABLED"})
        self.assertEqual(volume["transitEncryption"], "ENABLED")
        proxy, worker, api = task["containerDefinitions"]
        self.assertEqual(proxy["mountPoints"], api["mountPoints"])
        self.assertEqual([p["containerPort"] for c in (proxy, worker, api)
                          for p in c.get("portMappings", [])], [6379, 8090])
        self.assertEqual({v["name"]: v["value"] for v in worker["environment"]}
                         ["STATEMIG_ADMIN"], "127.0.0.1:7100")
        self.assertEqual(len(api["dependsOn"]), 2)
        self.assertEqual(service["serviceRegistries"][0]["registryArn"], self.config["serviceRegistryArn"])

    def test_invalid_or_missing_inputs_are_rejected_before_writing(self):
        with self.assertRaisesRegex(ValueError, "JSON object"):
            render([])
        cases = [("fileSystemId", "tmp"), ("accessPointId", None), ("subnetIds", []),
                 ("securityGroupIds", ["0.0.0.0/0"]), ("family", "bad/name"), ("taskRoleArn", ""),
                 ("serviceRegistryArn", "redis.internal")]
        for key, value in cases:
            with self.subTest(key=key):
                config = copy.deepcopy(self.config)
                config[key] = value
                with self.assertRaises(ValueError):
                    render(config)
        config = copy.deepcopy(self.config)
        config["secrets"]["proxyEndpoints"] = "aws=redis://app:password@10.0.0.1:6379"
        with self.assertRaisesRegex(ValueError, "Secrets|secretsmanager"):
            render(config)


if __name__ == "__main__":
    unittest.main()
