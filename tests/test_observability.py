import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from statemig.migration import Migration


class Observations(unittest.IsolatedAsyncioTestCase):
    def migration(self, status):
        migration = object.__new__(Migration)
        migration.sync = SimpleNamespace(call=AsyncMock(return_value=status))
        migration.source = SimpleNamespace(name="aws")
        migration.target = SimpleNamespace(name="gcp")
        migration._state = "ACTIVE"
        migration.sync_error = None
        return migration

    async def test_backlog_direction_and_verification_are_safe(self):
        migration = self.migration({"running": True, "sync": {"src": "gcp", "dst": "aws", "copied": 7,
            "backlog": 2, "epoch": "worker-2", "compare_valid": True,
            "last_compare": {"method": "values", "compared": 10, "differed": 2, "keys": ["session:secret"]}}})
        result = await migration.sync_view()
        self.assertEqual(result["direction"], "REVERSE")
        self.assertEqual(result["backlog"], 2)
        self.assertEqual(result["verification"]["mismatchCount"], 2)
        self.assertNotIn("session:secret", str(result))
        self.assertIsNotNone(result["observedAt"])

    async def test_unreachable_is_not_an_empty_backlog(self):
        migration = self.migration({})
        migration.sync.call.side_effect = RuntimeError("redis://password@host")
        result = await migration.sync_view()
        self.assertIsNone(result["backlog"])
        self.assertIsNone(result["observedAt"])
        self.assertNotIn("password", str(result))
