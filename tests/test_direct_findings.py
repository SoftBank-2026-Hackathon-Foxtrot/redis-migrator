"""The direct-connection check on a source that refuses CLIENT LIST (Memorystore)."""
import unittest

from statemig.migration import _direct_findings, persistent_proxy_env


class DirectFindingsTest(unittest.TestCase):
    def test_refused_client_list_is_a_warning_behind_a_persistent_proxy(self):
        errors, warnings = _direct_findings(None, persistent_proxy=True)
        self.assertEqual(errors, [])
        self.assertEqual(len(warnings), 1)
        self.assertIn("CLIENT LIST refused", warnings[0])

    def test_refused_client_list_is_an_error_without_a_persistent_proxy(self):
        errors, warnings = _direct_findings(None, persistent_proxy=False)
        self.assertEqual(len(errors), 1)
        self.assertIn("CLIENT LIST refused", errors[0])
        self.assertEqual(warnings, [])

    def test_direct_connections_are_an_error_either_way(self):
        for persistent_proxy in (False, True):
            with self.subTest(persistent_proxy=persistent_proxy):
                errors, _ = _direct_findings(["direct:api@10.0.0.5:5000"], persistent_proxy)
                self.assertEqual(len(errors), 1)
                self.assertIn("1 direct connections", errors[0])

    def test_no_direct_connections_pass(self):
        self.assertEqual(_direct_findings([], persistent_proxy=False), ([], []))


class PersistentProxyEnvTest(unittest.TestCase):
    def test_values(self):
        for value in ("1", "true", "TRUE", " yes ", "on"):
            self.assertTrue(persistent_proxy_env(value), value)
        for value in (None, "", "0", "false", "no"):
            self.assertFalse(persistent_proxy_env(value), value)


if __name__ == "__main__":
    unittest.main()
