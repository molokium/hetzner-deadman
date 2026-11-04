"""Client-level tests: pagination, dry-run safety, error classification."""

from __future__ import annotations

import unittest
from unittest import mock

from deadman.config import Config
from deadman.hetzner import (
    HCloudClient,
    HCloudError,
    NotFound,
    PERMANENT,
    TRANSIENT,
    classify,
)
from deadman.log import null_logger


def make_config(**overrides):
    base = {
        "token": "test-token",
        "state_dir": "/tmp",
        "log_file": "/tmp/deadman-test-client.log",
    }
    base.update(overrides)
    return Config.load(overrides=base, require_token=False)


class TestPagination(unittest.TestCase):
    def test_three_pages_no_duplicates(self):
        cfg = make_config()
        client = HCloudClient(cfg, logger=null_logger())
        pages = {
            1: {
                "servers": [{"id": i} for i in range(1, 51)],
                "meta": {"pagination": {"page": 1, "next_page": 2}},
            },
            2: {
                "servers": [{"id": i} for i in range(51, 101)],
                "meta": {"pagination": {"page": 2, "next_page": 3}},
            },
            3: {
                "servers": [{"id": i} for i in range(101, 121)],
                "meta": {"pagination": {"page": 3, "next_page": None}},
            },
        }

        def fake_get(path, allow_404=False):
            from urllib.parse import parse_qs, urlparse

            page = int(parse_qs(urlparse(path).query).get("page", ["1"])[0])
            return pages[page]

        with mock.patch.object(client, "_get", side_effect=fake_get):
            servers = client.list_servers()
        self.assertEqual(len(servers), 120)
        self.assertEqual(len({s["id"] for s in servers}), 120)

    def test_broken_next_page_stops(self):
        cfg = make_config()
        client = HCloudClient(cfg, logger=null_logger())
        with mock.patch.object(
            client,
            "_get",
            return_value={"servers": [{"id": 1}], "meta": {"pagination": {"next_page": 1}}},
        ):
            self.assertEqual(len(client.list_servers()), 1)


class TestDryRun(unittest.TestCase):
    def test_no_network_for_mutations(self):
        cfg = make_config(dry_run=True)
        client = HCloudClient(cfg, logger=null_logger())
        with mock.patch.object(client._opener, "open", side_effect=AssertionError("network!")):
            self.assertTrue(client.delete_server(1))
            client.change_server_protection(1, False)
            client.update_primary_ip(2, auto_delete=False)
            client.change_primary_ip_protection(2, False)
            client.unassign_primary_ip(2)
            self.assertTrue(client.delete_primary_ip(2))
            client.change_floating_ip_protection(3, False)
            client.unassign_floating_ip(3)
            self.assertTrue(client.delete_floating_ip(3))
            image, action = client.create_snapshot(1, "x", {"deadman": "preserved"})
            self.assertEqual(image["id"], 0)
        self.assertGreaterEqual(len(client.dry_run_log), 9)


class TestErrors(unittest.TestCase):
    def test_classification(self):
        self.assertEqual(classify(404, "not_found"), "ALREADY_DONE")
        self.assertEqual(classify(401, "unauthorized"), PERMANENT)
        self.assertEqual(classify(500, None), TRANSIENT)
        self.assertEqual(classify(422, "server_not_stopped"), "PRECONDITION")
        self.assertEqual(classify(429, None), "RATE_LIMITED")

    def test_delete_404_is_success(self):
        cfg = make_config()
        client = HCloudClient(cfg, logger=null_logger())

        def fake_request(method, path, **kwargs):
            raise NotFound("gone", method=method, path=path)

        with mock.patch.object(client, "request", side_effect=fake_request):
            self.assertTrue(client.delete_primary_ip(9))
            self.assertTrue(client.delete_floating_ip(9))
            self.assertTrue(client.delete_server(9))

    def test_resolve_protected_ip_ids(self):
        """Explicit configuration is authoritative (bug 3): naming one ID must
        not silently reserve the server's IPv4/IPv6 as well."""
        cfg = make_config(main_primary_ip_ids=[900001], reserve_main_ipv6=True)
        client = HCloudClient(cfg, logger=null_logger())
        server = {
            "id": 1,
            "public_net": {"ipv4": {"id": 900002, "ip": "1.2.3.4"}, "ipv6": {"id": 900003}},
        }
        self.assertEqual(client.resolve_protected_ip_ids(server, primary_ips=[]), [900001])

        # Nothing explicit: IPv4 always, IPv6 only when asked.
        cfg = make_config(reserve_main_ipv6=True)
        client = HCloudClient(cfg, logger=null_logger())
        self.assertEqual(client.resolve_protected_ip_ids(server, primary_ips=[]), [900002, 900003])

        cfg = make_config(reserve_main_ipv6=False)
        client = HCloudClient(cfg, logger=null_logger())
        self.assertEqual(client.resolve_protected_ip_ids(server, primary_ips=[]), [900002])

        # Address form resolves to the same ID, with or without a /64 suffix.
        cfg = make_config(main_primary_ip="1.2.3.4")
        client = HCloudClient(cfg, logger=null_logger())
        primary = [{"id": 900002, "ip": "1.2.3.4"}, {"id": 900003, "ip": "2001:db8::/64"}]
        self.assertEqual(client.resolve_protected_ip_ids(server, primary_ips=primary), [900002])

        cfg = make_config(main_primary_ip="2001:db8::")
        client = HCloudClient(cfg, logger=null_logger())
        self.assertEqual(
            client.resolve_protected_ip_ids(server, primary_ips=primary), [900003]
        )

        # PROTECTED_IPS addresses are always unioned in.
        cfg = make_config(protected_ips=["203.0.113.9"])
        client = HCloudClient(cfg, logger=null_logger())
        primary = [
            {"id": 900002, "ip": "1.2.3.4"},
            {"id": 900004, "ip": "203.0.113.9"},
        ]
        self.assertIn(900004, client.resolve_protected_ip_ids(server, primary_ips=primary))


if __name__ == "__main__":
    unittest.main()
