"""Bug 8 guard: the docs must describe the code that actually runs.

``SPEC.md`` §5.2/§10 and the README teardown table are asserted against
``deadman.engine.STEPS``, and the "runs on the main server" invariants (no
power-off, no unassign of the main IP, live snapshot, protection cleared last)
are asserted to be present in the prose.  Documentation drifting away from the
implementation is exactly how the unsafe ordering survived the first review.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from deadman.engine import STEPS

ROOT = Path(__file__).resolve().parent.parent
SPEC = (ROOT / "SPEC.md").read_text(encoding="utf-8")
README = (ROOT / "README.md").read_text(encoding="utf-8")


def _table_steps(text: str) -> list[str]:
    return re.findall(r"^\|\s*\d+\s*\|\s*`([A-Z_]+)`", text, flags=re.MULTILINE)


def _section(text: str, start: str, end: str) -> str:
    begin = text.index(start)
    finish = text.index(end, begin)
    return text[begin:finish]


class TestDocsMatchImplementation(unittest.TestCase):
    def test_spec_step_table_matches_steps(self):
        section = _section(SPEC, "## 10. Step sequence", "Ordering rationale:")
        self.assertEqual(_table_steps(section), list(STEPS))

    def test_readme_step_table_matches_steps(self):
        section = _section(README, "## Teardown sequence", "### Retry semantics")
        self.assertEqual(_table_steps(section), list(STEPS))

    def test_spec_52_is_on_host_aware(self):
        section = _section(SPEC, "### 5.2", "### 5.3")
        self.assertIn("runs on the main server", section)
        self.assertIn("live snapshot", section)
        self.assertIn("skip power off", section)
        self.assertIn("never power off the main server", section)
        self.assertIn("auto_delete", section)
        # The old, unsafe sequence must not come back.
        self.assertNotIn("2. power off        shutdown", section)
        self.assertNotIn("3. unassign         POST /primary_ips", section)

    def test_spec_documents_auto_delete_of_non_reserved_ips(self):
        section = _section(SPEC, "### 5.2", "### 5.3")
        self.assertIn("non-reserved", section)
        self.assertIn("auto_delete=true", section)

    def test_readme_order_matches(self):
        self.assertLess(README.index("takes a **live snapshot**"), README.index("prepares the main server's Primary IP"))
        self.assertIn("never powered off", README)


if __name__ == "__main__":
    unittest.main()
