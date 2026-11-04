"""hetzner-deadman — a fault-tolerant dead man's switch for Hetzner Cloud.

The process normally runs *on the main server itself*.  When the operator stops
checking in for the configured grace period it takes a final live snapshot,
preserves the main Primary IP, and deletes the project's servers / IPs — the
main server last, which also terminates this software.
"""

__version__ = "1.0.0"
__all__ = ["__version__"]
