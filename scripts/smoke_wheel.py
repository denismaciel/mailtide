"""Smoke check for a built mailtide wheel."""

from __future__ import annotations

import sys
import sysconfig

import mailtide
from mailtide.config import Blobs, Config, Database, Poll

assert hasattr(mailtide, "Runner")
assert Config(database=Database("sqlite::memory:"), blobs=Blobs("memory:///"), watch=Poll())

if sysconfig.get_config_var("Py_GIL_DISABLED"):
    assert not sys._is_gil_enabled(), "importing mailtide re-enabled the GIL"

print("mailtide wheel smoke check passed")
