import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "jobs", "spark"))
sys.path.insert(0, os.path.join(ROOT, "generator"))


@pytest.fixture(scope="session")
def spark():
    from common import get_spark

    s = get_spark("tests", shuffle_partitions=2)
    yield s
    s.stop()
