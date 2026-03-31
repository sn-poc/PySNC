import pytest
import os


def pytest_collection_modifyitems(config, items):
    if not os.environ.get('PYSNC_SERVER'):
        skip_online = pytest.mark.skip(reason="PYSNC_SERVER not set, skipping online tests")
        for item in items:
            if "offline" not in item.keywords:
                item.add_marker(skip_online)
