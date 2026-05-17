"""data_updater 配下の Lambda モジュールをインポート可能にする.

predictor / data_updater で同名モジュール (facility_config) を持つので、
各テスト実行前に sys.path と sys.modules を整える。
"""

import sys
from pathlib import Path

import pytest

PREDICTOR_SRC = Path(__file__).resolve().parents[2] / "src" / "predictor"
DATA_UPDATER_SRC = Path(__file__).resolve().parents[2] / "src" / "data_updater"


@pytest.fixture(autouse=True)
def _isolate_data_updater_path():
    sys.path[:] = [p for p in sys.path if p != str(PREDICTOR_SRC)]
    for mod in ("facility_config",):
        sys.modules.pop(mod, None)
    if str(DATA_UPDATER_SRC) not in sys.path:
        sys.path.insert(0, str(DATA_UPDATER_SRC))
    yield
