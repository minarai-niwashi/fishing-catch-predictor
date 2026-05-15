"""predictor 配下の Lambda モジュールをインポート可能にする."""

import sys
from pathlib import Path

PREDICTOR_SRC = Path(__file__).resolve().parents[2] / "src" / "predictor"
if str(PREDICTOR_SRC) not in sys.path:
    sys.path.insert(0, str(PREDICTOR_SRC))
