"""data_loader.S3DataLoader が日付を日本時間 (JST) で扱うことのテスト."""

import io
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def data_loader_module():
    import data_loader  # noqa: PLC0415 — autouse fixture が sys.path を整えた後でロード
    return data_loader


def _frozen_datetime_class(fixed_utc):
    """datetime.now() が常に fixed_utc を返すクラスを作る."""
    from datetime import datetime as _real_datetime  # noqa: PLC0415

    class _Frozen(_real_datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_utc.astimezone(tz) if tz is not None else fixed_utc.replace(tzinfo=None)

    return _Frozen


def test_load_historical_data_cutoff_uses_jst_today(data_loader_module, monkeypatch):
    # 日本が 7/2 の朝 6:00 の瞬間を再現する (このとき UTC はまだ 7/1)。
    # days=5 なので、JST の今日 7/2 から 5 日前 = 6/27 より新しいデータだけ残るはず。
    # UTC で計算すると今日を 7/1 と誤認し、5 日前が 6/26 になって 6/26 を余分に含めてしまう。
    fixed_utc = datetime(2026, 7, 1, 21, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(data_loader_module, "datetime", _frozen_datetime_class(fixed_utc))

    dates = [
        "2026-06-25", "2026-06-26", "2026-06-27",
        "2026-06-28", "2026-06-29", "2026-06-30", "2026-07-01",
    ]
    csv = "date,aji_count,visitors\n" + "".join(f"{d},100,200\n" for d in dates)

    loader = data_loader_module.S3DataLoader(bucket_name="b")
    loader.s3_client = MagicMock()
    loader.s3_client.get_object.return_value = {"Body": io.BytesIO(csv.encode("utf-8"))}

    df = loader.load_historical_data(facility="honmoku", days=5)

    kept = sorted(d.strftime("%Y-%m-%d") for d in df["date"])
    # JST cutoff 06-27 以降のみ (06-27 .. 07-01 の 5 日)
    assert kept == [
        "2026-06-27",
        "2026-06-28",
        "2026-06-29",
        "2026-06-30",
        "2026-07-01",
    ]
    assert "2026-06-26" not in kept  # UTC で計算すると混入していた行
