"""
apply_occupancy_filters のテスト
"""

from processors.faith_notification import apply_occupancy_filters
from tests.processors.faith_notification.conftest import (
    COL,
    create_notification_dataframe,
    base_valid_row_data,
)


def _make_row(occupancy, rank, mgmt_no="MGT001"):
    """入居ステータスと回収ランクを指定した行データを作成"""
    row = base_valid_row_data()
    row[COL["RESIDENCE_STATUS"]] = occupancy
    row[COL["COLLECTION_RANK"]] = rank
    row[COL["MANAGEMENT_NO"]] = mgmt_no
    return row


class TestLitigationOnly:
    """litigation_only フィルタのテスト"""

    def test_litigation_only_keeps_only_litigation(self):
        """入居中 + litigation_only: 訴訟中のみ残る"""
        rows = [
            _make_row("入居中", "訴訟中", "KEEP"),
            _make_row("入居中", "通常", "REMOVE1"),
            _make_row("入居中", "破産決定", "REMOVE2"),
        ]
        df = create_notification_dataframe(rows)
        result, _ = apply_occupancy_filters(df, "入居中", "litigation_only")
        assert len(result) == 1
        assert result.iloc[0, COL["MANAGEMENT_NO"]] == "KEEP"


class TestLitigationExcluded:
    """litigation_excluded フィルタのテスト"""

    def test_litigation_excluded_removes_all_bad_ranks(self):
        """入居中 + litigation_excluded: 4つの除外ランクを除外"""
        rows = [
            _make_row("入居中", "通常", "KEEP"),
            _make_row("入居中", "破産決定", "RM1"),
            _make_row("入居中", "死亡決定", "RM2"),
            _make_row("入居中", "弁護士介入", "RM3"),
            _make_row("入居中", "訴訟中", "RM4"),
        ]
        df = create_notification_dataframe(rows)
        result, _ = apply_occupancy_filters(df, "入居中", "litigation_excluded")
        assert len(result) == 1
        assert result.iloc[0, COL["MANAGEMENT_NO"]] == "KEEP"


class TestEvicted:
    """evicted フィルタのテスト"""

    def test_evicted_removes_three_bad_ranks(self):
        """退去済 + evicted: 3つの除外ランクを除外"""
        rows = [
            _make_row("退去済", "通常", "KEEP1"),
            _make_row("退去済", "訴訟中", "KEEP2"),
            _make_row("退去済", "死亡決定", "RM1"),
            _make_row("退去済", "破産決定", "RM2"),
            _make_row("退去済", "弁護士介入", "RM3"),
        ]
        df = create_notification_dataframe(rows)
        result, _ = apply_occupancy_filters(df, "退去済", "evicted")
        assert len(result) == 2
        mgmt_nos = result.iloc[:, COL["MANAGEMENT_NO"]].tolist()
        assert "KEEP1" in mgmt_nos
        assert "KEEP2" in mgmt_nos


class TestOccupancyStatusFilter:
    """入居ステータスフィルタのテスト"""

    def test_occupancy_status_filters_correctly(self):
        """指定したステータスのみが残る"""
        rows = [
            _make_row("入居中", "通常", "KEEP"),
            _make_row("退去済", "通常", "REMOVE"),
        ]
        df = create_notification_dataframe(rows)
        result, _ = apply_occupancy_filters(df, "入居中", "litigation_excluded")
        assert len(result) == 1
        assert result.iloc[0, COL["MANAGEMENT_NO"]] == "KEEP"

    def test_mixed_status_and_ranks(self):
        """複合条件: ステータスとランクの複合フィルタ"""
        rows = [
            _make_row("入居中", "通常", "KEEP1"),
            _make_row("入居中", "訴訟中", "RM_RANK"),
            _make_row("退去済", "通常", "RM_STATUS"),
            _make_row("入居中", "破産決定", "RM_BOTH"),
        ]
        df = create_notification_dataframe(rows)
        result, _ = apply_occupancy_filters(df, "入居中", "litigation_excluded")
        assert len(result) == 1
        assert result.iloc[0, COL["MANAGEMENT_NO"]] == "KEEP1"
