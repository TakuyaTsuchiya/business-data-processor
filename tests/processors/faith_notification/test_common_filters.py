"""
apply_common_filters のテスト
"""

import pandas as pd
from datetime import timedelta

from processors.faith_notification import apply_common_filters
from tests.processors.faith_notification.conftest import (
    COL,
    create_notification_dataframe,
    base_valid_row_data,
)


class TestTrusteeIdFilter:
    """委託先法人IDフィルタのテスト"""

    def test_valid_trustee_ids_accepted(self):
        """有効な委託先法人ID(1,2,3,4,8)はすべて通過する"""
        rows = []
        for tid in [1, 2, 3, 4, 8]:
            row = base_valid_row_data()
            row[COL["TRUSTEE_ID"]] = tid
            row[COL["MANAGEMENT_NO"]] = f"MGT-{tid}"
            rows.append(row)
        df = create_notification_dataframe(rows)
        result, logs = apply_common_filters(df)
        assert len(result) == 5

    def test_invalid_trustee_ids_rejected(self):
        """無効な委託先法人ID(0,5,6,7,99)は除外される"""
        rows = []
        for tid in [0, 5, 6, 7, 99]:
            row = base_valid_row_data()
            row[COL["TRUSTEE_ID"]] = tid
            rows.append(row)
        df = create_notification_dataframe(rows)
        result, logs = apply_common_filters(df)
        assert len(result) == 0


class TestPaymentDateFilter:
    """入金予定日フィルタのテスト"""

    def test_payment_date_past_accepted(self):
        """過去の日付は通過する"""
        row = base_valid_row_data()
        yesterday = (pd.Timestamp.now().normalize() - timedelta(days=1)).strftime(
            "%Y/%m/%d"
        )
        row[COL["PAYMENT_DATE"]] = yesterday
        df = create_notification_dataframe([row])
        result, _ = apply_common_filters(df)
        assert len(result) == 1

    def test_payment_date_empty_accepted(self):
        """空欄(NaT)は通過する"""
        row = base_valid_row_data()
        row[COL["PAYMENT_DATE"]] = ""
        df = create_notification_dataframe([row])
        result, _ = apply_common_filters(df)
        assert len(result) == 1

    def test_payment_date_today_excluded(self):
        """本日の日付は除外される"""
        row = base_valid_row_data()
        row[COL["PAYMENT_DATE"]] = pd.Timestamp.now().normalize().strftime("%Y/%m/%d")
        df = create_notification_dataframe([row])
        result, _ = apply_common_filters(df)
        assert len(result) == 0

    def test_payment_date_future_excluded(self):
        """未来の日付は除外される"""
        row = base_valid_row_data()
        tomorrow = (pd.Timestamp.now().normalize() + timedelta(days=1)).strftime(
            "%Y/%m/%d"
        )
        row[COL["PAYMENT_DATE"]] = tomorrow
        df = create_notification_dataframe([row])
        result, _ = apply_common_filters(df)
        assert len(result) == 0


class TestPaymentAmountFilter:
    """入金予定金額フィルタのテスト"""

    def test_payment_amount_excluded(self):
        """金額2,3,5は除外される"""
        rows = []
        for amt in [2, 3, 5]:
            row = base_valid_row_data()
            row[COL["PAYMENT_AMOUNT"]] = amt
            rows.append(row)
        df = create_notification_dataframe(rows)
        result, _ = apply_common_filters(df)
        assert len(result) == 0

    def test_payment_amount_normal_accepted(self):
        """通常の金額(1,4,6,100等)は通過する"""
        rows = []
        for amt in [1, 4, 6, 100]:
            row = base_valid_row_data()
            row[COL["PAYMENT_AMOUNT"]] = amt
            rows.append(row)
        df = create_notification_dataframe(rows)
        result, _ = apply_common_filters(df)
        assert len(result) == 4


class TestCollectionRankFilter:
    """回収ランクフィルタのテスト"""

    def test_collection_rank_excluded(self):
        """死亡決定と弁護士介入は除外される"""
        rows = []
        for rank in ["死亡決定", "弁護士介入"]:
            row = base_valid_row_data()
            row[COL["COLLECTION_RANK"]] = rank
            rows.append(row)
        df = create_notification_dataframe(rows)
        result, _ = apply_common_filters(df)
        assert len(result) == 0

    def test_collection_rank_normal_accepted(self):
        """通常ランクは通過する"""
        row = base_valid_row_data()
        row[COL["COLLECTION_RANK"]] = "通常"
        df = create_notification_dataframe([row])
        result, _ = apply_common_filters(df)
        assert len(result) == 1

    def test_collection_rank_skip_when_flag_true(self):
        """skip_rank_filter=Trueの場合、死亡決定も通過する"""
        row = base_valid_row_data()
        row[COL["COLLECTION_RANK"]] = "死亡決定"
        df = create_notification_dataframe([row])
        result, _ = apply_common_filters(df, skip_rank_filter=True)
        assert len(result) == 1


class TestArrearsFilter:
    """滞納残債フィルタのテスト"""

    def test_arrears_positive_accepted(self):
        """カンマ付き正数(10,000)は通過する"""
        row = base_valid_row_data()
        row[COL["DEBT_AMOUNT"]] = "10,000"
        df = create_notification_dataframe([row])
        result, _ = apply_common_filters(df)
        assert len(result) == 1

    def test_arrears_zero_excluded(self):
        """0は除外される"""
        row = base_valid_row_data()
        row[COL["DEBT_AMOUNT"]] = "0"
        df = create_notification_dataframe([row])
        result, _ = apply_common_filters(df)
        assert len(result) == 0

    def test_arrears_negative_excluded(self):
        """負数(-100)は除外される"""
        row = base_valid_row_data()
        row[COL["DEBT_AMOUNT"]] = "-100"
        df = create_notification_dataframe([row])
        result, _ = apply_common_filters(df)
        assert len(result) == 0


class TestCombinedFilters:
    """複合フィルタのテスト"""

    def test_all_filters_combined(self):
        """複数行で、有効な行のみが残る"""
        tomorrow = (pd.Timestamp.now().normalize() + timedelta(days=1)).strftime(
            "%Y/%m/%d"
        )

        # 有効な行
        valid = base_valid_row_data()
        valid[COL["MANAGEMENT_NO"]] = "VALID"

        # 無効: 委託先法人ID不正
        bad_id = base_valid_row_data()
        bad_id[COL["TRUSTEE_ID"]] = 99
        bad_id[COL["MANAGEMENT_NO"]] = "BAD_ID"

        # 無効: 入金予定日が未来
        bad_date = base_valid_row_data()
        bad_date[COL["PAYMENT_DATE"]] = tomorrow
        bad_date[COL["MANAGEMENT_NO"]] = "BAD_DATE"

        # 無効: 入金予定金額が除外対象
        bad_amt = base_valid_row_data()
        bad_amt[COL["PAYMENT_AMOUNT"]] = 3
        bad_amt[COL["MANAGEMENT_NO"]] = "BAD_AMT"

        # 無効: 回収ランクが除外対象
        bad_rank = base_valid_row_data()
        bad_rank[COL["COLLECTION_RANK"]] = "弁護士介入"
        bad_rank[COL["MANAGEMENT_NO"]] = "BAD_RANK"

        # 無効: 滞納残債が0
        bad_debt = base_valid_row_data()
        bad_debt[COL["DEBT_AMOUNT"]] = "0"
        bad_debt[COL["MANAGEMENT_NO"]] = "BAD_DEBT"

        df = create_notification_dataframe(
            [valid, bad_id, bad_date, bad_amt, bad_rank, bad_debt]
        )
        result, _ = apply_common_filters(df)
        assert len(result) == 1
        assert result.iloc[0, COL["MANAGEMENT_NO"]] == "VALID"
