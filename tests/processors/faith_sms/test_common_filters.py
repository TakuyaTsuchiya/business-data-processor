"""
フェイスSMS共通フィルタテスト

3つのプロセッサ（契約者・保証人・緊急連絡人）で共通の
フィルタ1〜5およびビジネスルール整合性をパラメータ化してテストする。
"""

import pytest
from datetime import datetime, timedelta

from processors.faith_sms.contract import process_faith_sms_contract_data
from processors.faith_sms.guarantor import process_faith_sms_guarantor_data
from processors.faith_sms.emergency_contact import (
    process_faith_sms_emergencycontact_data,
)
from domain.rules.business_rules import CLIENT_IDS

from tests.processors.faith_sms.conftest import (
    create_faith_sms_dataframe,
    dataframe_to_csv_bytes,
    create_valid_faith_row,
)


def _run_processor(processor_func, rows, payment_deadline_date):
    df = create_faith_sms_dataframe(rows)
    csv_bytes = dataframe_to_csv_bytes(df)
    return processor_func(csv_bytes, payment_deadline_date)


ALL_PROCESSORS = [
    process_faith_sms_contract_data,
    process_faith_sms_guarantor_data,
    process_faith_sms_emergencycontact_data,
]


@pytest.fixture(params=ALL_PROCESSORS, ids=["contract", "guarantor", "emergency"])
def processor(request):
    return request.param


class TestTrusteeIdFilter:
    """委託先法人IDフィルター（faith: [1, 2, 3, 4, 8]）"""

    @pytest.mark.parametrize("valid_id", ["1", "2", "3", "4", "8"])
    def test_valid_ids_accepted(self, valid_id, processor, payment_deadline_date):
        rows = [create_valid_faith_row(委託先法人ID=valid_id)]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1

    @pytest.mark.parametrize("invalid_id", ["0", "5", "6", "7", "99"])
    def test_invalid_ids_rejected(self, invalid_id, processor, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 委託先法人ID="1"),
            create_valid_faith_row(契約者氏名="無効", 委託先法人ID=invalid_id),
        ]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_empty_and_nan_rejected(self, processor, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 委託先法人ID="1"),
            create_valid_faith_row(契約者氏名="空白", 委託先法人ID=""),
            create_valid_faith_row(契約者氏名="NaN", 委託先法人ID="nan"),
        ]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1


class TestPaymentDateFilter:
    """入金予定日フィルター（空白 or 今日より前のみ通過）"""

    def test_past_date_accepted(self, processor, payment_deadline_date):
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")
        rows = [create_valid_faith_row(入金予定日=yesterday)]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_empty_date_accepted(self, processor, payment_deadline_date):
        rows = [create_valid_faith_row(入金予定日="")]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_today_excluded(self, processor, payment_deadline_date):
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")
        today = datetime.now().strftime("%Y/%m/%d")
        rows = [
            create_valid_faith_row(契約者氏名="有効", 入金予定日=yesterday),
            create_valid_faith_row(契約者氏名="除外", 入金予定日=today),
        ]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_future_date_excluded(self, processor, payment_deadline_date):
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y/%m/%d")
        rows = [
            create_valid_faith_row(契約者氏名="有効", 入金予定日=yesterday),
            create_valid_faith_row(契約者氏名="除外", 入金予定日=tomorrow),
        ]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1


class TestPaymentAmountFilter:
    """入金予定金額フィルター（faith: [2, 3, 5]を除外）"""

    @pytest.mark.parametrize("excluded_amount", ["2", "3", "5"])
    def test_excluded_amounts(self, excluded_amount, processor, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 入金予定金額="1"),
            create_valid_faith_row(契約者氏名="除外", 入金予定金額=excluded_amount),
        ]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_normal_amount_accepted(self, processor, payment_deadline_date):
        rows = [create_valid_faith_row(入金予定金額="1")]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1


class TestCollectionRankFilter:
    """回収ランクフィルター（弁護士介入, 破産決定, 死亡決定を除外）"""

    @pytest.mark.parametrize("excluded_rank", ["弁護士介入", "破産決定", "死亡決定"])
    def test_excluded_ranks(self, excluded_rank, processor, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 回収ランク="通常"),
            create_valid_faith_row(契約者氏名="除外", 回収ランク=excluded_rank),
        ]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_normal_rank_accepted(self, processor, payment_deadline_date):
        rows = [create_valid_faith_row(回収ランク="通常")]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1


class TestArrearsFilter:
    """滞納残債フィルター（iloc[:, 71] >= 1）"""

    def test_zero_excluded(self, processor, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 滞納残債="10,000"),
            create_valid_faith_row(契約者氏名="除外", 滞納残債="0"),
        ]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_positive_accepted(self, processor, payment_deadline_date):
        rows = [create_valid_faith_row(滞納残債="5000")]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_comma_formatted_amount_accepted(self, processor, payment_deadline_date):
        rows = [create_valid_faith_row(滞納残債="100,000")]
        result_df, _, _, _ = _run_processor(processor, rows, payment_deadline_date)
        assert len(result_df) == 1


class TestMixedData:
    """複数パターンのデータが正しくフィルタリングされることを検証"""

    def test_correct_filtering_with_mixed_data(self, processor, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="通過太郎"),
            create_valid_faith_row(契約者氏名="除外二郎", 委託先法人ID="99"),
            create_valid_faith_row(契約者氏名="除外三郎", 回収ランク="弁護士介入"),
            create_valid_faith_row(契約者氏名="通過四郎", 委託先法人ID="8"),
        ]
        result_df, _, _, stats = _run_processor(processor, rows, payment_deadline_date)

        assert len(result_df) == 2
        assert stats["initial_rows"] == 4
        assert stats["processed_rows"] == 2


class TestBusinessRulesConsistency:
    """ビジネスルールの定数がプロセッサの期待値と一致することを検証"""

    def test_faith_client_ids_match_expected(self):
        assert CLIENT_IDS["faith"] == [1, 2, 3, 4, 8]
