"""
ミライルSMS契約者処理（当日SMS版・委託先法人ID空白）のテスト

フィルター条件:
1. 委託先法人ID: 空白のみ
2. 回収ランク: 「訴訟中」「弁護士介入」を除外
3. 入金予定日: 当日のみ
4. 入金予定金額: 13円以上
5. クライアントCD: 10, 40, 9268を除外
6. 滞納残債: 1円以上
7. TEL携帯: 090/080/070形式のみ
"""

import pytest
import pandas as pd
from datetime import date, datetime, timedelta
from io import BytesIO

from processors.mirail_sms.contract_today_blank import process_mirail_sms_contract_today_blank_data
from tests.processors.sms.conftest import (
    create_mirail_sms_dataframe,
    dataframe_to_csv_bytes,
)


class TestTrusteeIdBlankFilter:
    """委託先法人IDフィルターのテスト（空白のみ）"""

    def test_blank_trustee_id_passes(self, payment_deadline_date):
        """委託先法人ID=空白のデータは通過する"""
        today = datetime.now().strftime('%Y/%m/%d')
        rows = [{
            'クライアントCD': '1',
            'TEL携帯': '090-1234-5678',
            '滞納残債': '10,000',
            '入金予定日': today,
            '入金予定金額': '100',
            '回収ランク': '通常',
            '委託先法人ID': '',  # 空白
            '契約者氏名': 'テスト太郎',
            '物件名': 'テストマンション',
            '物件番号': '101',
            '管理番号': 'M001',
            '回収口座銀行名': 'テスト銀行',
            '回収口座支店名': 'テスト支店',
            '回収口座種類': '普通',
            '回収口座番号': '1234567',
            '回収口座名義人': 'テスト名義',
        }]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1

    def test_non_blank_trustee_id_excluded(self, payment_deadline_date):
        """委託先法人ID!=空白のデータは除外される"""
        today = datetime.now().strftime('%Y/%m/%d')
        rows = [
            {
                'クライアントCD': '1',
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',
                '入金予定日': today,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '',  # 空白 - 通過
                '契約者氏名': '通過太郎',
                '物件名': 'テストマンション',
                '物件番号': '101',
                '管理番号': 'M001',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
            {
                'クライアントCD': '1',
                'TEL携帯': '090-2222-2222',
                '滞納残債': '10,000',
                '入金予定日': today,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '5',  # 5 - 除外
                '契約者氏名': '除外二郎',
                '物件名': 'テストマンション',
                '物件番号': '102',
                '管理番号': 'M002',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
        ]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1


class TestClientCdFilter:
    """クライアントCDフィルターのテスト（10, 40, 9268を除外）"""

    @pytest.mark.parametrize("excluded_cd", ['10', '40', '9268'])
    def test_excluded_client_cd_filtered(self, excluded_cd, payment_deadline_date):
        """クライアントCD=10, 40, 9268は除外される"""
        today = datetime.now().strftime('%Y/%m/%d')
        rows = [
            {
                'クライアントCD': '1',
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',
                '入金予定日': today,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '',
                '契約者氏名': '通過太郎',
                '物件名': 'テストマンション',
                '物件番号': '101',
                '管理番号': 'M001',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
            {
                'クライアントCD': excluded_cd,
                'TEL携帯': '090-2222-2222',
                '滞納残債': '10,000',
                '入金予定日': today,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '',
                '契約者氏名': '除外二郎',
                '物件名': 'テストマンション',
                '物件番号': '102',
                '管理番号': 'M002',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
        ]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, f"クライアントCD={excluded_cd}は除外されるべき"

    def test_valid_client_cd_passes(self, payment_deadline_date):
        """有効なクライアントCDは通過する"""
        today = datetime.now().strftime('%Y/%m/%d')
        rows = [{
            'クライアントCD': '1',
            'TEL携帯': '090-1234-5678',
            '滞納残債': '10,000',
            '入金予定日': today,
            '入金予定金額': '100',
            '回収ランク': '通常',
            '委託先法人ID': '',
            '契約者氏名': 'テスト太郎',
            '物件名': 'テストマンション',
            '物件番号': '101',
            '管理番号': 'M001',
            '回収口座銀行名': 'テスト銀行',
            '回収口座支店名': 'テスト支店',
            '回収口座種類': '普通',
            '回収口座番号': '1234567',
            '回収口座名義人': 'テスト名義',
        }]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1


class TestPaymentDateFilter:
    """入金予定日フィルターのテスト（当日のみ）"""

    def test_today_date_passes(self, payment_deadline_date):
        """入金予定日=今日のデータは通過する"""
        today = datetime.now().strftime('%Y/%m/%d')
        rows = [{
            'クライアントCD': '1',
            'TEL携帯': '090-1234-5678',
            '滞納残債': '10,000',
            '入金予定日': today,
            '入金予定金額': '100',
            '回収ランク': '通常',
            '委託先法人ID': '',
            '契約者氏名': 'テスト太郎',
            '物件名': 'テストマンション',
            '物件番号': '101',
            '管理番号': 'M001',
            '回収口座銀行名': 'テスト銀行',
            '回収口座支店名': 'テスト支店',
            '回収口座種類': '普通',
            '回収口座番号': '1234567',
            '回収口座名義人': 'テスト名義',
        }]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1

    def test_yesterday_date_excluded(self, payment_deadline_date):
        """入金予定日=昨日のデータは除外される"""
        today = datetime.now().strftime('%Y/%m/%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
        rows = [
            {
                'クライアントCD': '1',
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',
                '入金予定日': today,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '',
                '契約者氏名': '通過太郎',
                '物件名': 'テストマンション',
                '物件番号': '101',
                '管理番号': 'M001',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
            {
                'クライアントCD': '1',
                'TEL携帯': '090-2222-2222',
                '滞納残債': '10,000',
                '入金予定日': yesterday,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '',
                '契約者氏名': '除外二郎',
                '物件名': 'テストマンション',
                '物件番号': '102',
                '管理番号': 'M002',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
        ]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, "昨日の日付のデータは除外されるべき"

    def test_future_date_excluded(self, payment_deadline_date):
        """入金予定日=明日以降のデータは除外される"""
        today = datetime.now().strftime('%Y/%m/%d')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y/%m/%d')
        rows = [
            {
                'クライアントCD': '1',
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',
                '入金予定日': today,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '',
                '契約者氏名': '通過太郎',
                '物件名': 'テストマンション',
                '物件番号': '101',
                '管理番号': 'M001',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
            {
                'クライアントCD': '1',
                'TEL携帯': '090-2222-2222',
                '滞納残債': '10,000',
                '入金予定日': tomorrow,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '',
                '契約者氏名': '除外二郎',
                '物件名': 'テストマンション',
                '物件番号': '102',
                '管理番号': 'M002',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
        ]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, "未来の日付のデータは除外されるべき"


class TestPaymentAmountFilter:
    """入金予定金額フィルターのテスト（13円以上）"""

    def test_amount_13_passes(self, payment_deadline_date):
        """入金予定金額=13円は通過する"""
        today = datetime.now().strftime('%Y/%m/%d')
        rows = [{
            'クライアントCD': '1',
            'TEL携帯': '090-1234-5678',
            '滞納残債': '10,000',
            '入金予定日': today,
            '入金予定金額': '13',
            '回収ランク': '通常',
            '委託先法人ID': '',
            '契約者氏名': 'テスト太郎',
            '物件名': 'テストマンション',
            '物件番号': '101',
            '管理番号': 'M001',
            '回収口座銀行名': 'テスト銀行',
            '回収口座支店名': 'テスト支店',
            '回収口座種類': '普通',
            '回収口座番号': '1234567',
            '回収口座名義人': 'テスト名義',
        }]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1

    def test_amount_with_comma_passes(self, payment_deadline_date):
        """入金予定金額=カンマ区切り（54,410）も正しく処理される"""
        today = datetime.now().strftime('%Y/%m/%d')
        rows = [{
            'クライアントCD': '1',
            'TEL携帯': '090-1234-5678',
            '滞納残債': '10,000',
            '入金予定日': today,
            '入金予定金額': '54,410',  # カンマ区切り
            '回収ランク': '通常',
            '委託先法人ID': '',
            '契約者氏名': 'テスト太郎',
            '物件名': 'テストマンション',
            '物件番号': '101',
            '管理番号': 'M001',
            '回収口座銀行名': 'テスト銀行',
            '回収口座支店名': 'テスト支店',
            '回収口座種類': '普通',
            '回収口座番号': '1234567',
            '回収口座名義人': 'テスト名義',
        }]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, "カンマ区切り金額も正しく処理されるべき"

    @pytest.mark.parametrize("excluded_amount", [0, 1, 12])
    def test_amount_below_13_excluded(self, excluded_amount, payment_deadline_date):
        """入金予定金額<13円は除外される"""
        today = datetime.now().strftime('%Y/%m/%d')
        rows = [
            {
                'クライアントCD': '1',
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',
                '入金予定日': today,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '',
                '契約者氏名': '通過太郎',
                '物件名': 'テストマンション',
                '物件番号': '101',
                '管理番号': 'M001',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
            {
                'クライアントCD': '1',
                'TEL携帯': '090-2222-2222',
                '滞納残債': '10,000',
                '入金予定日': today,
                '入金予定金額': str(excluded_amount),
                '回収ランク': '通常',
                '委託先法人ID': '',
                '契約者氏名': '除外二郎',
                '物件名': 'テストマンション',
                '物件番号': '102',
                '管理番号': 'M002',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
        ]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, f"入金予定金額={excluded_amount}は除外されるべき"


class TestCollectionRankFilter:
    """回収ランクフィルターのテスト（弁護士介入、訴訟中を除外）"""

    @pytest.mark.parametrize("excluded_rank", ["弁護士介入", "訴訟中"])
    def test_excluded_ranks_filtered(self, excluded_rank, payment_deadline_date):
        """回収ランク=弁護士介入/訴訟中は除外される"""
        today = datetime.now().strftime('%Y/%m/%d')
        rows = [
            {
                'クライアントCD': '1',
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',
                '入金予定日': today,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '',
                '契約者氏名': '通過太郎',
                '物件名': 'テストマンション',
                '物件番号': '101',
                '管理番号': 'M001',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
            {
                'クライアントCD': '1',
                'TEL携帯': '090-2222-2222',
                '滞納残債': '10,000',
                '入金予定日': today,
                '入金予定金額': '100',
                '回収ランク': excluded_rank,
                '委託先法人ID': '',
                '契約者氏名': '除外二郎',
                '物件名': 'テストマンション',
                '物件番号': '102',
                '管理番号': 'M002',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
        ]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, f"回収ランク={excluded_rank}は除外されるべき"


class TestArrearsFilter:
    """滞納残債フィルターのテスト（1円以上）"""

    def test_zero_arrears_excluded(self, payment_deadline_date):
        """滞納残債=0は除外される"""
        today = datetime.now().strftime('%Y/%m/%d')
        rows = [
            {
                'クライアントCD': '1',
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',
                '入金予定日': today,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '',
                '契約者氏名': '通過太郎',
                '物件名': 'テストマンション',
                '物件番号': '101',
                '管理番号': 'M001',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
            {
                'クライアントCD': '1',
                'TEL携帯': '090-2222-2222',
                '滞納残債': '0',
                '入金予定日': today,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '',
                '契約者氏名': '除外二郎',
                '物件名': 'テストマンション',
                '物件番号': '102',
                '管理番号': 'M002',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
        ]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, "滞納残債=0は除外されるべき"


class TestMobilePhoneFilter:
    """携帯電話番号フィルターのテスト（090/080/070形式のみ）"""

    @pytest.mark.parametrize("valid_phone", ['090-1234-5678', '080-1234-5678', '070-1234-5678'])
    def test_valid_mobile_phones_pass(self, valid_phone, payment_deadline_date):
        """有効な携帯電話番号は通過する"""
        today = datetime.now().strftime('%Y/%m/%d')
        rows = [{
            'クライアントCD': '1',
            'TEL携帯': valid_phone,
            '滞納残債': '10,000',
            '入金予定日': today,
            '入金予定金額': '100',
            '回収ランク': '通常',
            '委託先法人ID': '',
            '契約者氏名': 'テスト太郎',
            '物件名': 'テストマンション',
            '物件番号': '101',
            '管理番号': 'M001',
            '回収口座銀行名': 'テスト銀行',
            '回収口座支店名': 'テスト支店',
            '回収口座種類': '普通',
            '回収口座番号': '1234567',
            '回収口座名義人': 'テスト名義',
        }]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1

    @pytest.mark.parametrize("invalid_phone", ['03-1234-5678', '050-1234-5678', ''])
    def test_invalid_phones_excluded(self, invalid_phone, payment_deadline_date):
        """無効な電話番号は除外される"""
        today = datetime.now().strftime('%Y/%m/%d')
        rows = [
            {
                'クライアントCD': '1',
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',
                '入金予定日': today,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '',
                '契約者氏名': '通過太郎',
                '物件名': 'テストマンション',
                '物件番号': '101',
                '管理番号': 'M001',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
            {
                'クライアントCD': '1',
                'TEL携帯': invalid_phone,
                '滞納残債': '10,000',
                '入金予定日': today,
                '入金予定金額': '100',
                '回収ランク': '通常',
                '委託先法人ID': '',
                '契約者氏名': '除外二郎',
                '物件名': 'テストマンション',
                '物件番号': '102',
                '管理番号': 'M002',
                '回収口座銀行名': 'テスト銀行',
                '回収口座支店名': 'テスト支店',
                '回収口座種類': '普通',
                '回収口座番号': '1234567',
                '回収口座名義人': 'テスト名義',
            },
        ]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, f"電話番号={invalid_phone}は除外されるべき"


class TestOutputFilename:
    """出力ファイル名のテスト"""

    def test_filename_contains_today_blank_suffix(self, payment_deadline_date):
        """ファイル名に「当日ID空白」が含まれる"""
        today = datetime.now().strftime('%Y/%m/%d')
        rows = [{
            'クライアントCD': '1',
            'TEL携帯': '090-1234-5678',
            '滞納残債': '10,000',
            '入金予定日': today,
            '入金予定金額': '100',
            '回収ランク': '通常',
            '委託先法人ID': '',
            '契約者氏名': 'テスト太郎',
            '物件名': 'テストマンション',
            '物件番号': '101',
            '管理番号': 'M001',
            '回収口座銀行名': 'テスト銀行',
            '回収口座支店名': 'テスト支店',
            '回収口座種類': '普通',
            '回収口座番号': '1234567',
            '回収口座名義人': 'テスト名義',
        }]
        df = create_mirail_sms_dataframe(rows)
        csv_bytes = dataframe_to_csv_bytes(df)

        result_df, logs, filename, stats = process_mirail_sms_contract_today_blank_data(
            csv_bytes, payment_deadline_date
        )

        assert '当日ID空白' in filename
        assert filename.endswith('.csv')
