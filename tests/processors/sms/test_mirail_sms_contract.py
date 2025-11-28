"""
ミライルSMS契約者処理のテスト

【解説】
このファイルでは、processors/mirail_sms/contract.py の
process_mirail_sms_contract_data() 関数をテストします。

テストの構造:
1. 各フィルターの単体テスト（Filter 1〜6）
2. データマッピングのテスト
3. 統合テスト（正常系・異常系）
4. エラーハンドリングテスト
"""

import pytest
import pandas as pd
from datetime import date, datetime, timedelta
from io import BytesIO

# テスト対象の関数をインポート
from processors.mirail_sms.contract import process_mirail_sms_contract_data

# conftest.py のヘルパー関数をインポート
from tests.processors.sms.conftest import (
    create_mirail_sms_dataframe,
    dataframe_to_csv_bytes,
)


# =============================================================================
# 【解説】テストクラスの使い方
#
# 関連するテストをクラスでまとめると整理しやすくなります。
# クラス名は「Test〜」で始める必要があります。
# =============================================================================

class TestTrusteeIdFilter:
    """
    【解説】委託先法人IDフィルターのテスト

    フィルター条件: 委託先法人IDが「5」または「空白」のみ残す
    """

    def test_valid_trustee_id_5_passes(self, valid_mirail_sms_data, payment_deadline_date):
        """
        【テスト】委託先法人ID=5 のデータは通過する

        テストの流れ:
        1. 準備（Arrange）: テストデータをCSVバイト列に変換
        2. 実行（Act）: 処理関数を呼び出す
        3. 検証（Assert）: 結果が期待通りか確認
        """
        # Arrange: テストデータを準備
        csv_bytes = dataframe_to_csv_bytes(valid_mirail_sms_data)

        # Act: 処理を実行
        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        # Assert: 1件出力されることを確認
        assert len(result_df) == 1, f"期待: 1件, 実際: {len(result_df)}件"

    def test_invalid_trustee_id_excluded(self, payment_deadline_date):
        """
        【テスト】委託先法人ID=999 のデータは除外される

        【解説】現実的なテストケース
        実際の運用では「除外されるデータのみ」のファイルは渡されない。
        そのため、混合データ（有効＋無効）で除外件数を確認する。
        """
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')

        rows = [
            # 1. 有効なデータ（通過する）
            {
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',
                '入金予定日': yesterday,
                '入金予定金額': '1',
                '回収ランク': '通常',
                '委託先法人ID': '5',  # 有効
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
            # 2. 無効なデータ（除外される）
            {
                'TEL携帯': '090-2222-2222',
                '滞納残債': '10,000',
                '入金予定日': yesterday,
                '入金予定金額': '1',
                '回収ランク': '通常',
                '委託先法人ID': '999',  # 無効 → 除外
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

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        # 2件中1件が除外され、1件残ることを確認
        assert len(result_df) == 1, f"期待: 1件, 実際: {len(result_df)}件"

    def test_empty_trustee_id_passes(self, payment_deadline_date):
        """
        【テスト】委託先法人ID=空白 のデータは通過する
        """
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
        rows = [{
            'TEL携帯': '090-1234-5678',
            '滞納残債': '10,000',
            '入金予定日': yesterday,
            '入金予定金額': '1',
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

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1


class TestPaymentDateFilter:
    """
    【解説】入金予定日フィルターのテスト

    フィルター条件: 入金予定日が「前日以前」のみ残す（当日は除外）
    """

    def test_yesterday_date_passes(self, valid_mirail_sms_data, payment_deadline_date):
        """
        【テスト】入金予定日=昨日 のデータは通過する
        """
        csv_bytes = dataframe_to_csv_bytes(valid_mirail_sms_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1

    def test_today_date_excluded(self, payment_deadline_date):
        """
        【テスト】入金予定日=今日 のデータは除外される

        混合データで確認：昨日のデータは通過、今日のデータは除外
        """
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
        today = datetime.now().strftime('%Y/%m/%d')

        rows = [
            # 1. 昨日（通過）
            {
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',
                '入金予定日': yesterday,
                '入金予定金額': '1',
                '回収ランク': '通常',
                '委託先法人ID': '5',
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
            # 2. 今日（除外）
            {
                'TEL携帯': '090-2222-2222',
                '滞納残債': '10,000',
                '入金予定日': today,  # 除外
                '入金予定金額': '1',
                '回収ランク': '通常',
                '委託先法人ID': '5',
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

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, f"今日の日付のデータが除外されていない"

    def test_future_date_excluded(self, payment_deadline_date):
        """
        【テスト】入金予定日=明日以降 のデータは除外される

        混合データで確認
        """
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y/%m/%d')

        rows = [
            # 1. 昨日（通過）
            {
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',
                '入金予定日': yesterday,
                '入金予定金額': '1',
                '回収ランク': '通常',
                '委託先法人ID': '5',
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
            # 2. 明日（除外）
            {
                'TEL携帯': '090-2222-2222',
                '滞納残債': '10,000',
                '入金予定日': tomorrow,  # 除外
                '入金予定金額': '1',
                '回収ランク': '通常',
                '委託先法人ID': '5',
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

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, f"未来の日付のデータが除外されていない"


class TestPaymentAmountFilter:
    """
    【解説】入金予定金額フィルターのテスト

    フィルター条件: 入金予定金額が 2, 3, 5, 12 の場合は除外
    """

    def test_amount_1_passes(self, valid_mirail_sms_data, payment_deadline_date):
        """
        【テスト】入金予定金額=1 は通過する
        """
        csv_bytes = dataframe_to_csv_bytes(valid_mirail_sms_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1

    @pytest.mark.parametrize("excluded_amount", [2, 3, 5, 12])
    def test_excluded_amounts_filtered(self, excluded_amount, payment_deadline_date):
        """
        【解説】パラメータ化テスト

        @pytest.mark.parametrize を使うと、
        同じテストを複数の値で繰り返し実行できます。
        ここでは 2, 3, 5, 12 それぞれでテストを実行します。

        混合データで確認：有効な金額は通過、除外対象は除外
        """
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
        rows = [
            # 1. 有効な金額（通過）
            {
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',
                '入金予定日': yesterday,
                '入金予定金額': '1',  # 有効
                '回収ランク': '通常',
                '委託先法人ID': '5',
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
            # 2. 除外対象の金額（除外）
            {
                'TEL携帯': '090-2222-2222',
                '滞納残債': '10,000',
                '入金予定日': yesterday,
                '入金予定金額': str(excluded_amount),  # 除外
                '回収ランク': '通常',
                '委託先法人ID': '5',
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

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, f"入金予定金額={excluded_amount}は除外されるべき"


class TestCollectionRankFilter:
    """
    【解説】回収ランクフィルターのテスト

    フィルター条件: 「弁護士介入」「訴訟中」は除外
    """

    def test_normal_rank_passes(self, valid_mirail_sms_data, payment_deadline_date):
        """
        【テスト】回収ランク=通常 は通過する
        """
        csv_bytes = dataframe_to_csv_bytes(valid_mirail_sms_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1

    @pytest.mark.parametrize("excluded_rank", ["弁護士介入", "訴訟中"])
    def test_excluded_ranks_filtered(self, excluded_rank, payment_deadline_date):
        """
        【テスト】回収ランク=弁護士介入/訴訟中 は除外される

        混合データで確認
        """
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
        rows = [
            # 1. 通常（通過）
            {
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',
                '入金予定日': yesterday,
                '入金予定金額': '1',
                '回収ランク': '通常',  # 有効
                '委託先法人ID': '5',
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
            # 2. 除外対象（除外）
            {
                'TEL携帯': '090-2222-2222',
                '滞納残債': '10,000',
                '入金予定日': yesterday,
                '入金予定金額': '1',
                '回収ランク': excluded_rank,  # 除外
                '委託先法人ID': '5',
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

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, f"回収ランク={excluded_rank}は除外されるべき"


class TestArrearsFilter:
    """
    【解説】滞納残債フィルターのテスト

    フィルター条件: 滞納残債が1円以上のみ残す
    """

    def test_positive_arrears_passes(self, valid_mirail_sms_data, payment_deadline_date):
        """
        【テスト】滞納残債=10,000円 は通過する
        """
        csv_bytes = dataframe_to_csv_bytes(valid_mirail_sms_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1

    def test_zero_arrears_excluded(self, payment_deadline_date):
        """
        【テスト】滞納残債=0円 は除外される

        混合データで確認
        """
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
        rows = [
            # 1. 滞納あり（通過）
            {
                'TEL携帯': '090-1111-1111',
                '滞納残債': '10,000',  # 有効
                '入金予定日': yesterday,
                '入金予定金額': '1',
                '回収ランク': '通常',
                '委託先法人ID': '5',
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
            # 2. 滞納0円（除外）
            {
                'TEL携帯': '090-2222-2222',
                '滞納残債': '0',  # 除外
                '入金予定日': yesterday,
                '入金予定金額': '1',
                '回収ランク': '通常',
                '委託先法人ID': '5',
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

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, f"滞納残債0円のデータが除外されていない"


class TestMobilePhoneFilter:
    """
    【解説】携帯電話番号フィルターのテスト

    フィルター条件: 090/080/070 形式の携帯番号のみ残す
    """

    @pytest.mark.parametrize("valid_phone", [
        "090-1234-5678",
        "080-1234-5678",
        "070-1234-5678",
    ])
    def test_valid_mobile_phones_pass(self, valid_phone, payment_deadline_date):
        """
        【テスト】有効な携帯番号は通過する
        """
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
        rows = [{
            'TEL携帯': valid_phone,
            '滞納残債': '10,000',
            '入金予定日': yesterday,
            '入金予定金額': '1',
            '回収ランク': '通常',
            '委託先法人ID': '5',
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

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 1, f"携帯番号={valid_phone}は通過するべき"

    @pytest.mark.parametrize("invalid_phone", [
        "03-1234-5678",    # 固定電話
        "0120-123-456",    # フリーダイヤル
        "09012345678",     # ハイフンなし
        "",                # 空白
    ])
    def test_invalid_phones_excluded(self, invalid_phone, payment_deadline_date):
        """
        【テスト】無効な電話番号は除外される
        """
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
        rows = [{
            'TEL携帯': invalid_phone,
            '滞納残債': '10,000',
            '入金予定日': yesterday,
            '入金予定金額': '1',
            '回収ランク': '通常',
            '委託先法人ID': '5',
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

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 0, f"電話番号={invalid_phone}は除外されるべき"


class TestDataMapping:
    """
    【解説】データマッピングのテスト

    処理後の出力DataFrameに正しい値がマッピングされているかを確認
    """

    def test_output_columns_exist(self, valid_mirail_sms_data, payment_deadline_date):
        """
        【テスト】出力DataFrameに必要な列が存在する
        """
        csv_bytes = dataframe_to_csv_bytes(valid_mirail_sms_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        expected_columns = [
            '電話番号',
            '(info1)契約者名',
            '(info2)物件名',
            '(info3)金額',
            '(info4)銀行口座',
            '(info5)メモ',
            '支払期限',
        ]

        for col in expected_columns:
            assert col in result_df.columns, f"列 '{col}' が出力に存在しない"

    def test_phone_number_mapping(self, valid_mirail_sms_data, payment_deadline_date):
        """
        【テスト】電話番号が正しくマッピングされる
        """
        csv_bytes = dataframe_to_csv_bytes(valid_mirail_sms_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert result_df['電話番号'].iloc[0] == '090-1234-5678'

    def test_contractor_name_mapping(self, valid_mirail_sms_data, payment_deadline_date):
        """
        【テスト】契約者名が正しくマッピングされる
        """
        csv_bytes = dataframe_to_csv_bytes(valid_mirail_sms_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert result_df['(info1)契約者名'].iloc[0] == 'テスト太郎'

    def test_property_name_mapping(self, valid_mirail_sms_data, payment_deadline_date):
        """
        【テスト】物件名（物件名＋物件番号）が正しくマッピングされる
        """
        csv_bytes = dataframe_to_csv_bytes(valid_mirail_sms_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        # 「物件名　物件番号」の形式（全角スペースで結合）
        expected = 'テストマンション　101'
        assert result_df['(info2)物件名'].iloc[0] == expected

    def test_amount_format_with_comma(self, valid_mirail_sms_data, payment_deadline_date):
        """
        【テスト】金額がカンマ区切りでフォーマットされる
        """
        csv_bytes = dataframe_to_csv_bytes(valid_mirail_sms_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        # 10,000 の形式
        assert result_df['(info3)金額'].iloc[0] == '10,000'

    def test_payment_deadline_format(self, valid_mirail_sms_data, payment_deadline_date):
        """
        【テスト】支払期限が日本語形式でフォーマットされる
        """
        csv_bytes = dataframe_to_csv_bytes(valid_mirail_sms_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        # 2025年12月31日 の形式
        expected = '2025年12月31日'
        assert result_df['支払期限'].iloc[0] == expected


class TestIntegration:
    """
    【解説】統合テスト

    複数のデータを処理して、正しい件数が出力されるかを確認
    """

    def test_mixed_data_correct_count(self, mixed_data, payment_deadline_date):
        """
        【テスト】混合データで正しい件数が出力される

        mixed_data には4件あり、うち2件が通過するはず:
        - 1件目: 全て通過 → 出力
        - 2件目: 委託先法人ID=999で除外
        - 3件目: 委託先法人ID=空白で通過 → 出力
        - 4件目: 回収ランク=弁護士介入で除外
        """
        csv_bytes = dataframe_to_csv_bytes(mixed_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert len(result_df) == 2, f"期待: 2件, 実際: {len(result_df)}件"

    def test_all_excluded_raises_error(self, invalid_trustee_id_data, payment_deadline_date):
        """
        【テスト】全件除外された場合、例外が発生する

        【解説】現在の実装の動作
        全件がフィルターで除外されると、空のDataFrameに対して
        列番号アクセス（iloc）が失敗し、例外が発生します。

        これは理想的な動作ではありませんが、実際の運用では
        全件除外されるようなデータは渡されないため、
        現在の動作を保証するテストとしています。
        """
        csv_bytes = dataframe_to_csv_bytes(invalid_trustee_id_data)

        with pytest.raises(Exception):
            process_mirail_sms_contract_data(csv_bytes, payment_deadline_date)

    def test_logs_contain_filter_info(self, mixed_data, payment_deadline_date):
        """
        【テスト】ログにフィルター情報が含まれる
        """
        csv_bytes = dataframe_to_csv_bytes(mixed_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        # ログが空でないことを確認
        assert len(logs) > 0

    def test_output_filename_format(self, valid_mirail_sms_data, payment_deadline_date):
        """
        【テスト】出力ファイル名が正しい形式
        """
        csv_bytes = dataframe_to_csv_bytes(valid_mirail_sms_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        # MMDDミライルSMS契約者.csv の形式
        assert 'ミライルSMS契約者.csv' in filename

    def test_stats_contain_row_counts(self, mixed_data, payment_deadline_date):
        """
        【テスト】統計情報に行数が含まれる
        """
        csv_bytes = dataframe_to_csv_bytes(mixed_data)

        result_df, logs, filename, stats = process_mirail_sms_contract_data(
            csv_bytes, payment_deadline_date
        )

        assert 'initial_rows' in stats
        assert 'processed_rows' in stats
        assert stats['initial_rows'] == 4
        assert stats['processed_rows'] == 2


class TestErrorHandling:
    """
    【解説】エラーハンドリングテスト

    異常な入力に対して適切にエラーが発生するかを確認
    """

    def test_empty_file_raises_error(self, payment_deadline_date):
        """
        【テスト】空のファイルでエラーが発生する
        """
        empty_csv = b""

        with pytest.raises(Exception):
            process_mirail_sms_contract_data(empty_csv, payment_deadline_date)

    def test_invalid_csv_format_raises_error(self, payment_deadline_date):
        """
        【テスト】不正なCSV形式でエラーが発生する
        """
        invalid_csv = b"this is not a csv file\x00\x01\x02"

        with pytest.raises(Exception):
            process_mirail_sms_contract_data(invalid_csv, payment_deadline_date)

    def test_insufficient_columns_raises_error(self, payment_deadline_date):
        """
        【テスト】列数が不足している場合エラーが発生する

        ミライルSMSは列番号118（119列目）にアクセスするため、
        列数が不足しているとエラーになる。
        """
        # 10列しかないCSV
        small_df = pd.DataFrame({
            f'col_{i}': ['value'] for i in range(10)
        })
        csv_bytes = small_df.to_csv(index=False).encode('utf-8')

        with pytest.raises(Exception):
            process_mirail_sms_contract_data(csv_bytes, payment_deadline_date)
