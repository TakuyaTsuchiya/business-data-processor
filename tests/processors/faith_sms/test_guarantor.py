"""
フェイスSMS保証人プロセッサのテスト

guarantor.pyは:
- Filter 6でiloc[:, 46]（AU列）を使用
- 出力の電話番号にiloc[:, 46]をマッピング
- 保証人=保証人１氏名, 連絡人=''
- ファイル名: {MMDD}フェイスSMS保証人.csv

【注意】プロセッサは全行がフィルタで除外されると空DataFrameでの処理で
例外が発生する。そのため、フィルター除外テストでは有効行1行+無効行1行の
2行を渡し、結果が1行（無効行が除外された）ことで検証する。
"""

import pandas as pd
import pytest
from datetime import datetime, timedelta

from processors.faith_sms.guarantor import process_faith_sms_guarantor_data
from domain.rules.business_rules import CLIENT_IDS

from tests.processors.faith_sms.conftest import (
    create_faith_sms_dataframe,
    dataframe_to_csv_bytes,
    create_valid_faith_row,
)


def _run_guarantor(rows, payment_deadline_date):
    """テスト用ヘルパー: rows から CSV bytes を作成してプロセッサを実行"""
    df = create_faith_sms_dataframe(rows)
    csv_bytes = dataframe_to_csv_bytes(df)
    return process_faith_sms_guarantor_data(csv_bytes, payment_deadline_date)


# ============================================================
# 統合テスト
# ============================================================
class TestFaithSmsGuarantorIntegration:
    """保証人プロセッサの統合テスト"""

    def test_valid_data_produces_output(self, payment_deadline_date):
        """全フィルターを通過するデータが1行出力される"""
        rows = [create_valid_faith_row()]
        result_df, logs, filename, stats = _run_guarantor(rows, payment_deadline_date)

        assert len(result_df) == 1
        assert stats["processed_rows"] == 1

    def test_output_has_59_columns(self, payment_deadline_date):
        """SMSテンプレートの59列が出力される"""
        rows = [create_valid_faith_row()]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)

        assert len(result_df.columns) == 59

    def test_output_filename_format(self, payment_deadline_date):
        """ファイル名が{MMDD}フェイスSMS保証人.csv形式"""
        rows = [create_valid_faith_row()]
        _, _, filename, _ = _run_guarantor(rows, payment_deadline_date)

        date_str = datetime.now().strftime("%m%d")
        assert filename == f"{date_str}フェイスSMS保証人.csv"

    def test_output_mapping(self, payment_deadline_date):
        """出力マッピングの検証: 電話番号はiloc[:, 46]、保証人=保証人１氏名、連絡人=空"""
        rows = [
            create_valid_faith_row(
                **{
                    "保証人１氏名": "保証山田",
                    "契約者氏名": "山田太郎",
                    "物件名": "サンプルマンション",
                    "物件番号": "202",
                    "管理番号": "F001",
                    "回収口座銀行名": "みずほ銀行",
                    "回収口座支店名": "渋谷支店",
                    "回収口座種類": "普通",
                    "回収口座番号": "9876543",
                    "回収口座名義人": "ヤマダタロウ",
                    "保証人TEL": "080-1111-2222",
                }
            )
        ]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)

        row = result_df.iloc[0]
        # 電話番号はiloc[:, 46]（保証人TEL）
        assert row["電話番号"] == "080-1111-2222"
        assert row["(info1)契約者名"] == "山田太郎"
        assert "サンプルマンション" in row["(info2)物件名"]
        assert row["(info3)金額"] == "10,000"
        assert "みずほ銀行" in row["(info4)銀行口座"]
        assert row["(info5)メモ"] == "F001"
        assert row["支払期限"] != ""
        # 保証人プロセッサでは保証人=保証人１氏名、連絡人=空またはNaN
        assert row["保証人"] == "保証山田"
        assert pd.isna(row["連絡人"]) or row["連絡人"] == ""


# ============================================================
# Filter 1: 委託先法人ID
# ============================================================
class TestTrusteeIdFilter:
    """委託先法人IDフィルター（faith: [1, 2, 3, 4, 8]）"""

    @pytest.mark.parametrize("valid_id", ["1", "2", "3", "4", "8"])
    def test_valid_ids_accepted(self, valid_id, payment_deadline_date):
        rows = [create_valid_faith_row(委託先法人ID=valid_id)]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1

    @pytest.mark.parametrize("invalid_id", ["0", "5", "6", "7", "99"])
    def test_invalid_ids_rejected(self, invalid_id, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 委託先法人ID="1"),
            create_valid_faith_row(契約者氏名="無効", 委託先法人ID=invalid_id),
        ]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_empty_and_nan_rejected(self, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 委託先法人ID="1"),
            create_valid_faith_row(契約者氏名="空白", 委託先法人ID=""),
            create_valid_faith_row(契約者氏名="NaN", 委託先法人ID="nan"),
        ]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1


# ============================================================
# Filter 2: 入金予定日
# ============================================================
class TestPaymentDateFilter:
    """入金予定日フィルター（空白 or 今日より前のみ通過）"""

    def test_past_date_accepted(self, payment_deadline_date):
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")
        rows = [create_valid_faith_row(入金予定日=yesterday)]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_empty_date_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(入金予定日="")]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_today_excluded(self, payment_deadline_date):
        today = datetime.now().strftime("%Y/%m/%d")
        rows = [
            create_valid_faith_row(
                契約者氏名="有効",
                入金予定日=(datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d"),
            ),
            create_valid_faith_row(契約者氏名="除外", 入金予定日=today),
        ]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_future_date_excluded(self, payment_deadline_date):
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y/%m/%d")
        rows = [
            create_valid_faith_row(
                契約者氏名="有効",
                入金予定日=(datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d"),
            ),
            create_valid_faith_row(契約者氏名="除外", 入金予定日=tomorrow),
        ]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1


# ============================================================
# Filter 3: 入金予定金額
# ============================================================
class TestPaymentAmountFilter:
    """入金予定金額フィルター（faith: [2, 3, 5]を除外）"""

    @pytest.mark.parametrize("excluded_amount", ["2", "3", "5"])
    def test_excluded_amounts(self, excluded_amount, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 入金予定金額="1"),
            create_valid_faith_row(契約者氏名="除外", 入金予定金額=excluded_amount),
        ]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_normal_amount_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(入金予定金額="1")]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1


# ============================================================
# Filter 4: 回収ランク
# ============================================================
class TestCollectionRankFilter:
    """回収ランクフィルター（弁護士介入, 破産決定, 死亡決定を除外）"""

    @pytest.mark.parametrize("excluded_rank", ["弁護士介入", "破産決定", "死亡決定"])
    def test_excluded_ranks(self, excluded_rank, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 回収ランク="通常"),
            create_valid_faith_row(契約者氏名="除外", 回収ランク=excluded_rank),
        ]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_normal_rank_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(回収ランク="通常")]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1


# ============================================================
# Filter 5: 滞納残債
# ============================================================
class TestArrearsFilter:
    """滞納残債フィルター（iloc[:, 71] >= 1）"""

    def test_zero_excluded(self, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 滞納残債="10,000"),
            create_valid_faith_row(契約者氏名="除外", 滞納残債="0"),
        ]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_positive_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(滞納残債="5000")]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_comma_formatted_amount_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(滞納残債="100,000")]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1


# ============================================================
# Filter 6: 保証人TEL（iloc[:, 46]）
# ============================================================
class TestPhoneFilter:
    """保証人電話番号フィルター（AU列 iloc[:, 46]、090/080/070-XXXX-XXXX形式）"""

    def test_valid_090_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(保証人TEL="090-1111-2222")]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_valid_080_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(保証人TEL="080-1111-2222")]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_valid_070_accepted(self, payment_deadline_date):
        rows = [create_valid_faith_row(保証人TEL="070-1111-2222")]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_landline_rejected(self, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 保証人TEL="090-1111-2222"),
            create_valid_faith_row(契約者氏名="除外", 保証人TEL="03-1234-5678"),
        ]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1

    def test_empty_rejected(self, payment_deadline_date):
        rows = [
            create_valid_faith_row(契約者氏名="有効", 保証人TEL="090-1111-2222"),
            create_valid_faith_row(契約者氏名="除外", 保証人TEL=""),
        ]
        result_df, _, _, _ = _run_guarantor(rows, payment_deadline_date)
        assert len(result_df) == 1


# ============================================================
# 複合データテスト
# ============================================================
class TestMixedData:
    """複数パターンのデータが正しくフィルタリングされることを検証"""

    def test_correct_filtering_with_mixed_data(self, payment_deadline_date):
        rows = [
            # 通過するデータ
            create_valid_faith_row(契約者氏名="通過太郎"),
            # 委託先法人IDで除外
            create_valid_faith_row(契約者氏名="除外二郎", 委託先法人ID="99"),
            # 保証人TELが固定電話で除外
            create_valid_faith_row(契約者氏名="除外三郎", 保証人TEL="03-1234-5678"),
            # 通過するデータ（別のID）
            create_valid_faith_row(契約者氏名="通過四郎", 委託先法人ID="8"),
        ]
        result_df, _, _, stats = _run_guarantor(rows, payment_deadline_date)

        assert len(result_df) == 2
        assert stats["initial_rows"] == 4
        assert stats["processed_rows"] == 2


# ============================================================
# ビジネスルール整合性テスト
# ============================================================
class TestBusinessRulesConsistency:
    """ビジネスルールの定数がプロセッサの期待値と一致することを検証"""

    def test_faith_client_ids_match_expected(self):
        assert CLIENT_IDS["faith"] == [1, 2, 3, 4, 8]
