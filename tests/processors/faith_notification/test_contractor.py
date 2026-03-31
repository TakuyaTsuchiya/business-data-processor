"""
process_contractor のテスト
"""

import pytest

from processors.faith_notification import process_contractor, process_faith_notification
from tests.processors.faith_notification.conftest import (
    COL,
    create_notification_dataframe,
    base_valid_row_data,
)


CONTRACTOR_COLUMNS = [
    "管理番号",
    "契約者氏名",
    "郵便番号",
    "現住所1",
    "現住所2",
    "現住所3",
    "滞納残債",
    "物件住所1",
    "物件住所2",
    "物件住所3",
    "物件名",
    "物件番号",
    "回収口座銀行CD",
    "回収口座銀行名",
    "回収口座支店CD",
    "回収口座支店名",
    "回収口座種類",
    "回収口座番号",
    "回収口座名義人",
]


class TestContractorAddressFilter:
    """契約者住所フィルタのテスト"""

    def test_complete_address_passes(self):
        """全住所フィールドが揃った行は通過する"""
        row = base_valid_row_data()
        df = create_notification_dataframe([row])
        result, _ = process_contractor(df)
        assert len(result) == 1

    @pytest.mark.parametrize(
        "field",
        ["POSTAL_CODE", "ADDRESS1", "ADDRESS2", "ADDRESS3"],
    )
    def test_missing_address_field_excluded(self, field):
        """住所関連フィールドが空の場合、除外される"""
        row = base_valid_row_data()
        row[COL[field]] = ""
        df = create_notification_dataframe([row])
        result, _ = process_contractor(df)
        assert len(result) == 0


class TestContractorOutput:
    """契約者出力のテスト"""

    def test_output_columns_correct(self):
        """出力は19列で正しい列名順序"""
        row = base_valid_row_data()
        df = create_notification_dataframe([row])
        result, _ = process_contractor(df)
        assert list(result.columns) == CONTRACTOR_COLUMNS
        assert len(result.columns) == 19

    def test_output_values_mapped_correctly(self):
        """出力値が入力から正しくマッピングされている"""
        row = base_valid_row_data()
        df = create_notification_dataframe([row])
        result, _ = process_contractor(df)
        r = result.iloc[0]
        assert r["管理番号"] == "MGT001"
        assert r["契約者氏名"] == "テスト太郎"
        assert r["郵便番号"] == "100-0001"
        assert r["現住所1"] == "東京都"
        assert r["現住所2"] == "千代田区"
        assert r["現住所3"] == "丸の内1-1-1"
        assert r["滞納残債"] == "10,000"
        assert r["物件住所1"] == "東京都"
        assert r["物件住所2"] == "港区"
        assert r["物件住所3"] == "六本木1-1"
        assert r["物件名"] == "テストマンション"
        assert r["物件番号"] == "101"
        assert r["回収口座銀行CD"] == "0001"
        assert r["回収口座銀行名"] == "みずほ銀行"
        assert r["回収口座支店CD"] == "001"
        assert r["回収口座支店名"] == "東京支店"
        assert r["回収口座種類"] == "普通"
        assert r["回収口座番号"] == "1234567"
        assert r["回収口座名義人"] == "テスト太郎"


class TestContractorE2E:
    """契約者E2Eテスト"""

    def test_e2e_contractor_litigation_only(self):
        """process_faith_notification経由でcontractorを処理"""
        row = base_valid_row_data()
        row[COL["RESIDENCE_STATUS"]] = "入居中"
        row[COL["COLLECTION_RANK"]] = "訴訟中"
        df = create_notification_dataframe([row])
        result_df, filename, message, logs = process_faith_notification(
            df, "contractor", occupancy_status="入居中", filter_type="litigation_only"
        )
        assert len(result_df) == 1
        assert "契約者" in filename
        assert "訴訟中" in filename
        assert list(result_df.columns) == CONTRACTOR_COLUMNS

    def test_e2e_contractor_evicted_filename(self):
        """退去済 + evicted時のファイル名で「退去済み」に変換される"""
        row = base_valid_row_data()
        row[COL["RESIDENCE_STATUS"]] = "退去済"
        row[COL["COLLECTION_RANK"]] = "通常"
        df = create_notification_dataframe([row])
        result_df, filename, message, logs = process_faith_notification(
            df, "contractor", occupancy_status="退去済", filter_type="evicted"
        )
        assert "退去済み" in filename
        assert "退去済】" not in filename


class TestInvalidTargetType:
    """不正なtarget_typeのテスト"""

    def test_invalid_target_type_raises_error(self):
        """不正なtarget_typeを渡すとエラーが発生する"""
        row = base_valid_row_data()
        df = create_notification_dataframe([row])
        with pytest.raises(Exception, match="不明なターゲットタイプ"):
            process_faith_notification(df, "invalid_type")
