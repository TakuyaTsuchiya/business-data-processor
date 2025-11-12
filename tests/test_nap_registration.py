#!/usr/bin/env python3
"""
ナップ新規登録プロセッサのテスト
"""

import pytest
import pandas as pd
import io
from datetime import datetime
from processors.nap_registration import (
    NapConfig,
    FileReader,
    DuplicateChecker,
    DataMapper,
    process_nap_data
)


class TestNapConfig:
    """NapConfigクラスのテスト"""

    def test_output_columns_count(self):
        """OUTPUT_COLUMNSが111列あることを確認"""
        assert len(NapConfig.OUTPUT_COLUMNS) == 111

    def test_output_columns_structure(self):
        """OUTPUT_COLUMNSの最初と最後の列名を確認"""
        assert NapConfig.OUTPUT_COLUMNS[0] == "引継番号"
        assert NapConfig.OUTPUT_COLUMNS[-1] == "登録フラグ"
        assert NapConfig.OUTPUT_COLUMNS[47] == "クライアントCD"

    def test_fixed_values_required_keys(self):
        """FIXED_VALUESに必須キーが含まれることを確認"""
        required_keys = [
            "クライアントCD",
            "委託先法人ID",
            "入居ステータス",
            "滞納ステータス",
            "受託状況",
            "契約種類",
            "回収口座金融機関CD",
            "回収口座金融機関名",
            "回収口座種類",
            "回収口座番号",
            "回収口座名義"
        ]
        for key in required_keys:
            assert key in NapConfig.FIXED_VALUES

    def test_fixed_values_content(self):
        """FIXED_VALUESの値が正しいことを確認"""
        assert NapConfig.FIXED_VALUES["クライアントCD"] == "9268"
        assert NapConfig.FIXED_VALUES["委託先法人ID"] == "5"
        assert NapConfig.FIXED_VALUES["回収口座金融機関名"] == "みずほ銀行"
        assert NapConfig.FIXED_VALUES["回収口座番号"] == "4389306"
        assert NapConfig.FIXED_VALUES["回収口座名義"] == "ナップ賃貸保証株式会社"

    def test_target_corporation_id(self):
        """TARGET_CORPORATION_IDが5であることを確認"""
        assert NapConfig.TARGET_CORPORATION_ID == "5"

    def test_excel_skiprows(self):
        """EXCEL_SKIPROWSが13であることを確認"""
        assert NapConfig.EXCEL_SKIPROWS == 13


# 他のテストクラスはこれから追加
