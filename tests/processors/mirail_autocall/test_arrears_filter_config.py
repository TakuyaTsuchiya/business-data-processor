"""
滞納残債フィルタ設定のユニットテスト (Issue #61)

統合プロセッサに滞納残債フィルタが正しく設定されていることを確認
"""

import pytest
from processors.mirail_autocall_unified import MirailAutocallUnifiedProcessor


class TestArrearsFilterConfig:
    """滞納残債フィルタ設定のテスト"""

    def test_arrears_filter_in_contract_with10k(self):
        """契約者（10k含む）に滞納残債フィルタが含まれることを確認"""
        processor = MirailAutocallUnifiedProcessor()
        filter_config = processor.get_base_filter_config("contract", with_10k=True)

        # arrears フィルタが存在することを確認
        assert "arrears" in filter_config, "滞納残債フィルタが設定されていません"

        # 設定内容を確認
        arrears_config = filter_config["arrears"]
        assert "column" in arrears_config
        assert "min_amount" in arrears_config
        assert arrears_config["min_amount"] == 1
        assert arrears_config["label"] == "滞納残債"

    def test_arrears_filter_in_contract_without10k(self):
        """契約者（10k除外）に滞納残債フィルタが含まれることを確認"""
        processor = MirailAutocallUnifiedProcessor()
        filter_config = processor.get_base_filter_config("contract", with_10k=False)

        # arrears フィルタが存在することを確認
        assert "arrears" in filter_config, "滞納残債フィルタが設定されていません"

        # 設定内容を確認
        arrears_config = filter_config["arrears"]
        assert "column" in arrears_config
        assert "min_amount" in arrears_config
        assert arrears_config["min_amount"] == 1
        assert arrears_config["label"] == "滞納残債"

    def test_arrears_filter_in_guarantor(self):
        """保証人に滞納残債フィルタが含まれることを確認"""
        processor = MirailAutocallUnifiedProcessor()
        filter_config = processor.get_base_filter_config("guarantor", with_10k=True)

        # arrears フィルタが存在することを確認
        assert "arrears" in filter_config, "滞納残債フィルタが設定されていません"

    def test_arrears_filter_in_emergency_contact(self):
        """緊急連絡人に滞納残債フィルタが含まれることを確認"""
        processor = MirailAutocallUnifiedProcessor()
        filter_config = processor.get_base_filter_config("emergency_contact", with_10k=True)

        # arrears フィルタが存在することを確認
        assert "arrears" in filter_config, "滞納残債フィルタが設定されていません"

    def test_arrears_filter_order(self):
        """滞納残債フィルタが適切な順序で配置されていることを確認"""
        processor = MirailAutocallUnifiedProcessor()
        filter_config = processor.get_base_filter_config("contract", with_10k=False)

        # フィルタの順序を確認
        filter_keys = list(filter_config.keys())

        # arrears は collection_rank の後に配置されている
        assert "collection_rank" in filter_keys
        assert "arrears" in filter_keys

        collection_rank_idx = filter_keys.index("collection_rank")
        arrears_idx = filter_keys.index("arrears")

        assert arrears_idx > collection_rank_idx, \
            "滞納残債フィルタはcollection_rankの後に配置されるべきです"
