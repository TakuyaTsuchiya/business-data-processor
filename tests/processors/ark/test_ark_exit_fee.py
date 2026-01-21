#!/usr/bin/env python3
"""
ARC退去手続き費用計算のテストケース

条件:
- 通常地域（東京・大阪・北関東）: 最低70,000円、上限100,000円
- 北海道: 最低40,000円、上限100,000円
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pytest
from processors.ark_registration import DataConverter


# テストデータ定義
# 通常地域（東京・大阪・北関東: region_code=1,2,4）
test_cases_normal = [
    # (賃料, 管理費, 駐車場, その他, 期待値, 説明)
    ("30000", "0", "0", "0", 70000, "合計3万→最低7万適用"),
    ("50000", "5000", "10000", "3000", 70000, "合計6.8万→最低7万適用"),
    ("60000", "5000", "10000", "5000", 80000, "合計8万→そのまま"),
    ("90000", "5000", "5000", "0", 100000, "合計10万→上限10万適用"),
    ("150000", "10000", "10000", "10000", 100000, "合計18万→上限10万適用"),
]

# 北海道（region_code=3）
test_cases_hokkaido = [
    ("30000", "0", "0", "0", 40000, "合計3万→最低4万適用"),
    ("40000", "0", "0", "0", 40000, "合計4万→最低4万ちょうど"),
    ("50000", "5000", "5000", "0", 60000, "合計6万→そのまま"),
    ("90000", "5000", "5000", "0", 100000, "合計10万→上限10万適用"),
    ("150000", "10000", "10000", "10000", 100000, "合計18万→上限10万適用"),
]

# 境界値テスト（通常地域）
boundary_normal = [
    ("69999", "0", "0", "0", 70000, "69,999円→最低7万適用"),
    ("70000", "0", "0", "0", 70000, "70,000円→ちょうど7万"),
    ("70001", "0", "0", "0", 70001, "70,001円→そのまま"),
    ("99999", "0", "0", "0", 99999, "99,999円→そのまま"),
    ("100000", "0", "0", "0", 100000, "100,000円→ちょうど10万"),
    ("100001", "0", "0", "0", 100000, "100,001円→上限10万適用"),
]

# 境界値テスト（北海道）
boundary_hokkaido = [
    ("39999", "0", "0", "0", 40000, "39,999円→最低4万適用"),
    ("40000", "0", "0", "0", 40000, "40,000円→ちょうど4万"),
    ("40001", "0", "0", "0", 40001, "40,001円→そのまま"),
    ("100001", "0", "0", "0", 100000, "100,001円→上限10万適用"),
]

# エッジケース
edge_cases = [
    ("0", "0", "0", "0", 70000, "全て0円→最低7万適用", 1),
    ("", "", "", "", 70000, "空文字→最低7万適用", 1),
    ("30,000", "5,000", "10,000", "5,000", 70000, "カンマ付き50,000円→最低7万適用", 1),
    ("abc", "def", "ghi", "jkl", 70000, "無効な文字列→最低7万適用", 1),
    ("0", "0", "0", "0", 40000, "全て0円→最低4万適用（北海道）", 3),
    ("", "", "", "", 40000, "空文字→最低4万適用（北海道）", 3),
]


class TestExitFeeNormalRegion:
    """通常地域（東京・大阪・北関東）の退去手続き費用テスト"""

    @pytest.fixture
    def converter(self):
        return DataConverter()

    @pytest.mark.parametrize("rent,mgmt,parking,other,expected,desc", test_cases_normal)
    def test_normal_region_cases(self, converter, rent, mgmt, parking, other, expected, desc):
        """通常地域の基本ケース"""
        result = converter.calculate_exit_procedure_fee(rent, mgmt, parking, other, region_code=1)
        assert result == expected, f"{desc}: got {result}, expected {expected}"

    @pytest.mark.parametrize("rent,mgmt,parking,other,expected,desc", boundary_normal)
    def test_boundary_cases(self, converter, rent, mgmt, parking, other, expected, desc):
        """通常地域の境界値テスト"""
        result = converter.calculate_exit_procedure_fee(rent, mgmt, parking, other, region_code=1)
        assert result == expected, f"{desc}: got {result}, expected {expected}"


class TestExitFeeHokkaido:
    """北海道の退去手続き費用テスト"""

    @pytest.fixture
    def converter(self):
        return DataConverter()

    @pytest.mark.parametrize("rent,mgmt,parking,other,expected,desc", test_cases_hokkaido)
    def test_hokkaido_cases(self, converter, rent, mgmt, parking, other, expected, desc):
        """北海道の基本ケース"""
        result = converter.calculate_exit_procedure_fee(rent, mgmt, parking, other, region_code=3)
        assert result == expected, f"{desc}: got {result}, expected {expected}"

    @pytest.mark.parametrize("rent,mgmt,parking,other,expected,desc", boundary_hokkaido)
    def test_boundary_cases(self, converter, rent, mgmt, parking, other, expected, desc):
        """北海道の境界値テスト"""
        result = converter.calculate_exit_procedure_fee(rent, mgmt, parking, other, region_code=3)
        assert result == expected, f"{desc}: got {result}, expected {expected}"


class TestExitFeeEdgeCases:
    """エッジケースのテスト"""

    @pytest.fixture
    def converter(self):
        return DataConverter()

    @pytest.mark.parametrize("rent,mgmt,parking,other,expected,desc,region", edge_cases)
    def test_edge_cases(self, converter, rent, mgmt, parking, other, expected, desc, region):
        """エッジケース（空文字、無効値など）"""
        result = converter.calculate_exit_procedure_fee(rent, mgmt, parking, other, region_code=region)
        assert result == expected, f"{desc}: got {result}, expected {expected}"


class TestExitFeeAllRegions:
    """全地域コードの動作確認"""

    @pytest.fixture
    def converter(self):
        return DataConverter()

    def test_tokyo_region_code_1(self, converter):
        """東京（region_code=1）は最低7万・上限10万"""
        # 最低額テスト
        assert converter.calculate_exit_procedure_fee("50000", "0", "0", "0", region_code=1) == 70000
        # 上限テスト
        assert converter.calculate_exit_procedure_fee("150000", "0", "0", "0", region_code=1) == 100000

    def test_osaka_region_code_2(self, converter):
        """大阪（region_code=2）は最低7万・上限10万"""
        # 最低額テスト
        assert converter.calculate_exit_procedure_fee("50000", "0", "0", "0", region_code=2) == 70000
        # 上限テスト
        assert converter.calculate_exit_procedure_fee("150000", "0", "0", "0", region_code=2) == 100000

    def test_hokkaido_region_code_3(self, converter):
        """北海道（region_code=3）は最低4万・上限10万"""
        # 最低額テスト
        assert converter.calculate_exit_procedure_fee("30000", "0", "0", "0", region_code=3) == 40000
        # 上限テスト
        assert converter.calculate_exit_procedure_fee("150000", "0", "0", "0", region_code=3) == 100000

    def test_kitakanto_region_code_4(self, converter):
        """北関東（region_code=4）は最低7万・上限10万"""
        # 最低額テスト
        assert converter.calculate_exit_procedure_fee("50000", "0", "0", "0", region_code=4) == 70000
        # 上限テスト
        assert converter.calculate_exit_procedure_fee("150000", "0", "0", "0", region_code=4) == 100000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
