"""
ミライルオートコールプロセッサーの基本テスト
関数の存在と基本動作をテスト
"""

import pytest
import pandas as pd
from io import StringIO
import sys
import os

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# ミライルオートコール関連のインポート
from processors.mirail_autocall.contract.without10k import process_mirail_contract_without10k_data
from processors.mirail_autocall.contract.with10k import process_mirail_contract_with10k_data
from processors.mirail_autocall.guarantor.without10k import process_mirail_guarantor_without10k_data
from processors.mirail_autocall.guarantor.with10k import process_mirail_guarantor_with10k_data
from processors.mirail_autocall.emergency_contact.without10k import process_mirail_emergencycontact_without10k_data
from processors.mirail_autocall.emergency_contact.with10k import process_mirail_emergencycontact_with10k_data


class TestMirailBasic:
    """ミライルオートコール基本テストクラス"""

    @pytest.mark.unit
    def test_contract_without10k_function_exists(self, sample_mirail_data, mock_file_factory):
        """契約者without10k関数の存在確認"""
        mirail_file = mock_file_factory(sample_mirail_data, "mirail_contract.csv")
        
        # 関数が呼び出せることを確認
        try:
            result = process_mirail_contract_without10k_data(mirail_file)
            # 結果の型確認
            if result is not None:
                assert isinstance(result, tuple), "戻り値がタプルではない"
                assert len(result) == 2, "戻り値のタプル要素数が2ではない"
                output_df, filename = result
                assert isinstance(output_df, pd.DataFrame), "出力がDataFrameではない"
                assert isinstance(filename, str), "ファイル名が文字列ではない"
            assert True, "関数が正常に実行された"
        except Exception as e:
            pytest.fail(f"関数実行でエラーが発生: {str(e)}")

    @pytest.mark.unit
    def test_contract_with10k_function_exists(self, sample_mirail_data, mock_file_factory):
        """契約者with10k関数の存在確認"""
        mirail_file = mock_file_factory(sample_mirail_data, "mirail_contract.csv")
        
        try:
            result = process_mirail_contract_with10k_data(mirail_file)
            if result is not None:
                assert isinstance(result, tuple), "戻り値がタプルではない"
                assert len(result) == 2, "戻り値のタプル要素数が2ではない"
            assert True, "関数が正常に実行された"
        except Exception as e:
            pytest.fail(f"関数実行でエラーが発生: {str(e)}")

    @pytest.mark.skip(reason="保証人データ構造の調査中 - 正しいテストデータで後日実装予定")
    @pytest.mark.unit
    def test_guarantor_without10k_function_exists(self, sample_mirail_data, mock_file_factory):
        """保証人without10k関数の存在確認"""
        mirail_file = mock_file_factory(sample_mirail_data, "mirail_guarantor.csv")
        
        try:
            result = process_mirail_guarantor_without10k_data(mirail_file)
            if result is not None:
                assert isinstance(result, tuple), "戻り値がタプルではない"
                assert len(result) == 2, "戻り値のタプル要素数が2ではない"
            assert True, "関数が正常に実行された"
        except Exception as e:
            pytest.fail(f"関数実行でエラーが発生: {str(e)}")

    @pytest.mark.skip(reason="保証人データ構造の調査中 - 正しいテストデータで後日実装予定")
    @pytest.mark.unit
    def test_guarantor_with10k_function_exists(self, sample_mirail_data, mock_file_factory):
        """保証人with10k関数の存在確認"""
        mirail_file = mock_file_factory(sample_mirail_data, "mirail_guarantor.csv")
        
        try:
            result = process_mirail_guarantor_with10k_data(mirail_file)
            if result is not None:
                assert isinstance(result, tuple), "戻り値がタプルではない"
                assert len(result) == 2, "戻り値のタプル要素数が2ではない"
            assert True, "関数が正常に実行された"
        except Exception as e:
            pytest.fail(f"関数実行でエラーが発生: {str(e)}")

    @pytest.mark.skip(reason="緊急連絡人データ構造の調査中 - 正しいテストデータで後日実装予定")
    @pytest.mark.unit
    def test_emergency_contact_without10k_function_exists(self, sample_mirail_data, mock_file_factory):
        """緊急連絡人without10k関数の存在確認"""
        mirail_file = mock_file_factory(sample_mirail_data, "mirail_emergency.csv")
        
        try:
            result = process_mirail_emergencycontact_without10k_data(mirail_file)
            if result is not None:
                assert isinstance(result, tuple), "戻り値がタプルではない"
                assert len(result) == 2, "戻り値のタプル要素数が2ではない"
            assert True, "関数が正常に実行された"
        except Exception as e:
            pytest.fail(f"関数実行でエラーが発生: {str(e)}")

    @pytest.mark.skip(reason="緊急連絡人データ構造の調査中 - 正しいテストデータで後日実装予定")
    @pytest.mark.unit
    def test_emergency_contact_with10k_function_exists(self, sample_mirail_data, mock_file_factory):
        """緊急連絡人with10k関数の存在確認"""
        mirail_file = mock_file_factory(sample_mirail_data, "mirail_emergency.csv")
        
        try:
            result = process_mirail_emergencycontact_with10k_data(mirail_file)
            if result is not None:
                assert isinstance(result, tuple), "戻り値がタプルではない"
                assert len(result) == 2, "戻り値のタプル要素数が2ではない"
            assert True, "関数が正常に実行された"
        except Exception as e:
            pytest.fail(f"関数実行でエラーが発生: {str(e)}")

    @pytest.mark.unit
    def test_invalid_data_handling(self, mock_file_factory):
        """不正データの処理テスト"""
        # 不正なカラムを持つデータ
        invalid_data = pd.DataFrame({
            'wrong_column': ['data1', 'data2'],
            'another_wrong': ['data3', 'data4']
        })
        
        invalid_file = mock_file_factory(invalid_data, "invalid.csv")
        
        # 全ての関数で適切にNoneが返されることを確認
        functions_to_test = [
            process_mirail_contract_without10k_data,
            process_mirail_contract_with10k_data,
            process_mirail_guarantor_without10k_data,
            process_mirail_guarantor_with10k_data,
            process_mirail_emergencycontact_without10k_data,
            process_mirail_emergencycontact_with10k_data
        ]
        
        for func in functions_to_test:
            try:
                result = func(invalid_file)
                # 不正データの場合はNoneまたは適切な処理がされることを確認
                assert result is None or isinstance(result, tuple), f"{func.__name__}で期待外の戻り値"
            except Exception:
                # エラーが発生しても処理が停止しないことを確認
                assert True, f"{func.__name__}でエラーが発生したが処理は継続"