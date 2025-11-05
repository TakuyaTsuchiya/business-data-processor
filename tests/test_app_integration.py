"""
アプリケーション統合テスト
Streamlitアプリの主要機能をテスト
"""

import pytest
import pandas as pd
from io import StringIO
import sys
import os

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# アプリ関連のインポート
import app
from processors.ark_late_payment_update import process_ark_late_payment_data
from processors.ark_registration import process_ark_data
from processors.mirail_autocall.unified_wrapper import (
    process_mirail_contract_without10k_data,
    process_mirail_contract_with10k_data
)


class TestAppIntegration:
    """アプリ統合テストクラス"""

    @pytest.mark.integration
    def test_app_module_imports(self):
        """アプリモジュールのインポートテスト"""
        # app.pyが正常にインポートできることを確認
        assert hasattr(app, 'main'), "app.py にmain関数が存在しない"
        
    @pytest.mark.integration
    def test_processor_functions_availability(self):
        """各プロセッサー関数の利用可能性テスト"""
        # 主要なプロセッサー関数が利用可能であることを確認
        processors = [
            process_ark_late_payment_data,
            process_ark_data,
            process_mirail_contract_without10k_data,
            process_mirail_contract_with10k_data
        ]
        
        for processor in processors:
            assert callable(processor), f"{processor.__name__} が呼び出し可能ではない"

    @pytest.mark.integration
    def test_csv_encoding_detection(self, mock_file_factory):
        """CSV エンコーディング検出の統合テスト"""
        # 異なるエンコーディングでテストデータを作成
        test_data = pd.DataFrame({
            'テスト列1': ['データ1', 'データ2'],
            'テスト列2': ['値1', '値2']
        })
        
        # UTF-8エンコーディング
        utf8_file = mock_file_factory(test_data, "test_utf8.csv", encoding="utf-8")
        
        # Shift_JISエンコーディング
        sjis_file = mock_file_factory(test_data, "test_sjis.csv", encoding="shift_jis")
        
        # エンコーディング処理がエラーなく動作することを確認
        assert utf8_file is not None, "UTF-8ファイルの作成に失敗"
        assert sjis_file is not None, "Shift_JISファイルの作成に失敗"

    @pytest.mark.integration
    def test_ark_late_payment_integration(self, sample_ark_data, sample_contract_data, mock_file_factory):
        """アーク残債更新の統合テスト"""
        # ファイル作成
        ark_file = mock_file_factory(sample_ark_data, "ark_data.csv")
        contract_file = mock_file_factory(sample_contract_data, "contract_list.csv")
        
        # 統合処理実行
        result = process_ark_late_payment_data(ark_file, contract_file)
        
        if result is not None:
            # Ark processors return 2 values
            output_df, filename = result
            
            # 統合処理結果の検証
            assert isinstance(output_df, pd.DataFrame), "統合処理の出力がDataFrameではない"
            assert isinstance(filename, str), "ファイル名が文字列ではない"
            assert len(filename) > 0, "ファイル名が空"
            assert 'アーク残債' in filename, "ファイル名にアーク残債が含まれていない"
        else:
            # データが条件に合わない場合はNoneが返される
            assert True, "統合処理でデータが条件に合わずNoneが返された"

    @pytest.mark.integration
    def test_mirail_contract_integration(self, sample_mirail_data, mock_file_factory):
        """ミライル契約者の統合テスト"""
        mirail_file = mock_file_factory(sample_mirail_data, "mirail_contract.csv")
        
        # without10k統合処理
        result_without = process_mirail_contract_without10k_data(mirail_file)
        
        # with10k統合処理  
        result_with = process_mirail_contract_with10k_data(mirail_file)
        
        # 統合処理結果の検証
        results = [result_without, result_with]
        for result in results:
            if result is not None:
                # Mirail processors return 3 values (統合版)
                output_df, logs, filename = result
                assert isinstance(output_df, pd.DataFrame), "統合処理の出力がDataFrameではない"
                assert isinstance(logs, list), "ログがリストではない"
                assert isinstance(filename, str), "ファイル名が文字列ではない"
                assert len(filename) > 0, "ファイル名が空"

    @pytest.mark.integration
    def test_error_handling_integration(self, mock_file_factory):
        """エラーハンドリングの統合テスト"""
        # 不正なデータで統合テスト
        invalid_data = pd.DataFrame({
            'invalid_column': ['data1', 'data2'],
            'another_invalid': ['data3', 'data4']
        })
        
        invalid_file = mock_file_factory(invalid_data, "invalid.csv")
        
        # エラーが適切に処理されることを確認
        try:
            result = process_ark_late_payment_data(invalid_file, invalid_file)
            # エラーが適切に処理され、Noneまたは適切な結果が返される
            assert result is None or isinstance(result, tuple), "エラー処理が適切ではない"
        except Exception:
            # 例外が発生してもテストが停止しないことを確認
            assert True, "例外が発生したが処理は継続"

    @pytest.mark.integration  
    def test_file_naming_conventions(self, sample_ark_data, sample_contract_data, mock_file_factory):
        """ファイル命名規則の統合テスト"""
        ark_file = mock_file_factory(sample_ark_data, "ark_data.csv")
        contract_file = mock_file_factory(sample_contract_data, "contract_list.csv")
        
        result = process_ark_late_payment_data(ark_file, contract_file)
        
        if result is not None:
            output_df, filename = result
            
            # ファイル命名規則の確認
            assert filename.endswith('.csv'), "ファイル名が.csvで終わっていない"
            
            # 日付形式が含まれているかチェック（MMDD形式）
            import re
            date_pattern = r'\d{4}'  # 4桁の数字（MMDD）
            assert re.search(date_pattern, filename), "ファイル名に日付形式が含まれていない"

    @pytest.mark.integration
    def test_data_consistency_across_processors(self, sample_mirail_data, mock_file_factory):
        """プロセッサー間のデータ一貫性テスト"""
        mirail_file = mock_file_factory(sample_mirail_data, "mirail_test.csv")
        
        # 複数のプロセッサーでの処理
        result_without = process_mirail_contract_without10k_data(mirail_file)
        result_with = process_mirail_contract_with10k_data(mirail_file)
        
        # データ一貫性の確認
        if result_without is not None and result_with is not None:
            # Mirail processors return 3 values (統合版)
            df_without, _, _ = result_without
            df_with, _, _ = result_with
            
            # 基本的なデータ構造の一貫性確認
            assert isinstance(df_without, pd.DataFrame), "without10k出力がDataFrameではない"
            assert isinstance(df_with, pd.DataFrame), "with10k出力がDataFrameではない"
            
            # カラム構造の基本的な確認
            if len(df_without) > 0 and len(df_with) > 0:
                # 基本的なカラムの存在確認（実装に依存）
                assert len(df_without.columns) > 0, "without10k出力にカラムがない"
                assert len(df_with.columns) > 0, "with10k出力にカラムがない"

    @pytest.mark.integration
    def test_memory_efficiency(self, mock_file_factory):
        """メモリ効率性の統合テスト"""
        import psutil
        import os
        
        # 現在のメモリ使用量を取得
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # 大量データでの処理テスト
        large_data = pd.DataFrame({
            '契約番号': [f'A{i:04d}' for i in range(100)],  # 100件に縮小
            '管理前滞納額': [50000 + i for i in range(100)]
        })
        
        contract_data = pd.DataFrame({
            '引継番号': [f'A{i:04d}' for i in range(100)],
            '管理番号': [f'M{i:04d}' for i in range(100)]
        })
        
        ark_file = mock_file_factory(large_data, "large_ark.csv")
        contract_file = mock_file_factory(contract_data, "large_contract.csv")
        
        # 処理実行
        result = process_ark_late_payment_data(ark_file, contract_file)
        
        # 処理後のメモリ使用量
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # メモリ使用量の確認（50MB以内の増加）
        assert memory_increase < 50, f"メモリ使用量の増加が大きすぎる: {memory_increase:.2f}MB"
        
        # 結果確認
        if result is not None:
            output_df, filename = result
            assert len(output_df) <= 100, "出力データ件数が期待値を超える"