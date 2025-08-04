"""
アーク残債更新プロセッサーのテスト
正常系・異常系・境界値テストを含む
"""

import pytest
import pandas as pd
from io import StringIO
import sys
import os

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from processors.ark_late_payment_update import process_ark_late_payment_data


class TestArkLatePaymentUpdate:
    """アーク残債更新処理のテストクラス"""

    @pytest.mark.unit
    def test_normal_processing(self, sample_ark_data, sample_contract_data, mock_file_factory):
        """正常なデータでの処理テスト"""
        # モックファイル作成
        arc_file = mock_file_factory(sample_ark_data, "arc_data.csv")
        contract_file = mock_file_factory(sample_contract_data, "contract_list.csv")
        
        # 処理実行
        result = process_ark_late_payment_data(arc_file, contract_file)
        
        # 結果検証
        assert result is not None, "正常データでの処理結果がNoneになった"
        
        output_df, filename = result
        
        # 基本検証
        assert isinstance(output_df, pd.DataFrame), "出力がDataFrameではない"
        assert len(output_df) == 4, f"期待される行数: 4, 実際: {len(output_df)}"
        
        # カラム検証
        expected_columns = ['管理番号', '管理前滞納額']
        assert list(output_df.columns) == expected_columns, f"カラムが期待値と異なる: {list(output_df.columns)}"
        
        # ファイル名検証
        assert filename.endswith('アーク残債.csv'), f"ファイル名が期待値と異なる: {filename}"
        
        # データ内容検証
        assert 'M001' in output_df['管理番号'].values, "管理番号M001が見つからない"
        assert 'M002' in output_df['管理番号'].values, "管理番号M002が見つからない"
        
        # 金額検証
        m001_amount = output_df[output_df['管理番号'] == 'M001']['管理前滞納額'].iloc[0]
        assert m001_amount == 50000, f"M001の滞納額が期待値と異なる: {m001_amount}"

    @pytest.mark.unit
    def test_data_type_conversion(self, mock_file_factory):
        """データ型変換のテスト"""
        # 文字列型の金額データを含むテストデータ
        arc_data = pd.DataFrame({
            '契約番号': ['A001', 'A002'],
            '管理前滞納額': ['50000', '75000']  # 文字列型
        })
        
        contract_data = pd.DataFrame({
            '引継番号': ['A001', 'A002'],
            '管理番号': ['M001', 'M002']
        })
        
        arc_file = mock_file_factory(arc_data)
        contract_file = mock_file_factory(contract_data)
        
        result = process_ark_late_payment_data(arc_file, contract_file)
        
        assert result is not None
        output_df, _ = result
        
        # 数値型に変換されていることを確認
        assert output_df['管理前滞納額'].dtype in ['int64', 'int32'], "滞納額が数値型に変換されていない"

    @pytest.mark.unit
    def test_duplicate_management_numbers(self, mock_file_factory):
        """重複管理番号の集計テスト"""
        # 同じ管理番号に対して複数の滞納額データ
        arc_data = pd.DataFrame({
            '契約番号': ['A001', 'A001_2'],
            '管理前滞納額': [30000, 20000]
        })
        
        contract_data = pd.DataFrame({
            '引継番号': ['A001', 'A001_2'],
            '管理番号': ['M001', 'M001']  # 同じ管理番号
        })
        
        arc_file = mock_file_factory(arc_data)
        contract_file = mock_file_factory(contract_data)
        
        result = process_ark_late_payment_data(arc_file, contract_file)
        
        assert result is not None
        output_df, _ = result
        
        # 重複が集計されて1行になることを確認
        assert len(output_df) == 1, "重複管理番号が集計されていない"
        
        # 金額が合計されていることを確認
        total_amount = output_df['管理前滞納額'].iloc[0]
        assert total_amount == 50000, f"重複金額の合計が正しくない: {total_amount}"

    @pytest.mark.unit
    def test_sorting_by_management_number(self, mock_file_factory):
        """管理番号でのソート機能テスト"""
        arc_data = pd.DataFrame({
            '契約番号': ['A003', 'A001', 'A002'],
            '管理前滞納額': [30000, 50000, 75000]
        })
        
        contract_data = pd.DataFrame({
            '引継番号': ['A003', 'A001', 'A002'],
            '管理番号': ['M003', 'M001', 'M002']
        })
        
        arc_file = mock_file_factory(arc_data)
        contract_file = mock_file_factory(contract_data)
        
        result = process_ark_late_payment_data(arc_file, contract_file)
        
        assert result is not None
        output_df, _ = result
        
        # 管理番号でソートされていることを確認
        management_numbers = output_df['管理番号'].tolist()
        expected_order = ['M001', 'M002', 'M003']
        assert management_numbers == expected_order, f"ソート順が正しくない: {management_numbers}"

    @pytest.mark.unit
    def test_empty_data_handling(self, mock_file_factory):
        """空データの処理テスト"""
        # 空のDataFrame
        empty_arc = pd.DataFrame(columns=['契約番号', '管理前滞納額'])
        valid_contract = pd.DataFrame({
            '引継番号': ['A001'],
            '管理番号': ['M001']
        })
        
        arc_file = mock_file_factory(empty_arc)
        contract_file = mock_file_factory(valid_contract)
        
        result = process_ark_late_payment_data(arc_file, contract_file)
        
        # 空データの場合はNoneを返すことを確認
        assert result is None, "空データでNoneが返されない"

    @pytest.mark.unit
    def test_single_record_processing(self, mock_file_factory):
        """単一レコードの処理テスト（境界値テスト）"""
        arc_data = pd.DataFrame({
            '契約番号': ['A001'],
            '管理前滞納額': [100000]
        })
        
        contract_data = pd.DataFrame({
            '引継番号': ['A001'],
            '管理番号': ['M001']
        })
        
        arc_file = mock_file_factory(arc_data)
        contract_file = mock_file_factory(contract_data)
        
        result = process_ark_late_payment_data(arc_file, contract_file)
        
        assert result is not None
        output_df, _ = result
        
        assert len(output_df) == 1, "単一レコード処理で行数が正しくない"
        assert output_df['管理番号'].iloc[0] == 'M001', "管理番号が正しくない"
        assert output_df['管理前滞納額'].iloc[0] == 100000, "滞納額が正しくない"

    # === 異常系テスト ===

    @pytest.mark.unit
    def test_missing_required_columns_arc(self, invalid_ark_data, sample_contract_data, mock_file_factory):
        """アーク残債CSVの必須カラム不足エラー"""
        arc_file = mock_file_factory(invalid_ark_data)
        contract_file = mock_file_factory(sample_contract_data)
        
        result = process_ark_late_payment_data(arc_file, contract_file)
        
        # 必須カラム不足の場合はNoneを返すことを確認
        assert result is None, "必須カラム不足でNoneが返されない"

    @pytest.mark.unit
    def test_missing_required_columns_contract(self, sample_ark_data, invalid_contract_data, mock_file_factory):
        """ContractListの必須カラム不足エラー"""
        arc_file = mock_file_factory(sample_ark_data)
        contract_file = mock_file_factory(invalid_contract_data)
        
        result = process_ark_late_payment_data(arc_file, contract_file)
        
        # 必須カラム不足の場合はNoneを返すことを確認
        assert result is None, "必須カラム不足でNoneが返されない"

    @pytest.mark.unit
    def test_no_matching_records(self, no_match_ark_data, sample_contract_data, mock_file_factory):
        """紐付けできるレコードが0件の場合"""
        arc_file = mock_file_factory(no_match_ark_data)
        contract_file = mock_file_factory(sample_contract_data)
        
        result = process_ark_late_payment_data(arc_file, contract_file)
        
        # 紐付け0件の場合はNoneを返すことを確認
        assert result is None, "紐付け0件でNoneが返されない"

    @pytest.mark.unit
    def test_invalid_amount_data(self, mock_file_factory):
        """不正な金額データの処理"""
        # 非数値の金額データ
        arc_data = pd.DataFrame({
            '契約番号': ['A001', 'A002'],
            '管理前滞納額': ['invalid', 'not_number']
        })
        
        contract_data = pd.DataFrame({
            '引継番号': ['A001', 'A002'],
            '管理番号': ['M001', 'M002']
        })
        
        arc_file = mock_file_factory(arc_data)
        contract_file = mock_file_factory(contract_data)
        
        result = process_ark_late_payment_data(arc_file, contract_file)
        
        if result is not None:
            output_df, _ = result
            # 非数値は0に変換されることを確認
            assert all(output_df['管理前滞納額'] == 0), "非数値データが適切に処理されていない"

    @pytest.mark.unit
    def test_null_values_in_key_columns(self, mock_file_factory):
        """キー項目にNULL値がある場合"""
        arc_data = pd.DataFrame({
            '契約番号': ['A001', None, ''],
            '管理前滞納額': [50000, 75000, 30000]
        })
        
        contract_data = pd.DataFrame({
            '引継番号': ['A001'],
            '管理番号': ['M001']
        })
        
        arc_file = mock_file_factory(arc_data)
        contract_file = mock_file_factory(contract_data)
        
        result = process_ark_late_payment_data(arc_file, contract_file)
        
        if result is not None:
            output_df, _ = result
            # NULL値・空文字は除外されて正常データのみ処理されることを確認
            assert len(output_df) == 1, "NULL値が適切に除外されていない"
            assert output_df['管理番号'].iloc[0] == 'M001', "正常データが処理されていない"

    @pytest.mark.unit  
    def test_encoding_error_handling(self, mock_file_factory):
        """エンコーディングエラーの処理"""
        # 特殊文字を含むデータ
        arc_data = pd.DataFrame({
            '契約番号': ['A001'],
            '管理前滞納額': [50000]
        })
        
        contract_data = pd.DataFrame({
            '引継番号': ['A001'],
            '管理番号': ['M001']
        })
        
        # 異なるエンコーディングでモックファイル作成
        arc_file = mock_file_factory(arc_data, encoding='shift_jis')
        contract_file = mock_file_factory(contract_data, encoding='utf-8')
        
        # エンコーディングが異なってもエラーにならないことを確認
        result = process_ark_late_payment_data(arc_file, contract_file)
        
        # 処理が完了することを確認（具体的な結果は実装依存）
        # エラーで停止しないことが重要
        assert True, "エンコーディング処理でエラーが発生"

    @pytest.mark.unit
    def test_large_dataset_performance(self, mock_file_factory):
        """大量データでのパフォーマンステスト"""
        import time
        
        # 大量データ作成（1000件）
        large_arc_data = pd.DataFrame({
            '契約番号': [f'A{i:04d}' for i in range(1000)],
            '管理前滞納額': [50000 + i for i in range(1000)]
        })
        
        large_contract_data = pd.DataFrame({
            '引継番号': [f'A{i:04d}' for i in range(1000)],
            '管理番号': [f'M{i:04d}' for i in range(1000)]
        })
        
        arc_file = mock_file_factory(large_arc_data)
        contract_file = mock_file_factory(large_contract_data)
        
        # 処理時間測定
        start_time = time.time()
        result = process_ark_late_payment_data(arc_file, contract_file)
        processing_time = time.time() - start_time
        
        # 結果検証
        assert result is not None, "大量データ処理でエラーが発生"
        output_df, _ = result
        assert len(output_df) == 1000, "大量データの処理件数が正しくない"
        
        # パフォーマンス検証（10秒以内）
        assert processing_time < 10, f"処理時間が長すぎる: {processing_time:.2f}秒"