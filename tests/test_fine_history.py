"""
ファイン履歴プロセッサのユニットテスト
Business Data Processor

TDDで実装: Red-Green-Refactorサイクル
"""

import pytest
import pandas as pd
from datetime import datetime
from io import StringIO


class TestFineHistoryProcessor:
    """FineHistoryProcessorクラスのテスト"""

    def test_空行除外_管理番号が空の行を除外(self, sample_fine_history_data):
        """管理番号が空の行が除外されることを確認"""
        from processors.fine_history import FineHistoryProcessor

        processor = FineHistoryProcessor(target_person="契約者")
        result_df = processor.process(sample_fine_history_data)

        # 元データ5行中、管理番号が空の行が2行
        # 期待: 3行のみ残る
        assert len(result_df) == 3
        # 空の管理番号がないことを確認
        assert result_df['管理番号'].isna().sum() == 0

    def test_mapping_10列の出力形式(self, sample_fine_history_data):
        """出力が10列のNegotiatesInfo形式であることを確認"""
        from processors.fine_history import FineHistoryProcessor

        processor = FineHistoryProcessor(target_person="契約者")
        result_df = processor.process(sample_fine_history_data)

        # 10列であることを確認
        expected_columns = [
            "管理番号", "交渉日時", "担当", "相手", "手段",
            "回収ランク", "結果", "入金予定日", "予定金額", "交渉備考"
        ]
        assert list(result_df.columns) == expected_columns

    def test_mapping_固定値が正しく設定される(self, sample_fine_history_data):
        """固定値（担当、手段、結果など）が正しく設定されることを確認"""
        from processors.fine_history import FineHistoryProcessor

        processor = FineHistoryProcessor(target_person="保証人")
        result_df = processor.process(sample_fine_history_data)

        # 固定値の確認
        assert all(result_df['担当'] == '')
        assert all(result_df['相手'] == '保証人')
        assert all(result_df['手段'] == '架電')
        assert all(result_df['回収ランク'] == '')
        assert all(result_df['結果'] == 'その他')
        assert all(result_df['入金予定日'] == '')
        assert all(result_df['予定金額'] == '')

    def test_mapping_交渉日時の結合と秒削除(self, sample_fine_history_data):
        """交渉日時が「発信日 発信時刻」形式で結合され、秒が削除されることを確認"""
        from processors.fine_history import FineHistoryProcessor

        processor = FineHistoryProcessor(target_person="契約者")
        result_df = processor.process(sample_fine_history_data)

        # 1行目の確認: 2025/12/9 15:52:27 → 2025/12/9 15:52
        assert result_df.iloc[0]['交渉日時'] == '2025/12/9 15:52'

        # 2行目の確認: 2025/12/9 16:30:45 → 2025/12/9 16:30
        assert result_df.iloc[1]['交渉日時'] == '2025/12/9 16:30'

        # 3行目の確認: 2025/12/10 09:15:00 → 2025/12/10 09:15
        assert result_df.iloc[2]['交渉日時'] == '2025/12/10 09:15'

    def test_mapping_交渉備考の文字列結合(self, sample_fine_history_data):
        """交渉備考が「{架電先}オートコール」形式で結合されることを確認"""
        from processors.fine_history import FineHistoryProcessor

        processor = FineHistoryProcessor(target_person="契約者")
        result_df = processor.process(sample_fine_history_data)

        # 1行目の確認
        assert result_df.iloc[0]['交渉備考'] == '090-1234-5678オートコール'

        # 2行目の確認
        assert result_df.iloc[1]['交渉備考'] == '080-9876-5432オートコール'

        # 3行目の確認
        assert result_df.iloc[2]['交渉備考'] == '070-1111-2222オートコール'

    def test_相手の種類が正しく反映される(self, sample_fine_history_data):
        """相手の種類（契約者/保証人/連絡人/勤務先）が正しく反映されることを確認"""
        from processors.fine_history import FineHistoryProcessor

        # 各相手の種類でテスト
        for target in ["契約者", "保証人", "連絡人", "勤務先"]:
            processor = FineHistoryProcessor(target_person=target)
            result_df = processor.process(sample_fine_history_data)
            assert all(result_df['相手'] == target)

    def test_complete_processing(self, sample_fine_history_data, expected_fine_history_output):
        """全体処理が期待通りに動作することを確認（統合テスト）"""
        from processors.fine_history import FineHistoryProcessor

        processor = FineHistoryProcessor(target_person="契約者")
        result_df = processor.process(sample_fine_history_data)

        # 期待されるDataFrameと比較
        pd.testing.assert_frame_equal(
            result_df.reset_index(drop=True),
            expected_fine_history_output.reset_index(drop=True),
            check_dtype=False
        )


class TestOutputGeneration:
    """出力ファイル名生成のテスト"""

    def test_generate_filename_MMDD形式_csv(self):
        """出力ファイル名がMMDD形式（csv）であることを確認"""
        from processors.fine_history import FineHistoryProcessor

        processor = FineHistoryProcessor(target_person="契約者")
        filename = processor.generate_output_filename(extension='csv')

        # ファイル名が「MMDDファイン履歴.csv」形式であることを確認
        assert filename.endswith("ファイン履歴.csv")

        # MMDDが4桁の数字であることを確認
        mmdd_part = filename.split("ファイン履歴")[0]
        assert len(mmdd_part) == 4
        assert mmdd_part.isdigit()

        # 現在の日付のMMDDと一致することを確認
        today = datetime.now()
        expected_mmdd = today.strftime('%m%d')
        assert mmdd_part == expected_mmdd

    def test_generate_filename_MMDD形式_xlsx(self):
        """出力ファイル名がMMDD形式（xlsx）であることを確認"""
        from processors.fine_history import FineHistoryProcessor

        processor = FineHistoryProcessor(target_person="契約者")
        filename = processor.generate_output_filename(extension='xlsx')

        # ファイル名が「MMDDファイン履歴.xlsx」形式であることを確認
        assert filename.endswith("ファイン履歴.xlsx")


class TestEdgeCases:
    """エッジケースのテスト"""

    def test_必須カラムが存在しない場合(self):
        """必須カラムが存在しない場合、ValueErrorが発生することを確認"""
        from processors.fine_history import FineHistoryProcessor

        # 必須カラムが存在しないデータ（出力ファイル形式）
        data = """管理番号,交渉日時,担当,相手,手段,回収ランク,結果,入金予定日,予定金額,交渉備考
12345,2025/12/9 15:52,,契約者,架電,,その他,,,090-1234-5678オートコール"""

        df = pd.read_csv(StringIO(data))
        processor = FineHistoryProcessor(target_person="契約者")

        with pytest.raises(ValueError) as excinfo:
            processor.process(df)

        assert "必須カラムが見つかりません" in str(excinfo.value)
        assert "携帯Mirail社納品データ" in str(excinfo.value)

    def test_全件空行の場合(self):
        """全レコードが空行の場合、空DataFrameが返されることを確認"""
        from processors.fine_history import FineHistoryProcessor

        # 全件空行のデータ
        data = """管理番号,契約者氏名,契約者カナ,架電先,発信日,発信時刻,発信応対結果コード,発信応対結果名,応対者コード,応対者名,出力日
,,,,,,,,,,
,,,,,,,,,,"""

        df = pd.read_csv(StringIO(data))
        processor = FineHistoryProcessor(target_person="契約者")
        result_df = processor.process(df)

        # 空DataFrameが返されることを確認
        assert len(result_df) == 0
        assert list(result_df.columns) == processor.OUTPUT_COLUMNS

    def test_架電先が空白の場合(self):
        """架電先が空白でも正常に処理されることを確認"""
        from processors.fine_history import FineHistoryProcessor

        data = """管理番号,契約者氏名,契約者カナ,架電先,発信日,発信時刻,発信応対結果コード,発信応対結果名,応対者コード,応対者名,出力日
12345,田中太郎,タナカタロウ,,2025/12/9,15:52:27,S01,コール音,,,2025/12/10 8:20"""

        df = pd.read_csv(StringIO(data))
        processor = FineHistoryProcessor(target_person="契約者")
        result_df = processor.process(df)

        # 架電先が空でも正常に処理される（空文字 + オートコール）
        assert len(result_df) == 1
        assert result_df.iloc[0]['交渉備考'] == 'オートコール'

    def test_発信日が空白の場合(self):
        """発信日が空白でも正常に処理されることを確認"""
        from processors.fine_history import FineHistoryProcessor

        data = """管理番号,契約者氏名,契約者カナ,架電先,発信日,発信時刻,発信応対結果コード,発信応対結果名,応対者コード,応対者名,出力日
12345,田中太郎,タナカタロウ,090-1234-5678,,15:52:27,S01,コール音,,,2025/12/10 8:20"""

        df = pd.read_csv(StringIO(data))
        processor = FineHistoryProcessor(target_person="契約者")
        result_df = processor.process(df)

        # 発信日が空でも正常に処理される
        assert len(result_df) == 1
        # 発信日が空の場合、時刻のみが表示される
        assert '15:52' in result_df.iloc[0]['交渉日時']

    def test_発信時刻が空白の場合(self):
        """発信時刻が空白でも正常に処理されることを確認"""
        from processors.fine_history import FineHistoryProcessor

        data = """管理番号,契約者氏名,契約者カナ,架電先,発信日,発信時刻,発信応対結果コード,発信応対結果名,応対者コード,応対者名,出力日
12345,田中太郎,タナカタロウ,090-1234-5678,2025/12/9,,S01,コール音,,,2025/12/10 8:20"""

        df = pd.read_csv(StringIO(data))
        processor = FineHistoryProcessor(target_person="契約者")
        result_df = processor.process(df)

        # 発信時刻が空でも正常に処理される
        assert len(result_df) == 1
        # 発信時刻が空の場合、日付のみが表示される
        assert result_df.iloc[0]['交渉日時'] == '2025/12/9'

    def test_発信時刻が秒なしの場合(self):
        """発信時刻に秒がない場合でも正常に処理されることを確認"""
        from processors.fine_history import FineHistoryProcessor

        data = """管理番号,契約者氏名,契約者カナ,架電先,発信日,発信時刻,発信応対結果コード,発信応対結果名,応対者コード,応対者名,出力日
12345,田中太郎,タナカタロウ,090-1234-5678,2025/12/9,15:52,S01,コール音,,,2025/12/10 8:20"""

        df = pd.read_csv(StringIO(data))
        processor = FineHistoryProcessor(target_person="契約者")
        result_df = processor.process(df)

        # 秒がなくても正常に処理される
        assert len(result_df) == 1
        assert result_df.iloc[0]['交渉日時'] == '2025/12/9 15:52'


class TestCSVGeneration:
    """CSV生成のテスト"""

    def test_generate_csv_正常系(self, sample_fine_history_data):
        """CSVファイルが正常に生成されることを確認"""
        from processors.fine_history import FineHistoryProcessor

        processor = FineHistoryProcessor(target_person="契約者")
        result_df = processor.process(sample_fine_history_data)
        csv_bytes, logs = processor.generate_csv(result_df)

        # バイト列が返されることを確認
        assert isinstance(csv_bytes, bytes)
        assert len(csv_bytes) > 0

        # ログが返されることを確認
        assert isinstance(logs, list)
        assert len(logs) > 0

        # CSVとしてデコードできることを確認（CP932エンコーディング）
        csv_content = csv_bytes.decode('cp932')
        assert '管理番号' in csv_content
        assert '交渉日時' in csv_content
        assert '12345' in csv_content  # サンプルデータの管理番号

    def test_generate_csv_エンコーディング検証(self, sample_fine_history_data):
        """CP932エンコーディングが正しく適用されることを確認"""
        from processors.fine_history import FineHistoryProcessor

        processor = FineHistoryProcessor(target_person="契約者")
        result_df = processor.process(sample_fine_history_data)
        csv_bytes, logs = processor.generate_csv(result_df)

        # CP932でデコードできることを確認
        try:
            csv_content = csv_bytes.decode('cp932')
            assert '管理番号' in csv_content
            assert '交渉日時' in csv_content
        except UnicodeDecodeError:
            pytest.fail("CP932エンコーディングでデコード失敗")

        # 日本語が含まれることを確認
        assert '契約者' in csv_content or '保証人' in csv_content or '連絡人' in csv_content

    def test_generate_csv_ログ内容確認(self, sample_fine_history_data):
        """生成ログに正しい件数が含まれることを確認"""
        from processors.fine_history import FineHistoryProcessor

        processor = FineHistoryProcessor(target_person="契約者")
        result_df = processor.process(sample_fine_history_data)
        csv_bytes, logs = processor.generate_csv(result_df)

        # ログに件数が含まれることを確認
        assert any('3件' in log for log in logs)
