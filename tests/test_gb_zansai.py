#!/usr/bin/env python3
"""
ガレージバンク残債取り込みプロセッサのテスト

TDD: Red → Green → Refactor
"""

import pytest
import pandas as pd
import io
from datetime import datetime
from unittest.mock import patch, MagicMock


class TestGBZansaiConfig:
    """GBZansaiConfigクラスのテスト"""

    def test_output_columns(self):
        """出力列が正しく定義されていることを確認"""
        from processors.gb_zansai import GBZansaiConfig
        assert GBZansaiConfig.OUTPUT_COLUMNS == ["管理番号", "管理前滞納額"]

    def test_excel_sheet_name(self):
        """Excelシート名が正しく定義されていることを確認"""
        from processors.gb_zansai import GBZansaiConfig
        assert GBZansaiConfig.EXCEL_SHEET_NAME == "01_請求データ"

    def test_output_filename_format(self):
        """出力ファイル名フォーマットが正しいことを確認"""
        from processors.gb_zansai import GBZansaiConfig
        assert "ガレージバンク管理前取込" in GBZansaiConfig.OUTPUT_FILENAME_FORMAT
        assert "{date}" in GBZansaiConfig.OUTPUT_FILENAME_FORMAT


class TestReadContractList:
    """ContractList読み込みのテスト"""

    def test_read_contract_list_basic(self):
        """ContractListの基本的な読み込みテスト"""
        from processors.gb_zansai import read_contract_list

        # cp932エンコードのCSVデータを作成
        csv_content = '"管理番号","引継番号","最新契約種類"\n"80451","266798","バックレント"\n"80450","266454","バックレント"'
        csv_bytes = csv_content.encode('cp932')
        mock_file = MagicMock()
        mock_file.read.return_value = csv_bytes

        df = read_contract_list(mock_file)

        assert len(df) == 2
        assert "管理番号" in df.columns
        assert "引継番号" in df.columns

    def test_read_contract_list_extracts_kanri_hikitsugi(self):
        """管理番号と引継番号が正しく抽出されることを確認"""
        from processors.gb_zansai import read_contract_list

        csv_content = '"管理番号","引継番号","最新契約種類"\n"80451","266798","バックレント"\n"80450","266454","バックレント"'
        csv_bytes = csv_content.encode('cp932')
        mock_file = MagicMock()
        mock_file.read.return_value = csv_bytes

        df = read_contract_list(mock_file)

        assert df.iloc[0]["管理番号"] == "80451"
        assert df.iloc[0]["引継番号"] == "266798"
        assert df.iloc[1]["管理番号"] == "80450"
        assert df.iloc[1]["引継番号"] == "266454"

    def test_read_contract_list_handles_japanese_characters(self):
        """日本語文字が正しく処理されることを確認"""
        from processors.gb_zansai import read_contract_list

        csv_content = '"管理番号","引継番号","契約者氏名"\n"80451","266798","山崎真"\n"80450","266454","杉本謙史朗"'
        csv_bytes = csv_content.encode('cp932')
        mock_file = MagicMock()
        mock_file.read.return_value = csv_bytes

        df = read_contract_list(mock_file)

        assert df.iloc[0]["契約者氏名"] == "山崎真"
        assert df.iloc[1]["契約者氏名"] == "杉本謙史朗"


class TestReadSeikyuData:
    """請求データ読み込みのテスト"""

    def test_read_seikyu_data_basic(self):
        """請求データの基本的な読み込みテスト"""
        from processors.gb_zansai import read_seikyu_data

        # テスト用のDataFrame作成（Excelから読み込んだことを想定）
        test_data = {
            "ユーザーID": [264048, 58801, 210581],
            "請求総額": [25596, 180639, 54821]
        }
        test_df = pd.DataFrame(test_data)

        # BytesIO でExcelファイルを模擬
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            test_df.to_excel(writer, sheet_name='01_請求データ', index=False)
        excel_buffer.seek(0)

        mock_file = MagicMock()
        mock_file.read.return_value = excel_buffer.getvalue()

        df = read_seikyu_data(mock_file)

        assert len(df) == 3
        assert "ユーザーID" in df.columns
        assert "請求総額" in df.columns

    def test_read_seikyu_data_extracts_user_id_and_amount(self):
        """ユーザーIDと請求総額が正しく抽出されることを確認"""
        from processors.gb_zansai import read_seikyu_data

        test_data = {
            "ユーザーID": [264048, 58801],
            "請求総額": [25596, 180639]
        }
        test_df = pd.DataFrame(test_data)

        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            test_df.to_excel(writer, sheet_name='01_請求データ', index=False)
        excel_buffer.seek(0)

        mock_file = MagicMock()
        mock_file.read.return_value = excel_buffer.getvalue()

        df = read_seikyu_data(mock_file)

        assert df.iloc[0]["ユーザーID"] == 264048
        assert df.iloc[0]["請求総額"] == 25596
        assert df.iloc[1]["ユーザーID"] == 58801
        assert df.iloc[1]["請求総額"] == 180639


class TestMatchingLogic:
    """マッチング処理のテスト"""

    def test_matching_basic(self):
        """基本的なマッチングテスト"""
        from processors.gb_zansai import match_data

        # ContractList
        contract_data = {
            "管理番号": ["80444", "80314", "80344"],
            "引継番号": ["264048", "58801", "210581"]
        }
        contract_df = pd.DataFrame(contract_data)

        # 請求データ
        seikyu_data = {
            "ユーザーID": [264048, 58801],
            "請求総額": [25596, 180639]
        }
        seikyu_df = pd.DataFrame(seikyu_data)

        result_df, logs = match_data(seikyu_df, contract_df)

        assert len(result_df) == 2
        assert result_df.iloc[0]["管理番号"] == "80444"
        assert result_df.iloc[0]["管理前滞納額"] == 25596
        assert result_df.iloc[1]["管理番号"] == "80314"
        assert result_df.iloc[1]["管理前滞納額"] == 180639

    def test_matching_with_unmatched_records(self):
        """マッチしないレコードがある場合のテスト"""
        from processors.gb_zansai import match_data

        # ContractList（264048のみ存在）
        contract_data = {
            "管理番号": ["80444"],
            "引継番号": ["264048"]
        }
        contract_df = pd.DataFrame(contract_data)

        # 請求データ（264048と999999）
        seikyu_data = {
            "ユーザーID": [264048, 999999],
            "請求総額": [25596, 10000]
        }
        seikyu_df = pd.DataFrame(seikyu_data)

        result_df, logs = match_data(seikyu_df, contract_df)

        # マッチしたのは1件のみ
        assert len(result_df) == 1
        assert result_df.iloc[0]["管理番号"] == "80444"

        # ログにマッチしなかったレコードが記録されている
        unmatched_logs = [log for log in logs if "マッチしませんでした" in log]
        assert len(unmatched_logs) == 1
        assert "999999" in unmatched_logs[0]

    def test_matching_all_unmatched(self):
        """全てマッチしない場合のテスト"""
        from processors.gb_zansai import match_data

        contract_data = {
            "管理番号": ["80444"],
            "引継番号": ["264048"]
        }
        contract_df = pd.DataFrame(contract_data)

        seikyu_data = {
            "ユーザーID": [999998, 999999],
            "請求総額": [10000, 20000]
        }
        seikyu_df = pd.DataFrame(seikyu_data)

        result_df, logs = match_data(seikyu_df, contract_df)

        assert len(result_df) == 0
        unmatched_logs = [log for log in logs if "マッチしませんでした" in log]
        assert len(unmatched_logs) == 2

    def test_matching_logs_include_user_id(self):
        """マッチしなかったユーザーIDがログに含まれることを確認"""
        from processors.gb_zansai import match_data

        contract_data = {
            "管理番号": ["80444"],
            "引継番号": ["264048"]
        }
        contract_df = pd.DataFrame(contract_data)

        seikyu_data = {
            "ユーザーID": [123456],
            "請求総額": [10000]
        }
        seikyu_df = pd.DataFrame(seikyu_data)

        result_df, logs = match_data(seikyu_df, contract_df)

        assert any("123456" in log for log in logs)


class TestOutputGeneration:
    """出力CSV生成のテスト"""

    def test_output_columns_order(self):
        """出力列の順序が正しいことを確認"""
        from processors.gb_zansai import generate_output

        data = {
            "管理番号": ["80444", "80314"],
            "管理前滞納額": [25596, 180639]
        }
        df = pd.DataFrame(data)

        output_df = generate_output(df)

        assert list(output_df.columns) == ["管理番号", "管理前滞納額"]

    def test_output_values_preserved(self):
        """出力値が正しく保持されることを確認"""
        from processors.gb_zansai import generate_output

        data = {
            "管理番号": ["80444", "80314"],
            "管理前滞納額": [25596, 180639]
        }
        df = pd.DataFrame(data)

        output_df = generate_output(df)

        assert output_df.iloc[0]["管理番号"] == "80444"
        assert output_df.iloc[0]["管理前滞納額"] == 25596
        assert output_df.iloc[1]["管理番号"] == "80314"
        assert output_df.iloc[1]["管理前滞納額"] == 180639

    def test_output_handles_zero_amount(self):
        """金額が0の場合も正しく出力されることを確認"""
        from processors.gb_zansai import generate_output

        data = {
            "管理番号": ["80444"],
            "管理前滞納額": [0]
        }
        df = pd.DataFrame(data)

        output_df = generate_output(df)

        assert output_df.iloc[0]["管理前滞納額"] == 0


class TestProcessGbZansai:
    """統合テスト: process_gb_zansai関数"""

    def test_process_gb_zansai_basic(self):
        """基本的な処理フローのテスト"""
        from processors.gb_zansai import process_gb_zansai

        # ContractList（cp932）
        contract_csv = '"管理番号","引継番号","最新契約種類"\n"80444","264048","バックレント"\n"80314","58801","バックレント"'
        contract_bytes = contract_csv.encode('cp932')
        mock_contract_file = MagicMock()
        mock_contract_file.read.return_value = contract_bytes

        # 請求データ（Excel）
        seikyu_data = {
            "ユーザーID": [264048, 58801],
            "請求総額": [25596, 180639]
        }
        seikyu_df = pd.DataFrame(seikyu_data)
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            seikyu_df.to_excel(writer, sheet_name='01_請求データ', index=False)
        excel_buffer.seek(0)
        mock_seikyu_file = MagicMock()
        mock_seikyu_file.read.return_value = excel_buffer.getvalue()

        output_df, logs, filename = process_gb_zansai(mock_seikyu_file, mock_contract_file)

        assert len(output_df) == 2
        assert "管理番号" in output_df.columns
        assert "管理前滞納額" in output_df.columns
        assert "ガレージバンク管理前取込" in filename
        assert filename.endswith(".csv")

    def test_process_gb_zansai_with_unmatched(self):
        """マッチしないレコードがある場合の統合テスト"""
        from processors.gb_zansai import process_gb_zansai

        # ContractList
        contract_csv = '"管理番号","引継番号","最新契約種類"\n"80444","264048","バックレント"'
        contract_bytes = contract_csv.encode('cp932')
        mock_contract_file = MagicMock()
        mock_contract_file.read.return_value = contract_bytes

        # 請求データ（1件はマッチ、1件はマッチしない）
        seikyu_data = {
            "ユーザーID": [264048, 999999],
            "請求総額": [25596, 10000]
        }
        seikyu_df = pd.DataFrame(seikyu_data)
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            seikyu_df.to_excel(writer, sheet_name='01_請求データ', index=False)
        excel_buffer.seek(0)
        mock_seikyu_file = MagicMock()
        mock_seikyu_file.read.return_value = excel_buffer.getvalue()

        output_df, logs, filename = process_gb_zansai(mock_seikyu_file, mock_contract_file)

        # 出力は1件
        assert len(output_df) == 1
        assert output_df.iloc[0]["管理番号"] == "80444"

        # ログにマッチしなかったレコードが記録
        assert any("999999" in log for log in logs)
        assert any("マッチしませんでした" in log for log in logs)

    def test_process_gb_zansai_filename_format(self):
        """出力ファイル名のフォーマットテスト"""
        from processors.gb_zansai import process_gb_zansai

        contract_csv = '"管理番号","引継番号"\n"80444","264048"'
        contract_bytes = contract_csv.encode('cp932')
        mock_contract_file = MagicMock()
        mock_contract_file.read.return_value = contract_bytes

        seikyu_data = {"ユーザーID": [264048], "請求総額": [25596]}
        seikyu_df = pd.DataFrame(seikyu_data)
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            seikyu_df.to_excel(writer, sheet_name='01_請求データ', index=False)
        excel_buffer.seek(0)
        mock_seikyu_file = MagicMock()
        mock_seikyu_file.read.return_value = excel_buffer.getvalue()

        with patch('processors.gb_zansai.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2026, 1, 21)
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            output_df, logs, filename = process_gb_zansai(mock_seikyu_file, mock_contract_file)

        assert filename == "ガレージバンク管理前取込_20260121.csv"

    def test_process_gb_zansai_logs_summary(self):
        """処理ログのサマリーが正しいことを確認"""
        from processors.gb_zansai import process_gb_zansai

        contract_csv = '"管理番号","引継番号"\n"80444","264048"\n"80314","58801"'
        contract_bytes = contract_csv.encode('cp932')
        mock_contract_file = MagicMock()
        mock_contract_file.read.return_value = contract_bytes

        seikyu_data = {"ユーザーID": [264048, 58801, 999999], "請求総額": [25596, 180639, 10000]}
        seikyu_df = pd.DataFrame(seikyu_data)
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            seikyu_df.to_excel(writer, sheet_name='01_請求データ', index=False)
        excel_buffer.seek(0)
        mock_seikyu_file = MagicMock()
        mock_seikyu_file.read.return_value = excel_buffer.getvalue()

        output_df, logs, filename = process_gb_zansai(mock_seikyu_file, mock_contract_file)

        # ログに処理サマリーが含まれる
        assert any("請求データ" in log and "3" in log for log in logs)  # 入力件数
        assert any("マッチ" in log and "2" in log for log in logs)  # マッチ件数


class TestEdgeCases:
    """境界値・異常系のテスト"""

    def test_empty_seikyu_data(self):
        """請求データが空の場合のテスト"""
        from processors.gb_zansai import process_gb_zansai

        contract_csv = '"管理番号","引継番号"\n"80444","264048"'
        contract_bytes = contract_csv.encode('cp932')
        mock_contract_file = MagicMock()
        mock_contract_file.read.return_value = contract_bytes

        seikyu_data = {"ユーザーID": [], "請求総額": []}
        seikyu_df = pd.DataFrame(seikyu_data)
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            seikyu_df.to_excel(writer, sheet_name='01_請求データ', index=False)
        excel_buffer.seek(0)
        mock_seikyu_file = MagicMock()
        mock_seikyu_file.read.return_value = excel_buffer.getvalue()

        output_df, logs, filename = process_gb_zansai(mock_seikyu_file, mock_contract_file)

        assert len(output_df) == 0

    def test_empty_contract_list(self):
        """ContractListが空の場合のテスト"""
        from processors.gb_zansai import process_gb_zansai

        contract_csv = '"管理番号","引継番号"'
        contract_bytes = contract_csv.encode('cp932')
        mock_contract_file = MagicMock()
        mock_contract_file.read.return_value = contract_bytes

        seikyu_data = {"ユーザーID": [264048], "請求総額": [25596]}
        seikyu_df = pd.DataFrame(seikyu_data)
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            seikyu_df.to_excel(writer, sheet_name='01_請求データ', index=False)
        excel_buffer.seek(0)
        mock_seikyu_file = MagicMock()
        mock_seikyu_file.read.return_value = excel_buffer.getvalue()

        output_df, logs, filename = process_gb_zansai(mock_seikyu_file, mock_contract_file)

        assert len(output_df) == 0
        assert any("マッチしませんでした" in log for log in logs)

    def test_user_id_as_string_in_contract_list(self):
        """ContractListの引継番号が文字列型の場合のマッチングテスト"""
        from processors.gb_zansai import match_data

        # ContractList（引継番号が文字列）
        contract_data = {
            "管理番号": ["80444"],
            "引継番号": ["264048"]  # 文字列
        }
        contract_df = pd.DataFrame(contract_data)

        # 請求データ（ユーザーIDが数値）
        seikyu_data = {
            "ユーザーID": [264048],  # 数値
            "請求総額": [25596]
        }
        seikyu_df = pd.DataFrame(seikyu_data)

        result_df, logs = match_data(seikyu_df, contract_df)

        # 型が違ってもマッチすること
        assert len(result_df) == 1
        assert result_df.iloc[0]["管理番号"] == "80444"

    def test_large_amount(self):
        """大きな金額のテスト"""
        from processors.gb_zansai import match_data

        contract_data = {
            "管理番号": ["80444"],
            "引継番号": ["264048"]
        }
        contract_df = pd.DataFrame(contract_data)

        seikyu_data = {
            "ユーザーID": [264048],
            "請求総額": [999999999]  # 大きな金額
        }
        seikyu_df = pd.DataFrame(seikyu_data)

        result_df, logs = match_data(seikyu_df, contract_df)

        assert result_df.iloc[0]["管理前滞納額"] == 999999999
