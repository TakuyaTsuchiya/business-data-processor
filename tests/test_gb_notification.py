"""
ガレージバンク催告書 契約者 プロセッサのテスト

TDD: Red-Green-Refactor
- テストが失敗する状態から始める
- 境界値、異常系、エラー系を網羅
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class TestGbNotificationProcessor:
    """ガレージバンク催告書プロセッサのテスト"""

    @pytest.fixture
    def sample_contract_list(self):
        """テスト用ContractListデータ（121列）"""
        # 必要な列位置にデータを配置
        # 0: 管理番号, 1: 引継番号, 20: 契約者氏名, 22-25: 住所
        # 34-40: 口座情報, 71: 滞納残債, 72: 入金予定日, 73: 入金予定金額
        # 86: 回収ランク, 92-96: 物件情報, 118: 委託先法人ID

        data = []
        # 行1: 正常データ（全条件クリア）
        row1 = [None] * 121
        row1[0] = '100001'  # 管理番号
        row1[1] = '266798'  # 引継番号（マッチする）
        row1[20] = '山田太郎'  # 契約者氏名
        row1[22] = '100-0001'  # 郵便番号
        row1[23] = '東京都'  # 現住所1
        row1[24] = '千代田区'  # 現住所2
        row1[25] = '1-1-1'  # 現住所3
        row1[34] = '0001'  # 回収口座銀行CD
        row1[35] = 'みずほ銀行'  # 回収口座銀行名
        row1[36] = '001'  # 回収口座支店CD
        row1[37] = '本店'  # 回収口座支店名
        row1[38] = '普通'  # 回収口座種類
        row1[39] = '1234567'  # 回収口座番号
        row1[40] = 'ヤマダタロウ'  # 回収口座名義人
        row1[71] = '50,000'  # 滞納残債（1円以上）
        row1[72] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  # 入金予定日（過去）
        row1[73] = 1  # 入金予定金額（2,3,5以外）
        row1[86] = '通常'  # 回収ランク（除外対象外）
        row1[92] = '東京都'  # 物件住所1
        row1[93] = '新宿区'  # 物件住所2
        row1[94] = '2-2-2'  # 物件住所3
        row1[95] = 'サンプルマンション'  # 物件名
        row1[96] = '101'  # 物件番号
        row1[118] = 7  # 委託先法人ID
        data.append(row1)

        # 行2: 委託先法人ID != 7（除外される）
        row2 = row1.copy()
        row2[0] = '100002'
        row2[1] = '266454'
        row2[118] = 1  # 委託先法人ID != 7
        data.append(row2)

        # 行3: 滞納残債 = 0（除外される）
        row3 = row1.copy()
        row3[0] = '100003'
        row3[1] = '266176'
        row3[71] = '0'  # 滞納残債 = 0
        data.append(row3)

        # 行4: 入金予定金額 = 2（除外される）
        row4 = row1.copy()
        row4[0] = '100004'
        row4[1] = '265395'
        row4[73] = 2  # 入金予定金額 = 2（除外）
        data.append(row4)

        # 行5: 回収ランク = 死亡決定（除外される）
        row5 = row1.copy()
        row5[0] = '100005'
        row5[1] = '264048'
        row5[86] = '死亡決定'  # 除外対象
        data.append(row5)

        # 行6: 住所不完全（除外される）
        row6 = row1.copy()
        row6[0] = '100006'
        row6[1] = '58801'
        row6[22] = ''  # 郵便番号が空
        data.append(row6)

        # 行7: マッチしない引継番号（除外される）
        row7 = row1.copy()
        row7[0] = '100007'
        row7[1] = '999999'  # マッチしない
        data.append(row7)

        # 行8: 入金予定日が空（通過する）
        row8 = row1.copy()
        row8[0] = '100008'
        row8[1] = '210581'
        row8[72] = np.nan  # 入金予定日が空（空白含むなので通過）
        data.append(row8)

        return pd.DataFrame(data)

    @pytest.fixture
    def sample_billing_data(self):
        """テスト用請求データ（情報連携シート 01_請求データ）"""
        return pd.DataFrame({
            'ユーザーID': [266798, 266454, 266176, 265395, 264048, 58801, 210581],
            'ユーザーID（アプリ表示ID）': ['A', 'B', 'C', 'D', 'E', 'F', 'G'],
            '姓': ['山田', '鈴木', '佐藤', '田中', '高橋', '伊藤', '渡辺'],
            '名': ['太郎', '次郎', '三郎', '四郎', '五郎', '六郎', '七郎'],
            '請求総額': [50000, 30000, 20000, 40000, 60000, 70000, 80000],
            '請求金額': [48000, 29000, 19000, 38000, 58000, 68000, 78000],
            '遅延損害金額': [1500, 800, 700, 1200, 1800, 1500, 1700],
            'その他費用': [500, 200, 300, 800, 200, 500, 300],
            '預かり金（未充当金額合計）': [0, 0, 0, 0, 0, 0, 0],
            '委託日時': ['2026-01-13'] * 7
        })

    def test_matching_by_user_id(self, sample_contract_list, sample_billing_data):
        """引継番号とユーザーIDでマッチングできること"""
        from processors.gb_notification import match_billing_data

        result = match_billing_data(sample_contract_list, sample_billing_data)

        # マッチしたデータに請求金額が追加されていること
        matched_row = result[result.iloc[:, 1].astype(str) == '266798']
        assert len(matched_row) == 1
        assert '請求金額' in result.columns
        assert matched_row['請求金額'].values[0] == 48000

    def test_unmatched_records_excluded(self, sample_contract_list, sample_billing_data):
        """マッチしないレコードが除外されること"""
        from processors.gb_notification import match_billing_data

        result = match_billing_data(sample_contract_list, sample_billing_data)

        # 引継番号999999はマッチしないので除外される
        unmatched = result[result.iloc[:, 1].astype(str) == '999999']
        assert len(unmatched) == 0

    def test_filter_by_client_id(self, sample_contract_list, sample_billing_data):
        """委託先法人ID=7でフィルタされること"""
        from processors.gb_notification import apply_gb_filters

        # まずマッチング
        from processors.gb_notification import match_billing_data
        matched = match_billing_data(sample_contract_list, sample_billing_data)

        # フィルタ適用
        result, logs = apply_gb_filters(matched)

        # 委託先法人ID != 7 のデータが除外されていること
        excluded_row = result[result.iloc[:, 1].astype(str) == '266454']
        assert len(excluded_row) == 0

    def test_filter_by_arrears(self, sample_contract_list, sample_billing_data):
        """滞納残債 >= 1円でフィルタされること"""
        from processors.gb_notification import match_billing_data, apply_gb_filters

        matched = match_billing_data(sample_contract_list, sample_billing_data)
        result, logs = apply_gb_filters(matched)

        # 滞納残債 = 0 のデータが除外されていること
        excluded_row = result[result.iloc[:, 1].astype(str) == '266176']
        assert len(excluded_row) == 0

    def test_filter_exclude_payment_amounts(self, sample_contract_list, sample_billing_data):
        """入金予定金額 2,3,5 が除外されること"""
        from processors.gb_notification import match_billing_data, apply_gb_filters

        matched = match_billing_data(sample_contract_list, sample_billing_data)
        result, logs = apply_gb_filters(matched)

        # 入金予定金額 = 2 のデータが除外されていること
        excluded_row = result[result.iloc[:, 1].astype(str) == '265395']
        assert len(excluded_row) == 0

    def test_filter_exclude_collection_ranks(self, sample_contract_list, sample_billing_data):
        """回収ランク 死亡決定,破産決定,弁護士介入 が除外されること"""
        from processors.gb_notification import match_billing_data, apply_gb_filters

        matched = match_billing_data(sample_contract_list, sample_billing_data)
        result, logs = apply_gb_filters(matched)

        # 回収ランク = 死亡決定 のデータが除外されていること
        excluded_row = result[result.iloc[:, 1].astype(str) == '264048']
        assert len(excluded_row) == 0

    def test_filter_incomplete_address(self, sample_contract_list, sample_billing_data):
        """住所不完全なレコードが除外されること"""
        from processors.gb_notification import match_billing_data, apply_gb_filters, filter_complete_address

        matched = match_billing_data(sample_contract_list, sample_billing_data)
        filtered, _ = apply_gb_filters(matched)
        result, logs = filter_complete_address(filtered)

        # 郵便番号が空のデータが除外されていること
        excluded_row = result[result.iloc[:, 1].astype(str) == '58801']
        assert len(excluded_row) == 0

    def test_payment_date_empty_passes(self, sample_contract_list, sample_billing_data):
        """入金予定日が空のレコードは通過すること"""
        from processors.gb_notification import match_billing_data, apply_gb_filters

        matched = match_billing_data(sample_contract_list, sample_billing_data)
        result, logs = apply_gb_filters(matched)

        # 入金予定日が空でも通過する（他の条件を満たしていれば）
        passed_row = result[result.iloc[:, 1].astype(str) == '210581']
        assert len(passed_row) == 1

    def test_output_columns_count(self, sample_contract_list, sample_billing_data):
        """出力カラムが22列であること"""
        from processors.gb_notification import process_gb_notification

        result_df, filename, message, logs = process_gb_notification(
            sample_contract_list, sample_billing_data
        )

        assert len(result_df.columns) == 22

    def test_output_columns_names(self, sample_contract_list, sample_billing_data):
        """出力カラム名が正しいこと"""
        from processors.gb_notification import process_gb_notification

        result_df, filename, message, logs = process_gb_notification(
            sample_contract_list, sample_billing_data
        )

        expected_columns = [
            '管理番号', '契約者氏名', '郵便番号', '現住所1', '現住所2', '現住所3',
            '滞納残債', '請求金額', '遅延損害金額', 'その他費用',
            '物件住所1', '物件住所2', '物件住所3', '物件名', '物件番号',
            '回収口座銀行CD', '回収口座銀行名', '回収口座支店CD', '回収口座支店名',
            '回収口座種類', '回収口座番号', '回収口座名義人'
        ]
        assert list(result_df.columns) == expected_columns

    def test_output_filename_format(self, sample_contract_list, sample_billing_data):
        """出力ファイル名が正しいフォーマットであること"""
        from processors.gb_notification import process_gb_notification

        result_df, filename, message, logs = process_gb_notification(
            sample_contract_list, sample_billing_data
        )

        # ガレージバンク催告書YYMMDD.xlsx の形式
        today = datetime.now().strftime('%y%m%d')
        expected_filename = f'ガレージバンク催告書{today}.xlsx'
        assert filename == expected_filename

    def test_billing_data_mapped_correctly(self, sample_contract_list, sample_billing_data):
        """請求データが正しくマッピングされること"""
        from processors.gb_notification import process_gb_notification

        result_df, filename, message, logs = process_gb_notification(
            sample_contract_list, sample_billing_data
        )

        if not result_df.empty:
            # 最初の行の請求金額が正しいこと
            first_row = result_df.iloc[0]
            assert first_row['請求金額'] == 48000
            assert first_row['遅延損害金額'] == 1500
            assert first_row['その他費用'] == 500

    def test_empty_contract_list(self, sample_billing_data):
        """ContractListが空の場合、空のDataFrameが返されること"""
        from processors.gb_notification import process_gb_notification

        empty_df = pd.DataFrame()
        result_df, filename, message, logs = process_gb_notification(
            empty_df, sample_billing_data
        )

        assert len(result_df) == 0

    def test_empty_billing_data(self, sample_contract_list):
        """請求データが空の場合、全レコードが除外されること"""
        from processors.gb_notification import process_gb_notification

        empty_billing = pd.DataFrame(columns=[
            'ユーザーID', '請求金額', '遅延損害金額', 'その他費用'
        ])
        result_df, filename, message, logs = process_gb_notification(
            sample_contract_list, empty_billing
        )

        # マッチするものがないので空
        assert len(result_df) == 0

    def test_logs_contain_filter_info(self, sample_contract_list, sample_billing_data):
        """ログにフィルタ情報が含まれること"""
        from processors.gb_notification import process_gb_notification

        result_df, filename, message, logs = process_gb_notification(
            sample_contract_list, sample_billing_data
        )

        # ログにフィルタ情報が含まれていること
        log_text = '\n'.join(logs)
        assert '委託先法人ID' in log_text or 'フィルタ' in log_text


class TestEdgeCases:
    """境界値・異常系テスト"""

    def test_arrears_exactly_one_yen(self):
        """滞納残債がちょうど1円の場合、通過すること"""
        from processors.gb_notification import apply_gb_filters

        # 最小限のテストデータ
        data = [[None] * 121]
        data[0][71] = '1'  # 滞納残債 = 1円
        data[0][118] = 7  # 委託先法人ID = 7
        data[0][72] = np.nan  # 入金予定日 = 空
        data[0][73] = 1  # 入金予定金額
        data[0][86] = '通常'  # 回収ランク
        df = pd.DataFrame(data)

        result, logs = apply_gb_filters(df)
        assert len(result) == 1

    def test_arrears_with_comma(self):
        """滞納残債にカンマが含まれる場合も正しく処理されること"""
        from processors.gb_notification import apply_gb_filters

        data = [[None] * 121]
        data[0][71] = '1,000,000'  # カンマ付き
        data[0][118] = 7
        data[0][72] = np.nan
        data[0][73] = 1
        data[0][86] = '通常'
        df = pd.DataFrame(data)

        result, logs = apply_gb_filters(df)
        assert len(result) == 1

    def test_payment_date_today_excluded(self):
        """入金予定日が本日の場合、除外されること"""
        from processors.gb_notification import apply_gb_filters

        data = [[None] * 121]
        data[0][71] = '50000'
        data[0][118] = 7
        data[0][72] = datetime.now().strftime('%Y-%m-%d')  # 本日
        data[0][73] = 1
        data[0][86] = '通常'
        df = pd.DataFrame(data)

        result, logs = apply_gb_filters(df)
        assert len(result) == 0

    def test_payment_date_yesterday_included(self):
        """入金予定日が昨日の場合、通過すること"""
        from processors.gb_notification import apply_gb_filters

        data = [[None] * 121]
        data[0][71] = '50000'
        data[0][118] = 7
        data[0][72] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  # 昨日
        data[0][73] = 1
        data[0][86] = '通常'
        df = pd.DataFrame(data)

        result, logs = apply_gb_filters(df)
        assert len(result) == 1

    def test_collection_rank_bankruptcy(self):
        """回収ランク=破産決定の場合、除外されること"""
        from processors.gb_notification import apply_gb_filters

        data = [[None] * 121]
        data[0][71] = '50000'
        data[0][118] = 7
        data[0][72] = np.nan
        data[0][73] = 1
        data[0][86] = '破産決定'
        df = pd.DataFrame(data)

        result, logs = apply_gb_filters(df)
        assert len(result) == 0

    def test_collection_rank_lawyer(self):
        """回収ランク=弁護士介入の場合、除外されること"""
        from processors.gb_notification import apply_gb_filters

        data = [[None] * 121]
        data[0][71] = '50000'
        data[0][118] = 7
        data[0][72] = np.nan
        data[0][73] = 1
        data[0][86] = '弁護士介入'
        df = pd.DataFrame(data)

        result, logs = apply_gb_filters(df)
        assert len(result) == 0

    def test_payment_amount_3_excluded(self):
        """入金予定金額=3の場合、除外されること"""
        from processors.gb_notification import apply_gb_filters

        data = [[None] * 121]
        data[0][71] = '50000'
        data[0][118] = 7
        data[0][72] = np.nan
        data[0][73] = 3
        data[0][86] = '通常'
        df = pd.DataFrame(data)

        result, logs = apply_gb_filters(df)
        assert len(result) == 0

    def test_payment_amount_5_excluded(self):
        """入金予定金額=5の場合、除外されること"""
        from processors.gb_notification import apply_gb_filters

        data = [[None] * 121]
        data[0][71] = '50000'
        data[0][118] = 7
        data[0][72] = np.nan
        data[0][73] = 5
        data[0][86] = '通常'
        df = pd.DataFrame(data)

        result, logs = apply_gb_filters(df)
        assert len(result) == 0
