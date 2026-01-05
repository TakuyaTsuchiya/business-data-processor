"""
FilterEngine（フィルターエンジン）のユニットテスト

滞納残債フィルタを含む各フィルタ機能が正しく実装されていることを確認
"""

import pytest
import pandas as pd
from processors.autocall_common.filter_engine import FilterEngine


class TestFilterArrears:
    """滞納残債フィルタのテスト（PR #59で追加）"""

    def test_keeps_data_above_minimum_amount(self):
        """1円以上のデータが保持される"""
        df = pd.DataFrame({
            'A': ['row1', 'row2', 'row3'],
            'arrears': [1, 100, 5000]
        })
        config = {
            "column": 1,  # 'arrears'列
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        assert len(result_df) == 3
        assert list(result_df['A']) == ['row1', 'row2', 'row3']
        assert any('滞納残債（1円以上）' in log for log in logs)

    def test_excludes_zero_amount(self):
        """0円のデータは除外される"""
        df = pd.DataFrame({
            'A': ['row1', 'row2', 'row3'],
            'arrears': [0, 100, 0]
        })
        config = {
            "column": 1,
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        assert len(result_df) == 1
        assert list(result_df['A']) == ['row2']
        assert result_df.iloc[0, 1] == 100

    def test_boundary_value_one_yen(self):
        """境界値：1円は含まれる"""
        df = pd.DataFrame({
            'A': ['row1', 'row2'],
            'arrears': [1, 0]
        })
        config = {
            "column": 1,
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        assert len(result_df) == 1
        assert result_df.iloc[0, 1] == 1

    def test_handles_comma_separated_numbers(self):
        """カンマ区切りの数値が正しく処理される"""
        df = pd.DataFrame({
            'A': ['row1', 'row2', 'row3'],
            'arrears': ['1,000', '10,000', '100']
        })
        config = {
            "column": 1,
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        assert len(result_df) == 3
        assert result_df.iloc[0, 1] == 1000
        assert result_df.iloc[1, 1] == 10000
        assert result_df.iloc[2, 1] == 100

    def test_excludes_negative_amounts(self):
        """マイナス値は除外される"""
        df = pd.DataFrame({
            'A': ['row1', 'row2', 'row3'],
            'arrears': [-100, 100, -50]
        })
        config = {
            "column": 1,
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        assert len(result_df) == 1
        assert result_df.iloc[0, 1] == 100

    def test_excludes_nan_values(self):
        """NaN（欠損値）は除外される"""
        df = pd.DataFrame({
            'A': ['row1', 'row2', 'row3'],
            'arrears': [pd.NA, 100, None]
        })
        config = {
            "column": 1,
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        assert len(result_df) == 1
        assert result_df.iloc[0, 1] == 100

    def test_excludes_empty_strings(self):
        """空文字は除外される"""
        df = pd.DataFrame({
            'A': ['row1', 'row2', 'row3'],
            'arrears': ['', '100', '  ']
        })
        config = {
            "column": 1,
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        assert len(result_df) == 1
        assert result_df.iloc[0, 1] == 100

    def test_excludes_invalid_strings(self):
        """無効な文字列（ABC, N/A等）は除外される"""
        df = pd.DataFrame({
            'A': ['row1', 'row2', 'row3', 'row4'],
            'arrears': ['ABC', '100', 'N/A', 'invalid']
        })
        config = {
            "column": 1,
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        assert len(result_df) == 1
        assert result_df.iloc[0, 1] == 100

    def test_handles_decimal_values(self):
        """小数値の処理（0.5円は除外、1.5円は含まれる）"""
        df = pd.DataFrame({
            'A': ['row1', 'row2', 'row3'],
            'arrears': [0.5, 1.5, 0.9]
        })
        config = {
            "column": 1,
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        assert len(result_df) == 1
        assert result_df.iloc[0, 1] == 1.5

    def test_generates_correct_logs(self):
        """ログが正しく生成される"""
        df = pd.DataFrame({
            'A': ['row1', 'row2', 'row3'],
            'arrears': [0, 100, 200]
        })
        config = {
            "column": 1,
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        # ログが生成されていることを確認
        assert len(logs) >= 1
        # フィルタ結果ログに「滞納残債（1円以上）」が含まれることを確認
        assert any('滞納残債（1円以上）' in log for log in logs)

    def test_logs_exclusion_details_when_data_excluded(self):
        """データが除外された場合、除外詳細ログが生成される"""
        df = pd.DataFrame({
            'A': ['row1', 'row2', 'row3'],
            'B': ['client1', 'client2', 'client3'],
            'arrears': [0, 100, -50]
        })
        config = {
            "column": 2,
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        # 2件除外されている
        assert len(result_df) == 1
        # ログが生成されている（除外詳細 + フィルタ結果）
        assert len(logs) >= 2

    def test_custom_min_amount(self):
        """カスタム最小金額が正しく機能する"""
        df = pd.DataFrame({
            'A': ['row1', 'row2', 'row3', 'row4'],
            'arrears': [50, 100, 500, 1000]
        })
        config = {
            "column": 1,
            "min_amount": 100,  # 100円以上に設定
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        # 100円以上のみ残る
        assert len(result_df) == 3
        assert list(result_df['arrears']) == [100, 500, 1000]

    def test_mixed_data_types(self):
        """混在データ型（数値、文字列、カンマ付き）が正しく処理される"""
        df = pd.DataFrame({
            'A': ['row1', 'row2', 'row3', 'row4', 'row5'],
            'arrears': [100, '200', '1,000', 0, 'invalid']
        })
        config = {
            "column": 1,
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        # 有効な3件のみ残る
        assert len(result_df) == 3
        assert result_df.iloc[0, 1] == 100
        assert result_df.iloc[1, 1] == 200
        assert result_df.iloc[2, 1] == 1000

    def test_all_data_excluded(self):
        """全データが除外される場合、空のDataFrameが返る"""
        df = pd.DataFrame({
            'A': ['row1', 'row2', 'row3'],
            'arrears': [0, -100, 0]
        })
        config = {
            "column": 1,
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        assert len(result_df) == 0
        # ログは生成される
        assert len(logs) >= 1

    def test_preserves_other_columns(self):
        """他の列が保持されることを確認"""
        df = pd.DataFrame({
            'name': ['田中', '佐藤', '鈴木'],
            'phone': ['090-1111-1111', '080-2222-2222', '070-3333-3333'],
            'arrears': [100, 0, 500]
        })
        config = {
            "column": 2,  # arrears列
            "min_amount": 1,
            "label": "滞納残債"
        }

        result_df, logs = FilterEngine._filter_arrears(df, config)

        # 2件残る
        assert len(result_df) == 2
        # 列が保持されている
        assert list(result_df.columns) == ['name', 'phone', 'arrears']
        # 正しいデータが保持されている
        assert list(result_df['name']) == ['田中', '鈴木']
        assert list(result_df['phone']) == ['090-1111-1111', '070-3333-3333']


class TestFilterPaymentDateTodayIncluded:
    """入金予定日フィルタ（当日約定込み）のテスト"""

    def test_today_included_accepts_today(self):
        """当日は処理対象に含まれる"""
        today = pd.Timestamp.now().normalize()
        df = pd.DataFrame({
            'A': ['row1'],
            'payment_date': [today]
        })
        config = {
            "column": 1,
            "type": "today_included",
            "label": "入金予定日",
            "top_n": 3
        }

        result_df, logs = FilterEngine._filter_payment_date(df, config)

        assert len(result_df) == 1
        assert any('入金予定日' in log for log in logs)

    def test_today_included_accepts_past(self):
        """過去日は処理対象"""
        yesterday = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
        week_ago = pd.Timestamp.now().normalize() - pd.Timedelta(days=7)
        df = pd.DataFrame({
            'A': ['row1', 'row2'],
            'payment_date': [yesterday, week_ago]
        })
        config = {
            "column": 1,
            "type": "today_included",
            "label": "入金予定日",
            "top_n": 3
        }

        result_df, logs = FilterEngine._filter_payment_date(df, config)

        assert len(result_df) == 2

    def test_today_included_rejects_future(self):
        """未来日は除外される"""
        tomorrow = pd.Timestamp.now().normalize() + pd.Timedelta(days=1)
        df = pd.DataFrame({
            'A': ['row1'],
            'payment_date': [tomorrow]
        })
        config = {
            "column": 1,
            "type": "today_included",
            "label": "入金予定日",
            "top_n": 3
        }

        result_df, logs = FilterEngine._filter_payment_date(df, config)

        assert len(result_df) == 0

    def test_today_included_accepts_nan(self):
        """NaNは処理対象"""
        df = pd.DataFrame({
            'A': ['row1', 'row2'],
            'payment_date': [pd.NaT, None]
        })
        config = {
            "column": 1,
            "type": "today_included",
            "label": "入金予定日",
            "top_n": 3
        }

        result_df, logs = FilterEngine._filter_payment_date(df, config)

        assert len(result_df) == 2

    def test_before_today_rejects_today(self):
        """before_todayでは当日は除外される（既存動作の確認）"""
        today = pd.Timestamp.now().normalize()
        df = pd.DataFrame({
            'A': ['row1'],
            'payment_date': [today]
        })
        config = {
            "column": 1,
            "type": "before_today",
            "label": "入金予定日",
            "top_n": 3
        }

        result_df, logs = FilterEngine._filter_payment_date(df, config)

        assert len(result_df) == 0


class TestFilterEngineIntegration:
    """FilterEngine全体の統合テスト"""

    def test_arrears_filter_in_filter_chain(self):
        """滞納残債フィルタがフィルタチェーンで正しく動作する"""
        df = pd.DataFrame({
            'trustee_id': ['', '', ''],
            'arrears': [0, 100, 200]
        })

        filter_config = {
            "arrears": {
                "column": 1,
                "min_amount": 1,
                "label": "滞納残債"
            }
        }

        result_df, logs = FilterEngine.apply_filters(df, filter_config)

        # 0円が除外され、2件残る
        assert len(result_df) == 2
        assert list(result_df['arrears']) == [100, 200]
        # ログが生成される
        assert len(logs) > 0
