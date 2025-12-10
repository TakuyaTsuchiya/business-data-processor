"""
フェイスオートコール保証人 委託先法人IDフィルタのテスト

ビジネスルール:
- 委託先法人ID: 1, 2, 3, 4 のみ処理対象（従来通り）
- ID 7 は契約者のみで、保証人は対象外
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta

from processors.faith_autocall.guarantor.standard import apply_faith_guarantor_filters
from domain.rules.business_rules import CLIENT_IDS


class TestFaithGuarantorClientIdFilter:
    """フェイス保証人の委託先法人IDフィルタテスト"""

    @pytest.fixture
    def base_valid_row(self):
        """フィルタを通過するベース行データ"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        return {
            '委託先法人ID': '1',
            '入金予定日': yesterday,
            '入金予定金額': '10000',
            '回収ランク': '通常',
            '滞納残債': '50000',
            'TEL携帯.1': '090-1234-5678',  # 保証人の電話番号列
            '入居ステータス': '入居中',
            '滞納ステータス': '滞納',
            '管理番号': 'TEST001',
            '契約者カナ': 'テスト タロウ',
            '物件名': 'テストマンション'
        }

    def create_test_df(self, rows):
        """テスト用DataFrameを作成"""
        return pd.DataFrame(rows)

    # === 委託先法人ID 1,2,3,4 は処理対象 ===

    def test_client_id_1_is_accepted(self, base_valid_row):
        """委託先法人ID=1は処理対象"""
        base_valid_row['委託先法人ID'] = '1'
        df = self.create_test_df([base_valid_row])

        result_df, logs = apply_faith_guarantor_filters(df)

        assert len(result_df) == 1
        assert result_df.iloc[0]['委託先法人ID'] == 1

    def test_client_id_2_is_accepted(self, base_valid_row):
        """委託先法人ID=2は処理対象"""
        base_valid_row['委託先法人ID'] = '2'
        df = self.create_test_df([base_valid_row])

        result_df, logs = apply_faith_guarantor_filters(df)

        assert len(result_df) == 1
        assert result_df.iloc[0]['委託先法人ID'] == 2

    def test_client_id_3_is_accepted(self, base_valid_row):
        """委託先法人ID=3は処理対象"""
        base_valid_row['委託先法人ID'] = '3'
        df = self.create_test_df([base_valid_row])

        result_df, logs = apply_faith_guarantor_filters(df)

        assert len(result_df) == 1
        assert result_df.iloc[0]['委託先法人ID'] == 3

    def test_client_id_4_is_accepted(self, base_valid_row):
        """委託先法人ID=4は処理対象"""
        base_valid_row['委託先法人ID'] = '4'
        df = self.create_test_df([base_valid_row])

        result_df, logs = apply_faith_guarantor_filters(df)

        assert len(result_df) == 1
        assert result_df.iloc[0]['委託先法人ID'] == 4

    def test_all_valid_client_ids_in_single_batch(self, base_valid_row):
        """全ての有効な委託先法人ID（1,2,3,4）が一括で処理される"""
        rows = []
        for client_id in [1, 2, 3, 4]:
            row = base_valid_row.copy()
            row['委託先法人ID'] = str(client_id)
            row['管理番号'] = f'TEST{client_id:03d}'
            rows.append(row)

        df = self.create_test_df(rows)
        result_df, logs = apply_faith_guarantor_filters(df)

        assert len(result_df) == 4
        result_ids = set(result_df['委託先法人ID'].tolist())
        assert result_ids == {1, 2, 3, 4}

    # === 委託先法人ID 7 は保証人では除外（契約者のみ対象） ===

    def test_client_id_7_is_rejected_for_guarantor(self, base_valid_row):
        """委託先法人ID=7は保証人では除外（契約者のみ対象）"""
        base_valid_row['委託先法人ID'] = '7'
        df = self.create_test_df([base_valid_row])

        result_df, logs = apply_faith_guarantor_filters(df)

        assert len(result_df) == 0

    # === その他の除外ID ===

    def test_client_id_5_is_rejected(self, base_valid_row):
        """委託先法人ID=5は除外（ミライル用）"""
        base_valid_row['委託先法人ID'] = '5'
        df = self.create_test_df([base_valid_row])

        result_df, logs = apply_faith_guarantor_filters(df)

        assert len(result_df) == 0

    def test_client_id_6_is_rejected(self, base_valid_row):
        """委託先法人ID=6は除外（プラザ用）"""
        base_valid_row['委託先法人ID'] = '6'
        df = self.create_test_df([base_valid_row])

        result_df, logs = apply_faith_guarantor_filters(df)

        assert len(result_df) == 0

    def test_client_id_8_is_rejected(self, base_valid_row):
        """委託先法人ID=8は除外"""
        base_valid_row['委託先法人ID'] = '8'
        df = self.create_test_df([base_valid_row])

        result_df, logs = apply_faith_guarantor_filters(df)

        assert len(result_df) == 0

    # === 空白・NaN・不正な値の処理 ===

    def test_empty_string_is_rejected(self, base_valid_row):
        """空文字は除外"""
        base_valid_row['委託先法人ID'] = ''
        df = self.create_test_df([base_valid_row])

        result_df, logs = apply_faith_guarantor_filters(df)

        assert len(result_df) == 0

    def test_nan_is_rejected(self, base_valid_row):
        """NaNは除外"""
        base_valid_row['委託先法人ID'] = None
        df = self.create_test_df([base_valid_row])

        result_df, logs = apply_faith_guarantor_filters(df)

        assert len(result_df) == 0

    def test_non_numeric_string_is_rejected(self, base_valid_row):
        """数値でない文字列は除外"""
        base_valid_row['委託先法人ID'] = 'ABC'
        df = self.create_test_df([base_valid_row])

        result_df, logs = apply_faith_guarantor_filters(df)

        assert len(result_df) == 0

    # === 混合データのフィルタリング ===

    def test_mixed_client_ids_filter_correctly(self, base_valid_row):
        """有効・無効な委託先法人IDが混在するデータが正しくフィルタされる"""
        rows = []
        # 有効なID: 1, 2, 3, 4（保証人は7を含まない）
        for client_id in [1, 2, 3, 4]:
            row = base_valid_row.copy()
            row['委託先法人ID'] = str(client_id)
            row['管理番号'] = f'VALID{client_id:03d}'
            rows.append(row)

        # 無効なID: 5, 6, 7, 8（7は契約者のみ）
        for client_id in [5, 6, 7, 8]:
            row = base_valid_row.copy()
            row['委託先法人ID'] = str(client_id)
            row['管理番号'] = f'INVALID{client_id:03d}'
            rows.append(row)

        df = self.create_test_df(rows)
        result_df, logs = apply_faith_guarantor_filters(df)

        # 有効なIDの4件のみ残る（7は含まない）
        assert len(result_df) == 4
        result_ids = set(result_df['委託先法人ID'].tolist())
        assert result_ids == {1, 2, 3, 4}
        assert 7 not in result_ids


class TestBusinessRulesConsistencyGuarantor:
    """ビジネスルール定義との整合性テスト（保証人）"""

    def test_faith_does_not_include_id_7(self):
        """faith（保証人用）には委託先法人ID 7が含まれていない"""
        assert 7 not in CLIENT_IDS['faith']

    def test_faith_has_original_ids(self):
        """faith（保証人用）が元のIDリストを維持している"""
        assert set(CLIENT_IDS['faith']) == {1, 2, 3, 4}

    def test_guarantor_uses_faith_not_faith_contract(self):
        """保証人はfaith_contractではなくfaithを使用すべき"""
        # faith_contractには7が含まれるが、faithには含まれない
        assert 7 in CLIENT_IDS['faith_contract']
        assert 7 not in CLIENT_IDS['faith']
