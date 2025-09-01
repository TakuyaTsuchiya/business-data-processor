"""
ミライル契約者フィルタリング関数のテスト

各フィルタ関数のビジネスルールが正しく実装されていることを確認
"""

import pytest
import pandas as pd
from datetime import datetime, date
from processors.mirail_autocall.contract.filters import (
    filter_client_id,
    filter_payment_date,
    filter_collection_rank,
    filter_mirail_special_debt,
    filter_mobile_phone,
    filter_exclude_amounts
)


class TestFilterClientId:
    """委託先法人IDフィルタのテスト"""
    
    def test_empty_string_is_accepted(self):
        """空文字は処理対象"""
        assert filter_client_id({'委託先法人ID': ''}) == True
    
    def test_nan_is_accepted(self):
        """NaNは処理対象"""
        assert filter_client_id({'委託先法人ID': pd.NA}) == True
        assert filter_client_id({'委託先法人ID': None}) == True
    
    def test_five_is_accepted(self):
        """'5'は処理対象"""
        assert filter_client_id({'委託先法人ID': '5'}) == True
        assert filter_client_id({'委託先法人ID': 5}) == True
        assert filter_client_id({'委託先法人ID': ' 5 '}) == True  # 前後の空白も考慮
    
    def test_other_values_are_rejected(self):
        """その他の値は除外"""
        assert filter_client_id({'委託先法人ID': '1'}) == False
        assert filter_client_id({'委託先法人ID': '2'}) == False
        assert filter_client_id({'委託先法人ID': '10'}) == False
        assert filter_client_id({'委託先法人ID': 'ABC'}) == False


class TestFilterPaymentDate:
    """入金予定日フィルタのテスト"""
    
    def test_nan_is_accepted(self):
        """NaNは処理対象"""
        assert filter_payment_date({'入金予定日': pd.NA}) == True
        assert filter_payment_date({'入金予定日': None}) == True
    
    def test_past_date_is_accepted(self):
        """前日以前は処理対象"""
        today = datetime.now().date()
        yesterday = pd.Timestamp(today) - pd.Timedelta(days=1)
        assert filter_payment_date({'入金予定日': yesterday}, today) == True
        
        week_ago = pd.Timestamp(today) - pd.Timedelta(days=7)
        assert filter_payment_date({'入金予定日': week_ago}, today) == True
    
    def test_today_is_rejected(self):
        """当日は除外"""
        today = datetime.now().date()
        assert filter_payment_date({'入金予定日': pd.Timestamp(today)}, today) == False
    
    def test_future_date_is_rejected(self):
        """未来日は除外"""
        today = datetime.now().date()
        tomorrow = pd.Timestamp(today) + pd.Timedelta(days=1)
        assert filter_payment_date({'入金予定日': tomorrow}, today) == False
    
    def test_string_date_conversion(self):
        """文字列の日付も正しく処理"""
        reference_date = date(2024, 1, 15)
        assert filter_payment_date({'入金予定日': '2024-01-14'}, reference_date) == True
        assert filter_payment_date({'入金予定日': '2024-01-15'}, reference_date) == False


class TestFilterCollectionRank:
    """回収ランクフィルタのテスト"""
    
    def test_lawyer_intervention_is_rejected(self):
        """'弁護士介入'は除外"""
        assert filter_collection_rank({'回収ランク': '弁護士介入'}) == False
    
    def test_other_ranks_are_accepted(self):
        """その他のランクは処理対象"""
        assert filter_collection_rank({'回収ランク': '通常'}) == True
        assert filter_collection_rank({'回収ランク': '督促'}) == True
        assert filter_collection_rank({'回収ランク': ''}) == True
        assert filter_collection_rank({'回収ランク': None}) == True


class TestFilterMirailSpecialDebt:
    """ミライル特殊残債フィルタのテスト"""
    
    def test_cd1_with_10000_is_rejected(self):
        """CD=1 かつ 10,000円は除外"""
        assert filter_mirail_special_debt({'クライアントCD': '1', '滞納残債': '10000'}) == False
        assert filter_mirail_special_debt({'クライアントCD': 1, '滞納残債': 10000}) == False
        assert filter_mirail_special_debt({'クライアントCD': '1', '滞納残債': '10,000'}) == False
    
    def test_cd4_with_11000_is_rejected(self):
        """CD=4 かつ 11,000円は除外"""
        assert filter_mirail_special_debt({'クライアントCD': '4', '滞納残債': '11000'}) == False
        assert filter_mirail_special_debt({'クライアントCD': 4, '滞納残債': 11000}) == False
    
    def test_cd1_with_other_amounts_are_accepted(self):
        """CD=1 でも 10,000/11,000円以外は処理対象"""
        assert filter_mirail_special_debt({'クライアントCD': '1', '滞納残債': '9999'}) == True
        assert filter_mirail_special_debt({'クライアントCD': '1', '滞納残債': '10001'}) == True
        assert filter_mirail_special_debt({'クライアントCD': '1', '滞納残債': '12000'}) == True
    
    def test_other_cd_with_10000_are_accepted(self):
        """CD≠1,4 なら 10,000円でも処理対象"""
        assert filter_mirail_special_debt({'クライアントCD': '2', '滞納残債': '10000'}) == True
        assert filter_mirail_special_debt({'クライアントCD': '3', '滞納残債': '11000'}) == True
        assert filter_mirail_special_debt({'クライアントCD': '5', '滞納残債': '10000'}) == True
    
    def test_invalid_values_are_accepted(self):
        """無効な値は処理対象（安全側に倒す）"""
        assert filter_mirail_special_debt({'クライアントCD': 'ABC', '滞納残債': '10000'}) == True
        assert filter_mirail_special_debt({'クライアントCD': '1', '滞納残債': 'XYZ'}) == True
        assert filter_mirail_special_debt({'クライアントCD': None, '滞納残債': None}) == True


class TestFilterMobilePhone:
    """携帯電話番号フィルタのテスト"""
    
    def test_valid_phone_is_accepted(self):
        """有効な電話番号は処理対象"""
        assert filter_mobile_phone({'TEL携帯': '090-1234-5678'}) == True
        assert filter_mobile_phone({'TEL携帯': '09012345678'}) == True
        assert filter_mobile_phone({'TEL携帯': '080-0000-0000'}) == True
    
    def test_empty_values_are_rejected(self):
        """空の値は除外"""
        assert filter_mobile_phone({'TEL携帯': ''}) == False
        assert filter_mobile_phone({'TEL携帯': ' '}) == False
        assert filter_mobile_phone({'TEL携帯': None}) == False
        assert filter_mobile_phone({'TEL携帯': pd.NA}) == False
    
    def test_nan_strings_are_rejected(self):
        """'nan', 'NaN'文字列は除外"""
        assert filter_mobile_phone({'TEL携帯': 'nan'}) == False
        assert filter_mobile_phone({'TEL携帯': 'NaN'}) == False


class TestFilterExcludeAmounts:
    """除外金額フィルタのテスト"""
    
    def test_excluded_amounts_are_rejected(self):
        """2,3,5,12円は除外"""
        assert filter_exclude_amounts({'入金予定金額': 2}) == False
        assert filter_exclude_amounts({'入金予定金額': 3}) == False
        assert filter_exclude_amounts({'入金予定金額': 5}) == False
        assert filter_exclude_amounts({'入金予定金額': 12}) == False
        assert filter_exclude_amounts({'入金予定金額': '2'}) == False
        assert filter_exclude_amounts({'入金予定金額': 2.0}) == False
    
    def test_other_amounts_are_accepted(self):
        """その他の金額は処理対象"""
        assert filter_exclude_amounts({'入金予定金額': 1}) == True
        assert filter_exclude_amounts({'入金予定金額': 4}) == True
        assert filter_exclude_amounts({'入金予定金額': 10}) == True
        assert filter_exclude_amounts({'入金予定金額': 100}) == True
        assert filter_exclude_amounts({'入金予定金額': 1000}) == True
    
    def test_nan_is_accepted(self):
        """NaNは処理対象"""
        assert filter_exclude_amounts({'入金予定金額': None}) == True
        assert filter_exclude_amounts({'入金予定金額': pd.NA}) == True
    
    def test_invalid_values_are_accepted(self):
        """変換できない値は処理対象（安全側に倒す）"""
        assert filter_exclude_amounts({'入金予定金額': 'ABC'}) == True
        assert filter_exclude_amounts({'入金予定金額': ''}) == True