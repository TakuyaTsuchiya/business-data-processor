"""
SMS処理設定モジュール

このモジュールは、SMS処理で使用する共通設定を管理します。
各SMS処理で共通して使用される定数、設定値、フィルタ条件などを定義します。
"""

from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional
from datetime import datetime


@dataclass
class SMSConfig:
    """
    SMS処理の共通設定クラス
    
    各種SMS処理で使用する共通設定を保持します。
    """
    
    # 電話番号関連
    mobile_prefixes: List[str] = field(default_factory=lambda: ['090', '080', '070'])
    phone_format_pattern: str = r'^\d{3}-?\d{4}-?\d{4}$'
    
    # 除外条件
    exclude_amounts: Set[int] = field(default_factory=lambda: {2, 3, 5, 12})
    exclude_collection_ranks: List[str] = field(
        default_factory=lambda: ['督促停止', '弁護士介入', '督促停止(弁護士介入)']
    )
    
    # 日付フォーマット
    date_format: str = '%Y-%m-%d'
    datetime_format: str = '%Y-%m-%d %H:%M:%S'
    
    # 出力設定
    output_encoding: str = 'cp932'
    output_date_format: str = '%m%d'
    
    # CSV列名マッピング（共通）
    common_columns: Dict[str, str] = field(default_factory=lambda: {
        '契約者ID': '契約者ID',
        '契約者氏名': '契約者氏名',
        '契約者カナ': '契約者カナ',
        '契約者携帯電話': '契約者携帯電話',
        '保証人携帯電話': '保証人携帯電話',
        '緊急連絡人携帯電話': '緊急連絡人携帯電話',
        '入金予定日': '入金予定日',
        '入金予定金額': '入金予定金額',
        '回収ランク': '回収ランク',
        '委託先法人ID': '委託先法人ID',
        '入居ステータス': '入居ステータス'
    })
    
    # フェイスSMS固有設定
    faith_sms_columns: Dict[str, str] = field(default_factory=lambda: {
        '顧客番号': '顧客番号',
        '送信先携帯番号': '送信先携帯番号',
        '物件名': '物件名',
        '号室': '号室'
    })
    
    # プラザSMS固有設定
    plaza_sms_columns: Dict[str, str] = field(default_factory=lambda: {
        '顧客管理コード': '顧客管理コード',
        '電話番号': '電話番号',
        '物件所在地１': '物件所在地１',
        '部屋番号': '部屋番号',
        '契約者：国籍': '契約者：国籍'
    })
    
    # バリデーション設定
    max_phone_length: int = 13  # ハイフン含む最大長
    min_phone_length: int = 10  # ハイフンなし最小長
    
    # フィルタ設定
    default_client_ids: Dict[str, List[int]] = field(default_factory=lambda: {
        'faith': [1, 2, 3, 4],
        'plaza': [6],
        'mirail': [5]
    })
    
    def get_output_filename(self, base_name: str, suffix: str = '') -> str:
        """
        出力ファイル名を生成
        
        Args:
            base_name: ベースファイル名
            suffix: サフィックス（オプション）
            
        Returns:
            生成されたファイル名
        """
        date_str = datetime.now().strftime(self.output_date_format)
        if suffix:
            return f"{date_str}{base_name}_{suffix}.csv"
        return f"{date_str}{base_name}.csv"
    
    def is_exclude_amount(self, amount: float) -> bool:
        """
        除外金額かどうかを判定
        
        Args:
            amount: 金額
            
        Returns:
            除外対象の場合True
        """
        try:
            return int(amount) in self.exclude_amounts
        except (ValueError, TypeError):
            return False
    
    def is_exclude_rank(self, rank: str) -> bool:
        """
        除外ランクかどうかを判定
        
        Args:
            rank: 回収ランク
            
        Returns:
            除外対象の場合True
        """
        if not rank:
            return False
        return any(exclude in str(rank) for exclude in self.exclude_collection_ranks)
    
    def merge_columns(self, *column_dicts: Dict[str, str]) -> Dict[str, str]:
        """
        複数の列名マッピングをマージ
        
        Args:
            *column_dicts: マージする列名マッピング
            
        Returns:
            マージされた列名マッピング
        """
        result = self.common_columns.copy()
        for d in column_dicts:
            result.update(d)
        return result


# デフォルト設定インスタンス
default_config = SMSConfig()


class FaithSMSConfig(SMSConfig):
    """フェイスSMS専用設定"""
    
    def __init__(self):
        super().__init__()
        self.output_columns = [
            '顧客番号',
            '送信先携帯番号',
            '契約者氏名',
            '物件名',
            '号室'
        ]
        self.required_columns = [
            '契約者ID',
            '契約者携帯電話',
            '契約者氏名',
            '入居ステータス',
            '委託先法人ID'
        ]


class PlazaSMSConfig(SMSConfig):
    """プラザSMS専用設定"""
    
    def __init__(self):
        super().__init__()
        self.output_columns_japanese = [
            '顧客管理コード',
            '電話番号',
            '契約者（カナ）',
            '物件所在地１',
            '部屋番号'
        ]
        self.output_columns_foreign = [
            '顧客管理コード',
            '電話番号',
            '契約者名',
            '物件所在地１',
            '部屋番号'
        ]
        self.required_columns = [
            '顧客管理コード',
            '契約者携帯電話',
            '契約者名',
            '契約者（カナ）',
            '委託先法人ID',
            '入金予定日',
            '入金予定金額',
            '回収ランク'
        ]
        # 国籍判定用キーワード
        self.foreign_keywords = [
            '中国', '韓国', 'ベトナム', 'フィリピン', 'ネパール',
            'インド', 'ミャンマー', 'タイ', 'インドネシア', 'ブラジル'
        ]