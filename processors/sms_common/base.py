"""
SMS処理基底クラスモジュール

このモジュールは、各種SMS処理の共通基底クラスを提供します。
フェイスSMSやプラザSMSなど、個別のSMS処理クラスはこの基底クラスを継承して実装します。
"""

import pandas as pd
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import chardet

from .config import SMSConfig
from .validators import validate_phone_number, is_mobile_number
from .utils import format_phone_number, clean_text, get_encoding


class BaseSMSProcessor(ABC):
    """
    SMS処理の基底クラス
    
    各種SMS処理で共通する処理フローとメソッドを定義します。
    個別のSMS処理クラスはこのクラスを継承し、必要なメソッドをオーバーライドします。
    """
    
    def __init__(self, config: Optional[SMSConfig] = None):
        """
        初期化
        
        Args:
            config: SMS処理設定（省略時はデフォルト設定を使用）
        """
        self.config = config or SMSConfig()
        self.logger = logging.getLogger(self.__class__.__name__)
        self._setup_logging()
        
    def _setup_logging(self):
        """ロギング設定"""
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def process(self, df: pd.DataFrame, **kwargs) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        SMS処理のメインフロー
        
        Args:
            df: 入力データフレーム
            **kwargs: 追加パラメータ
            
        Returns:
            処理済みデータフレームと処理統計情報のタプル
        """
        self.logger.info(f"SMS処理開始: 入力行数 {len(df)}")
        
        # 前処理
        df = self._preprocess(df, **kwargs)
        
        # バリデーション
        df = self._validate(df)
        
        # フィルタリング
        df = self._filter(df, **kwargs)
        
        # データ変換
        df = self._transform(df, **kwargs)
        
        # 後処理
        df, stats = self._postprocess(df, **kwargs)
        
        self.logger.info(f"SMS処理完了: 出力行数 {len(df)}")
        
        return df, stats
    
    def _preprocess(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        前処理
        
        共通の前処理を実行します。
        個別の前処理が必要な場合は、子クラスでオーバーライドしてください。
        
        Args:
            df: 入力データフレーム
            **kwargs: 追加パラメータ
            
        Returns:
            前処理済みデータフレーム
        """
        self.logger.info("前処理開始")
        
        # 列名の正規化（前後の空白を削除）
        df.columns = df.columns.str.strip()
        
        # テキストフィールドのクリーニング
        text_columns = self._get_text_columns()
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: clean_text(str(x)) if pd.notna(x) else '')
        
        return df
    
    def _validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        バリデーション
        
        データの妥当性を検証します。
        
        Args:
            df: データフレーム
            
        Returns:
            バリデーション済みデータフレーム
        """
        self.logger.info("バリデーション開始")
        
        # 必須列の存在確認
        required_columns = self._get_required_columns()
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"必須列が不足しています: {missing_columns}")
        
        # 電話番号のバリデーション
        phone_columns = self._get_phone_columns()
        for col in phone_columns:
            if col in df.columns:
                df[f'{col}_valid'] = df[col].apply(
                    lambda x: validate_phone_number(str(x)) if pd.notna(x) else False
                )
        
        return df
    
    @abstractmethod
    def _filter(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        フィルタリング（抽象メソッド）
        
        個別のSMS処理に応じたフィルタリングを実装してください。
        
        Args:
            df: データフレーム
            **kwargs: 追加パラメータ
            
        Returns:
            フィルタリング済みデータフレーム
        """
        pass
    
    @abstractmethod
    def _transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        データ変換（抽象メソッド）
        
        個別のSMS処理に応じたデータ変換を実装してください。
        
        Args:
            df: データフレーム
            **kwargs: 追加パラメータ
            
        Returns:
            変換済みデータフレーム
        """
        pass
    
    def _postprocess(self, df: pd.DataFrame, **kwargs) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        後処理
        
        共通の後処理と統計情報の生成を行います。
        
        Args:
            df: データフレーム
            **kwargs: 追加パラメータ
            
        Returns:
            後処理済みデータフレームと統計情報のタプル
        """
        self.logger.info("後処理開始")
        
        # 電話番号のフォーマット
        phone_columns = self._get_phone_columns()
        for col in phone_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: format_phone_number(str(x)) if pd.notna(x) else ''
                )
        
        # 統計情報の生成
        stats = self._generate_statistics(df)
        
        return df, stats
    
    def _generate_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        統計情報の生成
        
        Args:
            df: データフレーム
            
        Returns:
            統計情報の辞書
        """
        stats = {
            '総レコード数': len(df),
            '処理日時': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # 電話番号の統計
        phone_columns = self._get_phone_columns()
        for col in phone_columns:
            if f'{col}_valid' in df.columns:
                valid_count = df[f'{col}_valid'].sum()
                stats[f'{col}_有効数'] = valid_count
                stats[f'{col}_有効率'] = f"{(valid_count / len(df) * 100):.1f}%" if len(df) > 0 else "0%"
        
        return stats
    
    @abstractmethod
    def _get_required_columns(self) -> List[str]:
        """必須列のリストを返す（抽象メソッド）"""
        pass
    
    @abstractmethod
    def _get_phone_columns(self) -> List[str]:
        """電話番号列のリストを返す（抽象メソッド）"""
        pass
    
    def _get_text_columns(self) -> List[str]:
        """
        テキスト列のリストを返す
        
        デフォルトでは空リストを返します。
        必要に応じて子クラスでオーバーライドしてください。
        """
        return []
    
    def save_to_csv(self, df: pd.DataFrame, output_path: str, encoding: str = 'cp932'):
        """
        CSVファイルへの保存
        
        Args:
            df: 保存するデータフレーム
            output_path: 出力ファイルパス
            encoding: 文字エンコーディング（デフォルト: cp932）
        """
        try:
            df.to_csv(output_path, index=False, encoding=encoding)
            self.logger.info(f"CSVファイル保存完了: {output_path}")
        except Exception as e:
            self.logger.error(f"CSV保存エラー: {e}")
            raise