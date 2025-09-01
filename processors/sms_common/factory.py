"""
SMSプロセッサファクトリーモジュール

既存のインターフェースを維持しながら、内部実装を共通化するためのファクトリー。
"""

from typing import Tuple, List, Dict
import pandas as pd
from datetime import date

from .processor import CommonSMSProcessor
from ..plaza_sms.contract import process_plaza_sms_contract_data
from ..faith_sms.contract import process_faith_sms_contract_data


class SMSProcessorFactory:
    """
    SMSプロセッサのファクトリークラス
    
    既存のUI/UXを変更せずに、内部実装のみ共通化。
    """
    
    @staticmethod
    def process_sms(system: str, target: str, uploaded_file, payment_deadline: date = None) -> Tuple[pd.DataFrame, List[str], str, Dict]:
        """
        SMS処理を実行
        
        Args:
            system: システム種別 ('faith', 'mirail', 'plaza')
            target: 対象者種別 ('contract', 'guarantor', 'emergency')
            uploaded_file: アップロードされたファイル
            payment_deadline: 支払期限日付
            
        Returns:
            処理済みDataFrame, 処理ログ, 出力ファイル名, 統計情報
        """
        # 特殊処理のチェック
        if system == 'plaza' and target == 'contract':
            # プラザ契約者は既存の特殊処理を使用
            return process_plaza_sms_contract_data(uploaded_file, payment_deadline)
        
        # フェイス契約者（退去済み）も現時点では既存処理を使用
        # 将来的に共通化可能
        if system == 'faith' and target == 'contract':
            return process_faith_sms_contract_data(uploaded_file, payment_deadline)
        
        # その他7種類は共通プロセッサを使用
        processor = CommonSMSProcessor(system, target, payment_deadline)
        return processor.process(uploaded_file, payment_deadline)


# 既存のインターフェースとの互換性のための関数群
# 注意: これらの関数は古い実装です。新しい実装を使用してください。

# def process_faith_guarantor_sms(uploaded_file, payment_deadline: date) -> Tuple[pd.DataFrame, List[str], str, Dict]:
#     """フェイス保証人SMS処理 - 廃止予定: faith_sms.guarantor.process_faith_sms_guarantor_dataを使用してください"""
#     return SMSProcessorFactory.process_sms('faith', 'guarantor', uploaded_file, payment_deadline)


# def process_faith_emergency_sms(uploaded_file, payment_deadline: date) -> Tuple[pd.DataFrame, List[str], str, Dict]:
#     """フェイス緊急連絡人SMS処理 - 廃止予定: faith_sms.emergency_contact.process_faith_sms_emergency_contact_dataを使用してください"""
#     return SMSProcessorFactory.process_sms('faith', 'emergency', uploaded_file, payment_deadline)


# def process_mirail_contract_sms(uploaded_file, payment_deadline: date) -> Tuple[pd.DataFrame, List[str], str, Dict]:
#     """ミライル契約者SMS処理 - 廃止予定: mirail_sms.contract.process_mirail_sms_contract_dataを使用してください"""
#     return SMSProcessorFactory.process_sms('mirail', 'contract', uploaded_file, payment_deadline)


# def process_mirail_guarantor_sms(uploaded_file, payment_deadline: date) -> Tuple[pd.DataFrame, List[str], str, Dict]:
#     """ミライル保証人SMS処理 - 廃止予定: mirail_sms.guarantor.process_mirail_sms_guarantor_dataを使用してください"""
#     return SMSProcessorFactory.process_sms('mirail', 'guarantor', uploaded_file, payment_deadline)


# def process_mirail_emergency_sms(uploaded_file, payment_deadline: date) -> Tuple[pd.DataFrame, List[str], str, Dict]:
#     """ミライル緊急連絡人SMS処理 - 廃止予定: mirail_sms.emergency_contact.process_mirail_sms_emergency_contact_dataを使用してください"""
#     return SMSProcessorFactory.process_sms('mirail', 'emergency', uploaded_file, payment_deadline)


# def process_plaza_guarantor_sms(uploaded_file, payment_deadline: date) -> Tuple[pd.DataFrame, List[str], str, Dict]:
#     """プラザ保証人SMS処理 - 廃止予定: plaza_sms.guarantor.process_plaza_sms_guarantor_dataを使用してください"""
#     return SMSProcessorFactory.process_sms('plaza', 'guarantor', uploaded_file, payment_deadline)


# def process_plaza_emergency_sms(uploaded_file, payment_deadline: date) -> Tuple[pd.DataFrame, List[str], str, Dict]:
#     """プラザ緊急連絡人SMS処理 - 廃止予定: plaza_sms.contact.process_plaza_sms_contact_dataを使用してください"""
#     return SMSProcessorFactory.process_sms('plaza', 'emergency', uploaded_file, payment_deadline)