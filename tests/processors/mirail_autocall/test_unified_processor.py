"""
ミライルオートコール統合プロセッサーのテスト
既存の実装と統合版の出力が一致することを確認
"""

import pytest
import pandas as pd
from io import BytesIO

# 統合版
from processors.mirail_autocall_unified import MirailAutocallUnifiedProcessor

# 既存版（比較用）
from processors.mirail_autocall.contract.with10k_refactored import process_mirail_contract_with10k_data as old_contract_with10k
from processors.mirail_autocall.contract.without10k_refactored import process_mirail_contract_without10k_data as old_contract_without10k
from processors.mirail_autocall.guarantor.with10k_refactored import process_mirail_guarantor_with10k_data as old_guarantor_with10k
from processors.mirail_autocall.guarantor.without10k_refactored import process_mirail_guarantor_without10k_data as old_guarantor_without10k
from processors.mirail_autocall.emergency_contact.with10k_refactored import process_mirail_emergency_contact_with10k_data as old_emergency_with10k
from processors.mirail_autocall.emergency_contact.without10k_refactored import process_mirail_emergency_contact_without10k_data as old_emergency_without10k


class TestMirailUnifiedProcessor:
    """統合プロセッサーのテストクラス"""
    
    @pytest.fixture
    def sample_csv_data(self):
        """テスト用のサンプルCSVデータ"""
        # 実際のContractListフォーマットに近いサンプルデータを作成
        data = {
            # 基本情報
            'A': ['M001', 'M002', 'M003'],  # 管理番号
            'U': ['田中太郎', '佐藤花子', '鈴木一郎'],  # 契約者氏名
            'V': ['タナカタロウ', 'サトウハナコ', 'スズキイチロウ'],  # 契約者カナ
            
            # 電話番号（列位置に注意）
            **{f'col_{i}': [''] * 3 for i in range(1, 27)},  # ダミー列
            'AB': ['090-1234-5678', '080-2345-6789', ''],  # TEL携帯（契約者）
            **{f'col_{i}': [''] * 3 for i in range(28, 46)},  # ダミー列
            'AU': ['090-9876-5432', '', '070-3456-7890'],  # TEL携帯.1（保証人）
            **{f'col_{i}': [''] * 3 for i in range(47, 56)},  # ダミー列  
            'BE': ['', '080-8765-4321', '090-4567-8901'],  # 緊急連絡人電話
            
            # 金額・日付関連
            **{f'col_{i}': [''] * 3 for i in range(57, 71)},  # ダミー列
            'BT': ['50000', '10000', '11000'],  # 滞納残債
            'BU': ['2024-01-01', '2024-01-02', ''],  # 入金予定日
            'BV': ['50000', '10000', '2'],  # 入金予定金額
            
            # ステータス
            **{f'col_{i}': [''] * 3 for i in range(74, 86)},  # ダミー列
            'CI': ['通常', '弁護士介入', '通常'],  # 回収ランク
            
            # その他必要な列
            **{f'col_{i}': [''] * 3 for i in range(87, 118)},  # ダミー列
            'DO': ['', '5', ''],  # 委託先法人ID
            
            # 残りの列
            **{f'col_{i}': [''] * 3 for i in range(119, 130)},  # ダミー列
        }
        
        df = pd.DataFrame(data)
        # CSVバイトデータに変換
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        return csv_buffer.getvalue()
    
    def test_contract_with10k_compatibility(self, sample_csv_data):
        """契約者（10k含む）の互換性テスト"""
        processor = MirailAutocallUnifiedProcessor()
        
        # 統合版の実行
        new_df, new_logs, new_filename = processor.process_mirail_autocall(
            sample_csv_data, "contract", with_10k=True
        )
        
        # 既存版の実行
        old_df, old_logs, old_filename = old_contract_with10k(sample_csv_data)
        
        # ファイル名が一致することを確認
        assert new_filename == old_filename
        
        # データフレームの形状が一致
        assert new_df.shape == old_df.shape
        
        # 列名が一致
        assert list(new_df.columns) == list(old_df.columns)
    
    def test_contract_without10k_compatibility(self, sample_csv_data):
        """契約者（10k除外）の互換性テスト"""
        processor = MirailAutocallUnifiedProcessor()
        
        # 統合版の実行
        new_df, new_logs, new_filename = processor.process_mirail_autocall(
            sample_csv_data, "contract", with_10k=False
        )
        
        # 既存版の実行
        old_df, old_logs, old_filename = old_contract_without10k(sample_csv_data)
        
        # 基本的な検証
        assert new_filename == old_filename
        assert new_df.shape == old_df.shape
    
    def test_all_targets_basic(self, sample_csv_data):
        """全ターゲットの基本動作テスト"""
        processor = MirailAutocallUnifiedProcessor()
        
        targets = ["contract", "guarantor", "emergency_contact"]
        with_10k_options = [True, False]
        
        for target in targets:
            for with_10k in with_10k_options:
                # エラーなく実行できることを確認
                df, logs, filename = processor.process_mirail_autocall(
                    sample_csv_data, target, with_10k
                )
                
                # 基本的な検証
                assert isinstance(df, pd.DataFrame)
                assert isinstance(logs, list)
                assert isinstance(filename, str)
                
                # ファイル名のパターンが正しい
                prefix = "with10k" if with_10k else "without10k"
                suffix = processor.TARGET_CONFIG[target]["name_suffix"]
                assert f"ミライル_{prefix}_{suffix}.csv" in filename
    
    def test_invalid_target(self, sample_csv_data):
        """無効なターゲットでエラーになることを確認"""
        processor = MirailAutocallUnifiedProcessor()
        
        with pytest.raises(ValueError) as exc_info:
            processor.process_mirail_autocall(sample_csv_data, "invalid_target", True)
        
        assert "無効な対象者タイプ" in str(exc_info.value)