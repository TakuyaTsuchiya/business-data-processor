"""
プラザ新規登録処理画面モジュール
Business Data Processor

プラザ用の新規登録処理画面
"""

import streamlit as st
from datetime import datetime
from components.result_display import display_processing_result, display_error_result
from components.screen_template import ScreenConfig, render_screen
from services.registration import process_plaza_data


def show_plaza_registration():
    timestamp = datetime.now().strftime("%m%d")
    
    # カスタム処理関数
    def process_with_message(files):
        output_df, new_contracts, existing_contracts, stats, logs = process_plaza_data(files[0], files[1])
        
        # 統計ログを追加
        logs.insert(0, f"プラザCSV総数: {stats['total_plaza']}件")
        logs.insert(1, f"新規契約: {stats['new_contracts']}件 ({stats['new_percentage']:.1f}%)")
        logs.insert(2, f"既存契約（重複）: {stats['existing_contracts']}件")
        
        # 全て既存の場合、warningメッセージを含む特別な処理
        if stats['new_contracts'] == 0:
            # 空のDataFrameを返すが、logsに詳細情報を含める
            logs.insert(0, "【処理結果】全てのデータが既に登録済みです。新規登録対象はありません。")
            return (output_df, logs, f"{timestamp}プラザ新規登録.csv")
        
        return (output_df, logs, f"{timestamp}プラザ新規登録.csv")
    
    config = ScreenConfig(
        title="新規登録CSV加工",
        subtitle="プラザ新規登録",
        filter_conditions=[
            "重複チェック → 会員番号（プラザCSV）↔引継番号（ContractList）",
            "新規データ → 重複除外後のプラザデータのみ処理",
            "委託先法人ID → 6",
            "クライアントCD → 7"
        ],
        process_function=process_with_message,
        file_count=2,
        info_message="⏱️ 処理時間: 処理には1分ほどかかります。お待ちください。\n\n📂 必要ファイル: プラザCSV + ContractList（2ファイル処理）",
        file_labels=["ファイル1: コールセンター回収委託_ミライル.csv", "ファイル2: ContractList"],
        title_icon="📋",
        no_data_message="✅ 処理完了: 全てのデータが既に登録済みです。新規登録対象はありません。"
    )
    render_screen(config, 'plaza')