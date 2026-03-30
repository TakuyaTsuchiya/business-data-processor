"""
ナップ新規登録処理画面モジュール
Business Data Processor

ナップ賃貸保証株式会社用の新規登録処理画面
ミライル様経由でデータ提供
"""

import streamlit as st
from datetime import datetime
from components.result_display import display_processing_result, display_error_result
from components.screen_template import ScreenConfig, render_screen
from services.registration import process_nap_data


def show_nap_registration():
    timestamp = datetime.now().strftime("%m%d")

    # カスタム処理関数
    def process_with_message(files):
        output_df, logs, filename = process_nap_data(files[0], files[1])

        # 全て既存の場合、特別な処理
        if len(output_df) == 0:
            logs.insert(0, "【処理結果】全てのデータが既に登録済みです。新規登録対象はありません。")
            return (output_df, logs, f"{timestamp}ナップ新規登録.csv")

        return (output_df, logs, filename)

    config = ScreenConfig(
        title="新規登録CSV加工",
        subtitle="ナップ新規登録",
        filter_conditions=[
            "重複チェック → 承認番号（依頼データ）↔引継番号（ContractList）",
            "委託先法人ID=5"
        ],
        process_function=process_with_message,
        file_count=2,
        info_message="📂 必要ファイル: ミライル様依頼データ（Excel/CSV） + ContractList（2ファイル処理）",
        file_labels=["ファイル1: XX月分依頼データ（xlsx/csv）", "ファイル2: ContractList"],
        title_icon="📋",
        no_data_message="✅ 処理完了: 全てのデータが既に登録済みです。新規登録対象はありません。",
        file_types=["xlsx", "csv"]
    )
    render_screen(config, 'nap')
