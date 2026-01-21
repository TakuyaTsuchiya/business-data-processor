"""
ガレージバンク残債取り込み処理画面モジュール
Business Data Processor

ガレージバンク用の残債取り込み処理画面
"""

import streamlit as st
from datetime import datetime
from components.result_display import display_processing_result, display_error_result
from components.screen_template import ScreenConfig, render_screen
from processors.gb_zansai import process_gb_zansai


def show_gb_zansai():
    """ガレージバンク残債取り込み画面を表示"""
    timestamp = datetime.now().strftime("%m%d")

    # カスタム処理関数
    def process_with_message(files):
        # files[0]: 請求データExcel, files[1]: ContractList
        output_df, logs, filename = process_gb_zansai(files[0], files[1])

        # 全てマッチしなかった場合
        if len(output_df) == 0:
            logs.insert(0, "【処理結果】マッチするデータがありませんでした。")
            return (output_df, logs, f"{timestamp}ガレージバンク管理前取込.csv")

        return (output_df, logs, filename)

    config = ScreenConfig(
        title="残債取り込み",
        subtitle="ガレージバンク残債の取り込み",
        filter_conditions=[
            "マッチング → ユーザーID（請求データ）↔引継番号（ContractList）",
            "出力 → 管理番号, 管理前滞納額（請求総額）"
        ],
        process_function=process_with_message,
        file_count=2,
        info_message="📂 必要ファイル: 情報連携シートExcel + ContractList（2ファイル処理）",
        file_labels=["ファイル1: 情報連携シート.xlsx（01_請求データ）", "ファイル2: ContractList（委託先法人ID=7）"],
        title_icon="💰",
        no_data_message="✅ 処理完了: マッチするデータがありませんでした。",
        file_types=["xlsx", "csv"]
    )
    render_screen(config, 'gb_zansai')
