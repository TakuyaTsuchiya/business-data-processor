"""
カプコ新規登録処理画面モジュール
Business Data Processor

カプコ用の新規登録処理画面
"""

import streamlit as st
from datetime import datetime
from components.result_display import display_processing_result, display_error_result
from components.screen_template import ScreenConfig, render_screen
from services.registration import process_capco_data


def show_capco_registration():
    timestamp = datetime.now().strftime("%m%d")
    config = ScreenConfig(
        title="カプコ新規登録",
        subtitle="",
        filter_conditions=[
            "データ統合 → カプコデータ + ContractList の結合処理"
        ],
        process_function=lambda files: process_capco_data(files[0], files[1]),
        file_count=2,
        info_message="📂 必要ファイル: カプコデータ + ContractList（2ファイル処理）",
        file_labels=["ファイル1: カプコデータ", "ファイル2: ContractList"],
        title_icon="📋"
    )
    # 結果にファイル名を追加するためのカスタム処理
    original_process = config.process_function
    config.process_function = lambda files: (*original_process(files), f"{timestamp}カプコ_新規登録.csv")
    render_screen(config, 'capco_registration')