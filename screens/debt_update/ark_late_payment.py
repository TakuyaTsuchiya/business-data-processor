"""
アーク遅延損害金処理画面モジュール
Business Data Processor

アーク残債の更新処理画面
"""

import streamlit as st
from datetime import datetime
from components.result_display import display_processing_result, display_error_result
from components.screen_template import ScreenConfig, render_screen
from services.debt_update import process_ark_late_payment_data


def show_ark_late_payment():
    timestamp = datetime.now().strftime("%m%d")
    config = ScreenConfig(
        title="アーク残債の更新",
        subtitle="",
        filter_conditions=[
            "データ統合 → アークデータ + ContractList の結合処理",
            "マッチング → 管理番号での照合処理",
            "残債更新 → 管理前滞納額の更新処理"
        ],
        process_function=lambda files: process_ark_late_payment_data(files[0], files[1]),
        file_count=2,
        info_message="📂 必要ファイル: アークデータ + ContractList（2ファイル処理）",
        file_labels=["ファイル1: アークデータ", "ファイル2: ContractList"],
        title_icon="💰"
    )
    render_screen(config, 'ark_late_payment')