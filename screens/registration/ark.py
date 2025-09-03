"""
アーク新規登録処理画面モジュール
Business Data Processor

アーク用の新規登録処理画面（4地域）
- 東京
- 大阪
- 北海道
- 北関東
"""

import streamlit as st
from datetime import datetime
from components.result_display import display_processing_result, display_error_result
from components.screen_template import ScreenConfig, render_screen
from services.registration import process_ark_data


def show_ark_registration_tokyo():
    timestamp = datetime.now().strftime("%m%d")
    config = ScreenConfig(
        title="新規登録CSV加工",
        subtitle="アーク新規登録（東京）",
        filter_conditions=[
            "重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）",
            "新規データ → 重複除外後の案件取込用レポートデータのみ統合",
            "地域コード → 1（東京）"
        ],
        process_function=lambda files: process_ark_data(files[0], files[1], region_code=1),
        file_count=2,
        info_message="📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）",
        file_labels=["ファイル1: 案件取込用レポート", "ファイル2: ContractList"],
        title_icon="📋"
    )
    # 結果にファイル名を追加するためのカスタム処理
    original_process = config.process_function
    config.process_function = lambda files: (*original_process(files), f"{timestamp}アーク_新規登録_東京.csv")
    render_screen(config, 'ark_tokyo')


def show_ark_registration_osaka():
    timestamp = datetime.now().strftime("%m%d")
    config = ScreenConfig(
        title="新規登録CSV加工",
        subtitle="アーク新規登録（大阪）",
        filter_conditions=[
            "重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）",
            "新規データ → 重複除外後の案件取込用レポートデータのみ統合",
            "地域コード → 2（大阪）"
        ],
        process_function=lambda files: process_ark_data(files[0], files[1], region_code=2),
        file_count=2,
        info_message="📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）",
        file_labels=["ファイル1: 案件取込用レポート", "ファイル2: ContractList"],
        title_icon="📋"
    )
    original_process = config.process_function
    config.process_function = lambda files: (*original_process(files), f"{timestamp}アーク_新規登録_大阪.csv")
    render_screen(config, 'ark_osaka')


def show_ark_registration_hokkaido():
    timestamp = datetime.now().strftime("%m%d")
    config = ScreenConfig(
        title="新規登録CSV加工",
        subtitle="アーク新規登録（北海道）",
        filter_conditions=[
            "重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）",
            "新規データ → 重複除外後の案件取込用レポートデータのみ統合",
            "地域コード → 3（北海道）"
        ],
        process_function=lambda files: process_ark_data(files[0], files[1], region_code=3),
        file_count=2,
        info_message="📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）",
        file_labels=["ファイル1: 案件取込用レポート", "ファイル2: ContractList"],
        title_icon="📋"
    )
    original_process = config.process_function
    config.process_function = lambda files: (*original_process(files), f"{timestamp}アーク_新規登録_北海道.csv")
    render_screen(config, 'ark_hokkaido')


def show_ark_registration_kitakanto():
    timestamp = datetime.now().strftime("%m%d")
    config = ScreenConfig(
        title="新規登録CSV加工",
        subtitle="アーク新規登録（北関東）",
        filter_conditions=[
            "重複チェック → 契約番号（案件取込用レポート）↔引継番号（ContractList）",
            "新規データ → 重複除外後の案件取込用レポートデータのみ統合",
            "地域コード → 4（北関東）"
        ],
        process_function=lambda files: process_ark_data(files[0], files[1], region_code=4),
        file_count=2,
        info_message="📂 必要ファイル: 案件取込用レポート + ContractList（2ファイル処理）",
        file_labels=["ファイル1: 案件取込用レポート", "ファイル2: ContractList"],
        title_icon="📋"
    )
    original_process = config.process_function
    config.process_function = lambda files: (*original_process(files), f"{timestamp}アーク_新規登録_北関東.csv")
    render_screen(config, 'ark_kitakanto')