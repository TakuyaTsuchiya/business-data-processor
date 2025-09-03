"""
プラザオートコール画面モジュール
Business Data Processor

プラザ用のオートコール処理画面（3種類）
- 契約者（main）
- 保証人
- 緊急連絡人（contact）
"""

import streamlit as st
from components.common_ui import display_filter_conditions
from components.result_display import display_processing_result, display_error_result
from components.screen_template import ScreenConfig, render_screen
from services.autocall import (
    process_plaza_main_data,
    process_plaza_guarantor_data,
    process_plaza_contact_data
)


def show_plaza_main():
    config = ScreenConfig(
        title="オートコール用CSV加工",
        subtitle="プラザ契約者",
        filter_conditions=[
            "委託先法人ID → 6",
            "入金予定日 → 当日以前とNaN",
            "入金予定金額 → 2,3,5,12円除外",
            "「TEL携帯」 → 空でない値のみ",
            "回収ランク → 「督促停止」「弁護士介入」除外"
        ],
        process_function=process_plaza_main_data,
        title_icon="📞"
    )
    render_screen(config, 'plaza_main')


def show_plaza_guarantor():
    config = ScreenConfig(
        title="オートコール用CSV加工",
        subtitle="プラザ保証人",
        filter_conditions=[
            "委託先法人ID → 6",
            "入金予定日 → 前日以前とNaN",
            "入金予定金額 → 2,3,5,12円除外",
            "「TEL携帯.1」 → 空でない値のみ",
            "回収ランク → 「督促停止」「弁護士介入」除外"
        ],
        process_function=process_plaza_guarantor_data,
        title_icon="📞"
    )
    render_screen(config, 'plaza_guarantor')


def show_plaza_contact():
    config = ScreenConfig(
        title="オートコール用CSV加工",
        subtitle="プラザ緊急連絡人",
        filter_conditions=[
            "委託先法人ID → 6",
            "入金予定日 → 前日以前とNaN",
            "入金予定金額 → 2,3,5,12円除外",
            "「緊急連絡人１のTEL（携帯）」 → 空でない値のみ",
            "回収ランク → 「督促停止」「弁護士介入」除外"
        ],
        process_function=process_plaza_contact_data,
        title_icon="📞"
    )
    render_screen(config, 'plaza_contact')