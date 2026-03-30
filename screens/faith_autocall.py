"""
フェイスオートコール画面モジュール
Business Data Processor

フェイス用のオートコール処理画面（3種類）
- 契約者
- 保証人
- 緊急連絡人
"""

import streamlit as st
from components.common_ui import display_filter_conditions
from components.result_display import display_processing_result, display_error_result
from components.screen_template import ScreenConfig, render_screen
from services.autocall import (
    process_faith_contract_data,
    process_faith_guarantor_data,
    process_faith_emergencycontact_data,
)


def show_faith_contract():
    config = ScreenConfig(
        title="オートコール用CSV加工",
        subtitle="フェイス契約者",
        filter_conditions=[
            "委託先法人ID → 1-4,7,8",
            "入金予定日 → 当日以前とNaN",
            "回収ランク → 「死亡決定」「破産決定」「弁護士介入」除外",
            "滞納残債 → 1円以上のみ対象",
            "入金予定金額 → 2,3,5,12除外",
            "滞納残債フィルタ → なし（全件処理）",
            "「TEL携帯」 → 空でない値のみ",
        ],
        process_function=process_faith_contract_data,
        title_icon="📞",
    )
    render_screen(config, "faith_contract")


def show_faith_guarantor():
    config = ScreenConfig(
        title="オートコール用CSV加工",
        subtitle="フェイス保証人",
        filter_conditions=[
            "委託先法人ID → 1-4,8",
            "入金予定日 → 前日以前とNaN",
            "回収ランク → 「死亡決定」「破産決定」「弁護士介入」除外",
            "滞納残債 → 1円以上のみ対象",
            "入金予定金額 → 2,3,5,12除外",
            "滞納残債フィルタ → なし（全件処理）",
            "「TEL携帯.1」 → 空でない値のみ",
        ],
        process_function=process_faith_guarantor_data,
        title_icon="📞",
    )
    render_screen(config, "faith_guarantor")


def show_faith_emergency():
    config = ScreenConfig(
        title="オートコール用CSV加工",
        subtitle="フェイス緊急連絡人",
        filter_conditions=[
            "委託先法人ID → 1-4,8",
            "入金予定日 → 前日以前とNaN",
            "回収ランク → 「死亡決定」「破産決定」「弁護士介入」除外",
            "滞納残債 → 1円以上のみ対象",
            "入金予定金額 → 2,3,5,12除外",
            "滞納残債フィルタ → なし（全件処理）",
            "「緊急連絡人１のTEL（携帯）」 → 空でない値のみ",
        ],
        process_function=process_faith_emergencycontact_data,
        title_icon="📞",
    )
    render_screen(config, "faith_emergency")
