"""
フェイスSMS処理画面モジュール
Business Data Processor

フェイス用のSMS処理画面（3種類）
- 契約者（退去済み）
- 保証人
- 緊急連絡人
"""

import streamlit as st
from datetime import date
from components.common_ui import (
    safe_csv_download,
    display_processing_logs
)
from components.result_display import display_error_result
from components.screen_template import ScreenConfig, render_screen, create_payment_deadline_input
from services.sms import (
    process_faith_sms_contract_data,
    process_faith_sms_guarantor_data,
    process_faith_sms_emergencycontact_data
)


def show_faith_sms_vacated():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="フェイス　契約者",
        filter_conditions=[
            "委託先法人ID → 1-4",
            "入金予定日 → 前日以前とNaN",
            "入金予定金額 → 2,3,5円除外",
            "回収ランク → 「弁護士介入」「破産決定」「死亡決定」除外",
            "TEL携帯 → 090/080/070形式のみ"
        ],
        process_function=process_faith_sms_contract_data,
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'faith_sms_vacated')


def show_faith_sms_guarantor():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="フェイス　保証人",
        filter_conditions=[
            "委託先法人ID → 1-4",
            "入金予定日 → 前日以前とNaN",
            "入金予定金額 → 2,3,5円除外",
            "回収ランク → 「弁護士介入」「破産決定」「死亡決定」除外",
            "AU列TEL携帯 → 090/080/070形式のみ（保証人電話番号）"
        ],
        process_function=process_faith_sms_guarantor_data,
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'faith_sms_guarantor')


def show_faith_sms_emergency_contact():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="フェイス　連絡人",
        filter_conditions=[
            "委託先法人ID → 1-4",
            "入金予定日 → 前日以前とNaN",
            "入金予定金額 → 2,3,5円除外",
            "回収ランク → 「弁護士介入」「破産決定」「死亡決定」除外",
            "BE列「緊急連絡人１のTEL（携帯）」 → 090/080/070形式のみ"
        ],
        process_function=process_faith_sms_emergencycontact_data,
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'faith_sms_emergency_contact')