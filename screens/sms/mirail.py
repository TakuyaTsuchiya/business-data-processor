"""
ミライルSMS処理画面モジュール
Business Data Processor

ミライル用のSMS処理画面（3種類）
- 契約者
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
    process_mirail_sms_contract_data,
    process_mirail_sms_guarantor_data,
    process_mirail_sms_emergencycontact_data
)


def show_mirail_sms_contract():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="ミライル　契約者",
        filter_conditions=[
            "DO列　委託先法人ID → 5と空白セルのみ選択",
            "CI列　回収ランク → 「弁護士介入」「訴訟中」のみ除外",
            "BU列　入金予定日 → 前日以前が対象（当日は除外）",
            "BV列　入金予定金額 → 2,3,5,12を除外",
            "AB列　TEL携帯 → 090/080/070形式の携帯電話番号のみ"
        ],
        process_function=process_mirail_sms_contract_data,
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'mirail_sms_contract')


def show_mirail_sms_guarantor():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="ミライル　保証人",
        filter_conditions=[
            "DO列　委託先法人ID → 5と空白セルのみ選択",
            "CI列　回収ランク → 「弁護士介入」「訴訟中」のみ除外",
            "BU列　入金予定日 → 前日以前が対象（当日は除外）",
            "BV列　入金予定金額 → 2,3,5,12を除外",
            "AU列　TEL携帯 → 090/080/070形式の携帯電話番号のみ"
        ],
        process_function=process_mirail_sms_guarantor_data,
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'mirail_sms_guarantor')


def show_mirail_sms_emergencycontact():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="ミライル　連絡人",
        filter_conditions=[
            "DO列　委託先法人ID → 5と空白セルのみ選択",
            "CI列　回収ランク → 「弁護士介入」「訴訟中」のみ除外",
            "BU列　入金予定日 → 前日以前が対象（当日は除外）",
            "BV列　入金予定金額 → 2,3,5,12を除外",
            "BE列　TEL携帯 → 090/080/070形式の携帯電話番号のみ"
        ],
        process_function=process_mirail_sms_emergencycontact_data,
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'mirail_sms_emergencycontact')