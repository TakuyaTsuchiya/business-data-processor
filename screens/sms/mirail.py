"""
ミライルSMS処理画面モジュール
Business Data Processor

ミライル用のSMS処理画面（6種類）
- 契約者（ID=5）
- 契約者（空白）
- 保証人（ID=5）
- 保証人（空白）
- 連絡人（ID=5）
- 連絡人（空白）
"""

import streamlit as st
from datetime import date
from functools import partial
from components.common_ui import (
    safe_csv_download,
    display_processing_logs
)
from components.result_display import display_error_result
from components.screen_template import ScreenConfig, render_screen, create_payment_deadline_input
from services.sms import (
    process_mirail_sms_contract_data,
    process_mirail_sms_contract_today_data,
    process_mirail_sms_guarantor_data,
    process_mirail_sms_emergencycontact_data
)


# =============================================================================
# 契約者（ID=5）
# =============================================================================
def show_mirail_sms_contract_id5():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="ミライル　契約者（ID=5）",
        filter_conditions=[
            "DO列　委託先法人ID → 5のみ選択",
            "CI列　回収ランク → 「弁護士介入」「訴訟中」のみ除外",
            "BU列　入金予定日 → 前日以前が対象（当日は除外）",
            "滞納残債 → 1円以上のみ対象",
            "BV列　入金予定金額 → 2,3,5,12を除外",
            "AB列　TEL携帯 → 090/080/070形式の携帯電話番号のみ"
        ],
        process_function=partial(process_mirail_sms_contract_data, trustee_filter_type='id5'),
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'mirail_sms_contract_id5')


# =============================================================================
# 契約者（空白）
# =============================================================================
def show_mirail_sms_contract_blank():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="ミライル　契約者（空白）",
        filter_conditions=[
            "DO列　委託先法人ID → 空白のみ選択",
            "CI列　回収ランク → 「弁護士介入」「訴訟中」のみ除外",
            "BU列　入金予定日 → 前日以前が対象（当日は除外）",
            "滞納残債 → 1円以上のみ対象",
            "BV列　入金予定金額 → 2,3,5,12を除外",
            "AB列　TEL携帯 → 090/080/070形式の携帯電話番号のみ"
        ],
        process_function=partial(process_mirail_sms_contract_data, trustee_filter_type='blank'),
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'mirail_sms_contract_blank')


# =============================================================================
# 保証人（ID=5）
# =============================================================================
def show_mirail_sms_guarantor_id5():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="ミライル　保証人（ID=5）",
        filter_conditions=[
            "DO列　委託先法人ID → 5のみ選択",
            "CI列　回収ランク → 「弁護士介入」「訴訟中」のみ除外",
            "BU列　入金予定日 → 前日以前が対象（当日は除外）",
            "滞納残債 → 1円以上のみ対象",
            "BV列　入金予定金額 → 2,3,5,12を除外",
            "AU列　TEL携帯 → 090/080/070形式の携帯電話番号のみ"
        ],
        process_function=partial(process_mirail_sms_guarantor_data, trustee_filter_type='id5'),
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'mirail_sms_guarantor_id5')


# =============================================================================
# 保証人（空白）
# =============================================================================
def show_mirail_sms_guarantor_blank():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="ミライル　保証人（空白）",
        filter_conditions=[
            "DO列　委託先法人ID → 空白のみ選択",
            "CI列　回収ランク → 「弁護士介入」「訴訟中」のみ除外",
            "BU列　入金予定日 → 前日以前が対象（当日は除外）",
            "滞納残債 → 1円以上のみ対象",
            "BV列　入金予定金額 → 2,3,5,12を除外",
            "AU列　TEL携帯 → 090/080/070形式の携帯電話番号のみ"
        ],
        process_function=partial(process_mirail_sms_guarantor_data, trustee_filter_type='blank'),
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'mirail_sms_guarantor_blank')


# =============================================================================
# 連絡人（ID=5）
# =============================================================================
def show_mirail_sms_emergencycontact_id5():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="ミライル　連絡人（ID=5）",
        filter_conditions=[
            "DO列　委託先法人ID → 5のみ選択",
            "CI列　回収ランク → 「弁護士介入」「訴訟中」のみ除外",
            "BU列　入金予定日 → 前日以前が対象（当日は除外）",
            "滞納残債 → 1円以上のみ対象",
            "BV列　入金予定金額 → 2,3,5,12を除外",
            "BE列　TEL携帯 → 090/080/070形式の携帯電話番号のみ"
        ],
        process_function=partial(process_mirail_sms_emergencycontact_data, trustee_filter_type='id5'),
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'mirail_sms_emergencycontact_id5')


# =============================================================================
# 連絡人（空白）
# =============================================================================
def show_mirail_sms_emergencycontact_blank():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="ミライル　連絡人（空白）",
        filter_conditions=[
            "DO列　委託先法人ID → 空白のみ選択",
            "CI列　回収ランク → 「弁護士介入」「訴訟中」のみ除外",
            "BU列　入金予定日 → 前日以前が対象（当日は除外）",
            "滞納残債 → 1円以上のみ対象",
            "BV列　入金予定金額 → 2,3,5,12を除外",
            "BE列　TEL携帯 → 090/080/070形式の携帯電話番号のみ"
        ],
        process_function=partial(process_mirail_sms_emergencycontact_data, trustee_filter_type='blank'),
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'mirail_sms_emergencycontact_blank')


# =============================================================================
# 当日SMS用　契約者（ID=5）
# =============================================================================
def show_mirail_sms_contract_today():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="ミライル　当日SMS用　契約者　委託先法人ID→5",
        filter_conditions=[
            "DO列　委託先法人ID → 5のみ選択",
            "CI列　回収ランク → 「訴訟中」「弁護士介入」を除外",
            "BU列　入金予定日 → 当日のみ対象",
            "BV列　入金予定金額 → 13円以上のみ対象",
            "CR列　クライアントCD → 10, 40, 9268を除外",
            "BT列　滞納残債 → 1円以上",
            "AB列　TEL携帯 → 090/080/070形式の携帯電話番号のみ"
        ],
        process_function=process_mirail_sms_contract_today_data,
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'mirail_sms_contract_today')
