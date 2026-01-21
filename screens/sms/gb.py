"""
ガレージバンクSMS処理画面モジュール
Business Data Processor

ガレージバンク用のSMS処理画面
- 契約者
"""

from components.screen_template import ScreenConfig, render_screen, create_payment_deadline_input
from services.sms import process_gb_sms_contract_data


def show_gb_sms_contract():
    config = ScreenConfig(
        title="SMS送信用CSV加工",
        subtitle="ガレージバンク　契約者",
        filter_conditions=[
            "委託先法人ID → 7",
            "入金予定日 → 前日以前とNaN",
            "回収ランク → 「弁護士介入」「破産決定」「死亡決定」除外",
            "滞納残債 → 1円以上のみ対象",
            "入金予定金額 → 2,3,5円除外",
            "TEL携帯 → 090/080/070形式のみ"
        ],
        process_function=process_gb_sms_contract_data,
        payment_deadline_input=create_payment_deadline_input,
        title_icon="📱"
    )
    render_screen(config, 'gb_sms_contract')
