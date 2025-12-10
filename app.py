"""
Business Data Processor v2.3.0
統合データ処理システム - SMS機能削除版

対応システム:
- ミライル用オートコール（6種類）
- フェイス用オートコール（3種類）
- プラザ用オートコール（3種類）
- フェイスSMS（1種類）- 支払期限機能付き
- アーク新規登録
- カプコ新規登録
- アーク残債更新
- カプコ残債の更新
"""

import streamlit as st
import pandas as pd
import io
import zipfile
from datetime import datetime, date

# 共通UIコンポーネントをインポート
from components.common_ui import (
    safe_dataframe_display,
    safe_csv_download,
    display_processing_logs,
    display_filter_conditions
)
from components.styles import get_custom_css
from components.sidebar import build_sidebar_menu
from components.result_display import display_processing_result, display_error_result
from components.welcome import show_welcome_screen

# screens import
from screens.mirail_autocall import (
    show_mirail_contract_without10k,
    show_mirail_contract_with10k,
    show_mirail_guarantor_without10k,
    show_mirail_guarantor_with10k,
    show_mirail_emergency_without10k,
    show_mirail_emergency_with10k
)
from screens.faith_autocall import (
    show_faith_contract,
    show_faith_guarantor,
    show_faith_emergency
)
from screens.plaza_autocall import (
    show_plaza_main,
    show_plaza_guarantor,
    show_plaza_contact
)
from screens.sms.faith import (
    show_faith_sms_vacated,
    show_faith_sms_guarantor,
    show_faith_sms_emergency_contact
)
from screens.sms.mirail import (
    show_mirail_sms_contract_id5,
    show_mirail_sms_contract_blank,
    show_mirail_sms_guarantor_id5,
    show_mirail_sms_guarantor_blank,
    show_mirail_sms_emergencycontact_id5,
    show_mirail_sms_emergencycontact_blank
)
from screens.sms.plaza import (
    show_plaza_sms_contract,
    show_plaza_sms_guarantor,
    show_plaza_sms_contact
)
from screens.registration.ark import (
    show_ark_registration_tokyo,
    show_ark_registration_osaka,
    show_ark_registration_hokkaido,
    show_ark_registration_kitakanto
)
from screens.registration.arktrust import (
    show_arktrust_registration_tokyo
)
from screens.registration.capco import (
    show_capco_registration
)
from screens.registration.plaza import (
    show_plaza_registration
)
from screens.registration.iog import (
    show_jid_registration
)
from screens.registration.nap import (
    show_nap_registration
)
from screens.debt_update.ark_late_payment import (
    show_ark_late_payment
)
from screens.debt_update.capco_debt_update import (
    show_capco_debt_update
)
from screens.debt_update.plaza_debt_update import (
    show_plaza_debt_update
)
from screens.notification.faith import (
    render_faith_notification_contractor,
    render_faith_notification_guarantor,
    render_faith_notification_contact,
    render_faith_c_litigation,
    render_faith_c_excluded,
    render_faith_c_evicted,
    render_faith_g_litigation,
    render_faith_g_excluded,
    render_faith_g_evicted,
    render_faith_e_litigation,
    render_faith_e_excluded,
    render_faith_e_evicted
)
from screens.notification.mirail import (
    show_mirail_contractor_145,
    show_mirail_contractor_not145,
    show_mirail_guarantor_145,
    show_mirail_guarantor_not145,
    show_mirail_contact_145,
    show_mirail_contact_not145
)
from screens.billing.residence_survey import (
    render_residence_survey_billing
)
from screens.visit_list import (
    render_visit_list
)
from screens.autocall_history import (
    render_autocall_history
)
from screens.fine_history import (
    render_fine_history
)

# プロセッサーをインポート

from processors.ark_registration import process_ark_data, process_arktrust_data
from processors.ark_late_payment_update import process_ark_late_payment_data
from processors.capco_registration import process_capco_data
from processors.plaza_debt_update import process_plaza_debt_update
from processors.faith_notification import process_faith_notification

def main():
    st.set_page_config(
        page_title="Business Data Processor",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # プロセッサーのマッピング
    PROCESSOR_MAPPING = {
        "mirail_contract_without10k": show_mirail_contract_without10k,
        "mirail_contract_with10k": show_mirail_contract_with10k,
        "mirail_guarantor_without10k": show_mirail_guarantor_without10k,
        "mirail_guarantor_with10k": show_mirail_guarantor_with10k,
        "mirail_emergency_without10k": show_mirail_emergency_without10k,
        "mirail_emergency_with10k": show_mirail_emergency_with10k,
        "faith_contract": show_faith_contract,
        "faith_guarantor": show_faith_guarantor,
        "faith_emergency": show_faith_emergency,
        "plaza_main": show_plaza_main,
        "plaza_guarantor": show_plaza_guarantor,
        "plaza_contact": show_plaza_contact,
        "faith_sms_vacated": show_faith_sms_vacated,
        "faith_sms_guarantor": show_faith_sms_guarantor,
        "faith_sms_emergency_contact": show_faith_sms_emergency_contact,
        "mirail_sms_contract_id5": show_mirail_sms_contract_id5,
        "mirail_sms_contract_blank": show_mirail_sms_contract_blank,
        "mirail_sms_guarantor_id5": show_mirail_sms_guarantor_id5,
        "mirail_sms_guarantor_blank": show_mirail_sms_guarantor_blank,
        "mirail_sms_emergencycontact_id5": show_mirail_sms_emergencycontact_id5,
        "mirail_sms_emergencycontact_blank": show_mirail_sms_emergencycontact_blank,
        "plaza_sms_contract": show_plaza_sms_contract,
        "plaza_sms_guarantor": show_plaza_sms_guarantor,
        "plaza_sms_contact": show_plaza_sms_contact,
        "ark_registration_tokyo": show_ark_registration_tokyo,
        "ark_registration_osaka": show_ark_registration_osaka,
        "ark_registration_hokkaido": show_ark_registration_hokkaido,
        "ark_registration_kitakanto": show_ark_registration_kitakanto,
        "arktrust_registration_tokyo": show_arktrust_registration_tokyo,
        "capco_registration": show_capco_registration,
        "plaza_registration": show_plaza_registration,
        "jid_registration": show_jid_registration,
        "nap_registration": show_nap_registration,
        "ark_late_payment": show_ark_late_payment,
        "capco_debt_update": show_capco_debt_update,
        "plaza_debt_update": show_plaza_debt_update,
        "faith_notification_contractor": render_faith_notification_contractor,
        "faith_notification_guarantor": render_faith_notification_guarantor,
        "faith_notification_contact": render_faith_notification_contact,
        "faith_c_litigation": render_faith_c_litigation,
        "faith_c_excluded": render_faith_c_excluded,
        "faith_c_evicted": render_faith_c_evicted,
        "faith_g_litigation": render_faith_g_litigation,
        "faith_g_excluded": render_faith_g_excluded,
        "faith_g_evicted": render_faith_g_evicted,
        "faith_e_litigation": render_faith_e_litigation,
        "faith_e_excluded": render_faith_e_excluded,
        "faith_e_evicted": render_faith_e_evicted,
        "mirail_c_145": show_mirail_contractor_145,
        "mirail_c_not145": show_mirail_contractor_not145,
        "mirail_g_145": show_mirail_guarantor_145,
        "mirail_g_not145": show_mirail_guarantor_not145,
        "mirail_e_145": show_mirail_contact_145,
        "mirail_e_not145": show_mirail_contact_not145,
        "residence_survey_billing": render_residence_survey_billing,
        "visit_list": render_visit_list,
        "autocall_history": render_autocall_history,
        "fine_history": render_fine_history
    }
    
    # カスタムCSSを適用
    st.markdown(get_custom_css(), unsafe_allow_html=True)
    
    # 固定ヘッダー
    st.title("📊 Business Data Processor")
    st.markdown("**📊 Business Data Processor** - 業務データ処理統合システム")
    
    # セッション状態の初期化
    if 'selected_processor' not in st.session_state:
        st.session_state.selected_processor = None
    
    # サイドバー：プルダウンレス常時表示メニュー
    build_sidebar_menu()
    
    # メインコンテンツエリア
    if st.session_state.selected_processor is None:
        show_welcome_screen()
        return
    
    # 各プロセッサーの処理画面
    processor = st.session_state.selected_processor
    if processor in PROCESSOR_MAPPING:
        PROCESSOR_MAPPING[processor]()


if __name__ == "__main__":
    main()