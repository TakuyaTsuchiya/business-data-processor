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
    show_mirail_sms_contract,
    show_mirail_sms_guarantor,
    show_mirail_sms_emergencycontact
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
from screens.debt_update.ark_late_payment import (
    show_ark_late_payment
)
from screens.debt_update.capco_debt_update import (
    show_capco_debt_update
)
from screens.debt_update.plaza_debt_update import (
    show_plaza_debt_update
)

# プロセッサーをインポート

from processors.ark_registration import process_ark_data, process_arktrust_data
from processors.ark_late_payment_update import process_ark_late_payment_data
from processors.capco_registration import process_capco_data
from processors.plaza_debt_update import process_plaza_debt_update

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
        "mirail_sms_guarantor": show_mirail_sms_guarantor,
        "mirail_sms_emergencycontact": show_mirail_sms_emergencycontact,
        "mirail_sms_contract": show_mirail_sms_contract,
        "plaza_sms_contract": show_plaza_sms_contract,
        "plaza_sms_guarantor": show_plaza_sms_guarantor,
        "plaza_sms_contact": show_plaza_sms_contact,
        "ark_registration_tokyo": show_ark_registration_tokyo,
        "ark_registration_osaka": show_ark_registration_osaka,
        "ark_registration_hokkaido": show_ark_registration_hokkaido,
        "ark_registration_kitakanto": show_ark_registration_kitakanto,
        "arktrust_registration_tokyo": show_arktrust_registration_tokyo,
        "capco_registration": show_capco_registration,
        "ark_late_payment": show_ark_late_payment,
        "capco_debt_update": show_capco_debt_update,
        "plaza_debt_update": show_plaza_debt_update
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