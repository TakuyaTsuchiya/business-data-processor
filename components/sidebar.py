"""
サイドバーメニューモジュール
Business Data Processor

サイドバーのメニュー構築を管理（タブUI版）
"""

import streamlit as st


def build_sidebar_menu():
    """サイドバーメニューを構築（タブUI版）"""
    with st.sidebar:
        # ウェルカムボタン
        if st.button("🏠 ホーム", key="home", use_container_width=True):
            st.session_state.selected_processor = None

        st.markdown("---")

        # タブ選択状態の初期化
        if 'selected_tab' not in st.session_state:
            st.session_state.selected_tab = "ミライル"

        # タブ選択ボタン（4つ）
        st.markdown("### 📂 カテゴリ選択")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🏢 ミライル", key="tab_mirail", use_container_width=True,
                        type="primary" if st.session_state.selected_tab == "ミライル" else "secondary"):
                st.session_state.selected_tab = "ミライル"
                st.rerun()
            if st.button("🏪 プラザ", key="tab_plaza", use_container_width=True,
                        type="primary" if st.session_state.selected_tab == "プラザ" else "secondary"):
                st.session_state.selected_tab = "プラザ"
                st.rerun()
        with col2:
            if st.button("📱 フェイス", key="tab_faith", use_container_width=True,
                        type="primary" if st.session_state.selected_tab == "フェイス" else "secondary"):
                st.session_state.selected_tab = "フェイス"
                st.rerun()
            if st.button("⚙️ その他", key="tab_others", use_container_width=True,
                        type="primary" if st.session_state.selected_tab == "その他" else "secondary"):
                st.session_state.selected_tab = "その他"
                st.rerun()

        st.markdown("---")

        # 選択されたタブに応じてメニューを表示
        if st.session_state.selected_tab == "ミライル":
            _show_mirail_menu()
        elif st.session_state.selected_tab == "フェイス":
            _show_faith_menu()
        elif st.session_state.selected_tab == "プラザ":
            _show_plaza_menu()
        elif st.session_state.selected_tab == "その他":
            _show_others_menu()


def _show_mirail_menu():
    """ミライルメニュー表示"""
    # オートコール
    st.markdown('<div class="sidebar-category">📞 ミライル用オートコール</div>', unsafe_allow_html=True)
    if st.button("契約者（10,000円を除外するパターン）", key="mirail_contract_without10k", use_container_width=True):
        st.session_state.selected_processor = "mirail_contract_without10k"
    if st.button("契約者（10,000円を除外しないパターン）", key="mirail_contract_with10k", use_container_width=True):
        st.session_state.selected_processor = "mirail_contract_with10k"
    if st.button("保証人（10,000円を除外するパターン）", key="mirail_guarantor_without10k", use_container_width=True):
        st.session_state.selected_processor = "mirail_guarantor_without10k"
    if st.button("保証人（10,000円を除外しないパターン）", key="mirail_guarantor_with10k", use_container_width=True):
        st.session_state.selected_processor = "mirail_guarantor_with10k"
    if st.button("緊急連絡人（10,000円を除外するパターン）", key="mirail_emergency_without10k", use_container_width=True):
        st.session_state.selected_processor = "mirail_emergency_without10k"
    if st.button("緊急連絡人（10,000円を除外しないパターン）", key="mirail_emergency_with10k", use_container_width=True):
        st.session_state.selected_processor = "mirail_emergency_with10k"

    # SMS
    st.markdown('<div class="sidebar-category">📱 ミライル用SMS送信用</div>', unsafe_allow_html=True)
    if st.button("契約者　委託先法人ID→5", key="mirail_sms_contract_id5", use_container_width=True):
        st.session_state.selected_processor = "mirail_sms_contract_id5"
    if st.button("契約者　委託先法人ID→空白", key="mirail_sms_contract_blank", use_container_width=True):
        st.session_state.selected_processor = "mirail_sms_contract_blank"
    if st.button("保証人　委託先法人ID→5", key="mirail_sms_guarantor_id5", use_container_width=True):
        st.session_state.selected_processor = "mirail_sms_guarantor_id5"
    if st.button("保証人　委託先法人ID→空白", key="mirail_sms_guarantor_blank", use_container_width=True):
        st.session_state.selected_processor = "mirail_sms_guarantor_blank"
    if st.button("連絡人　委託先法人ID→5", key="mirail_sms_emergencycontact_id5", use_container_width=True):
        st.session_state.selected_processor = "mirail_sms_emergencycontact_id5"
    if st.button("連絡人　委託先法人ID→空白", key="mirail_sms_emergencycontact_blank", use_container_width=True):
        st.session_state.selected_processor = "mirail_sms_emergencycontact_blank"

    # 催告書
    st.markdown('<div class="sidebar-category">📝 ミライル用催告書 差し込みリスト</div>', unsafe_allow_html=True)
    if st.button("契約者（1,4,5）", key="mirail_c_145", use_container_width=True):
        st.session_state.selected_processor = "mirail_c_145"
    if st.button("契約者（1,4,5,10,40以外）", key="mirail_c_not145", use_container_width=True):
        st.session_state.selected_processor = "mirail_c_not145"
    if st.button("保証人（1,4,5）", key="mirail_g_145", use_container_width=True):
        st.session_state.selected_processor = "mirail_g_145"
    if st.button("保証人（1,4,5,10,40以外）", key="mirail_g_not145", use_container_width=True):
        st.session_state.selected_processor = "mirail_g_not145"
    if st.button("連絡人（1,4,5）", key="mirail_e_145", use_container_width=True):
        st.session_state.selected_processor = "mirail_e_145"
    if st.button("連絡人（1,4,5,10,40以外）", key="mirail_e_not145", use_container_width=True):
        st.session_state.selected_processor = "mirail_e_not145"


def _show_faith_menu():
    """フェイスメニュー表示"""
    # オートコール
    st.markdown('<div class="sidebar-category">📞 フェイス用オートコール</div>', unsafe_allow_html=True)
    if st.button("契約者", key="faith_contract", use_container_width=True):
        st.session_state.selected_processor = "faith_contract"
    if st.button("保証人", key="faith_guarantor", use_container_width=True):
        st.session_state.selected_processor = "faith_guarantor"
    if st.button("緊急連絡人", key="faith_emergency_contact", use_container_width=True):
        st.session_state.selected_processor = "faith_emergency"

    # SMS
    st.markdown('<div class="sidebar-category">📱 フェイス用SMS送信用</div>', unsafe_allow_html=True)
    if st.button("契約者", key="faith_sms_vacated", use_container_width=True):
        st.session_state.selected_processor = "faith_sms_vacated"
    if st.button("保証人", key="faith_sms_guarantor", use_container_width=True):
        st.session_state.selected_processor = "faith_sms_guarantor"
    if st.button("連絡人", key="faith_sms_emergency_contact", use_container_width=True):
        st.session_state.selected_processor = "faith_sms_emergency_contact"

    # 催告書
    st.markdown('<div class="sidebar-category">📝 フェイス用催告書 差し込みリスト</div>', unsafe_allow_html=True)
    if st.button("契約者「入居中」「訴訟中」", key="faith_c_litigation", use_container_width=True):
        st.session_state.selected_processor = "faith_c_litigation"
    if st.button("契約者「入居中」「訴訟対象外」", key="faith_c_excluded", use_container_width=True):
        st.session_state.selected_processor = "faith_c_excluded"
    if st.button("契約者「退去済み」", key="faith_c_evicted", use_container_width=True):
        st.session_state.selected_processor = "faith_c_evicted"
    if st.button("連帯保証人「入居中」「訴訟中」", key="faith_g_litigation", use_container_width=True):
        st.session_state.selected_processor = "faith_g_litigation"
    if st.button("連帯保証人「入居中」「訴訟対象外」", key="faith_g_excluded", use_container_width=True):
        st.session_state.selected_processor = "faith_g_excluded"
    if st.button("連帯保証人「退去済み」", key="faith_g_evicted", use_container_width=True):
        st.session_state.selected_processor = "faith_g_evicted"
    if st.button("連絡人「入居中」「訴訟中」", key="faith_e_litigation", use_container_width=True):
        st.session_state.selected_processor = "faith_e_litigation"
    if st.button("連絡人「入居中」「訴訟対象外」", key="faith_e_excluded", use_container_width=True):
        st.session_state.selected_processor = "faith_e_excluded"
    if st.button("連絡人「退去済み」", key="faith_e_evicted", use_container_width=True):
        st.session_state.selected_processor = "faith_e_evicted"


def _show_plaza_menu():
    """プラザメニュー表示"""
    # オートコール
    st.markdown('<div class="sidebar-category">📞 プラザ用オートコール</div>', unsafe_allow_html=True)
    if st.button("契約者", key="plaza_main", use_container_width=True):
        st.session_state.selected_processor = "plaza_main"
    if st.button("保証人", key="plaza_guarantor", use_container_width=True):
        st.session_state.selected_processor = "plaza_guarantor"
    if st.button("緊急連絡人", key="plaza_contact", use_container_width=True):
        st.session_state.selected_processor = "plaza_contact"

    # SMS
    st.markdown('<div class="sidebar-category">📱 プラザ用SMS送信用</div>', unsafe_allow_html=True)
    if st.button("契約者", key="plaza_sms_contract", use_container_width=True):
        st.session_state.selected_processor = "plaza_sms_contract"
    if st.button("保証人", key="plaza_sms_guarantor", use_container_width=True):
        st.session_state.selected_processor = "plaza_sms_guarantor"
    if st.button("連絡人", key="plaza_sms_contact", use_container_width=True):
        st.session_state.selected_processor = "plaza_sms_contact"

    # 新規登録
    st.markdown('<div class="sidebar-category">📋 プラザ新規登録</div>', unsafe_allow_html=True)
    if st.button("プラザ新規登録", key="plaza_registration", use_container_width=True):
        st.session_state.selected_processor = "plaza_registration"

    # 残債更新
    st.markdown('<div class="sidebar-category">💰 プラザ残債の更新</div>', unsafe_allow_html=True)
    if st.button("プラザ残債の更新", key="plaza_debt_update", use_container_width=True):
        st.session_state.selected_processor = "plaza_debt_update"


def _show_others_menu():
    """その他メニュー表示"""
    # 新規登録
    st.markdown('<div class="sidebar-category">📋 新規登録</div>', unsafe_allow_html=True)
    if st.button("アーク新規登録（東京）", key="ark_registration_tokyo", use_container_width=True):
        st.session_state.selected_processor = "ark_registration_tokyo"
    if st.button("アーク新規登録（大阪）", key="ark_registration_osaka", use_container_width=True):
        st.session_state.selected_processor = "ark_registration_osaka"
    if st.button("アーク新規登録（北海道）", key="ark_registration_hokkaido", use_container_width=True):
        st.session_state.selected_processor = "ark_registration_hokkaido"
    if st.button("アーク新規登録（北関東）", key="ark_registration_kitakanto", use_container_width=True):
        st.session_state.selected_processor = "ark_registration_kitakanto"
    if st.button("アークトラスト新規登録（東京）", key="arktrust_registration_tokyo", use_container_width=True):
        st.session_state.selected_processor = "arktrust_registration_tokyo"
    if st.button("カプコ新規登録", key="capco_registration", use_container_width=True):
        st.session_state.selected_processor = "capco_registration"
    if st.button("IOG新規登録", key="iog_registration", use_container_width=True):
        st.session_state.selected_processor = "jid_registration"
    if st.button("ナップ新規登録", key="nap_registration", use_container_width=True):
        st.session_state.selected_processor = "nap_registration"

    # 残債の更新
    st.markdown('<div class="sidebar-category">💰 残債の更新</div>', unsafe_allow_html=True)
    if st.button("アーク残債の更新", key="ark_late_payment", use_container_width=True):
        st.session_state.selected_processor = "ark_late_payment"
    if st.button("カプコ残債の更新", key="capco_debt_update", use_container_width=True):
        st.session_state.selected_processor = "capco_debt_update"

    # 居住訪問調査報告書
    st.markdown('<div class="sidebar-category">居住訪問調査報告書</div>', unsafe_allow_html=True)
    if st.button("請求書作成用データを生成", key="residence_survey_billing", use_container_width=True):
        st.session_state.selected_processor = "residence_survey_billing"

    # 訪問リスト作成
    st.markdown('<div class="sidebar-category">📋 訪問リスト作成</div>', unsafe_allow_html=True)
    if st.button("訪問リスト作成", key="visit_list", use_container_width=True):
        st.session_state.selected_processor = "visit_list"

    # オートコール履歴
    st.markdown('<div class="sidebar-category">📋 オートコール履歴</div>', unsafe_allow_html=True)
    if st.button("オートコール履歴作成", key="autocall_history", use_container_width=True):
        st.session_state.selected_processor = "autocall_history"

    # ファイン履歴
    st.markdown('<div class="sidebar-category">📋 ファイン履歴</div>', unsafe_allow_html=True)
    if st.button("ファイン履歴作成", key="fine_history", use_container_width=True):
        st.session_state.selected_processor = "fine_history"
