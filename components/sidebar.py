"""
サイドバーメニューモジュール
Business Data Processor

サイドバーのメニュー構築を管理
"""

import streamlit as st


def build_sidebar_menu():
    """サイドバーメニューを構築"""
    with st.sidebar:
        # ウェルカムボタン
        if st.button("🏠 ホーム", key="home", use_container_width=True):
            st.session_state.selected_processor = None
        
        # 📞 オートコール用CSV加工
        st.markdown('<div class="sidebar-category">📞 オートコール用CSV加工</div>', unsafe_allow_html=True)
        
        # ミライル用オートコール
        st.markdown('<div class="sidebar-subcategory">🏢 ミライル用オートコール</div>', unsafe_allow_html=True)
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
        
        # フェイス用オートコール
        st.markdown('<div class="sidebar-subcategory">📱 フェイス用オートコール</div>', unsafe_allow_html=True)
        if st.button("契約者", key="faith_contract", use_container_width=True):
            st.session_state.selected_processor = "faith_contract"
        if st.button("保証人", key="faith_guarantor", use_container_width=True):
            st.session_state.selected_processor = "faith_guarantor"
        if st.button("緊急連絡人", key="faith_emergency_contact", use_container_width=True):
            st.session_state.selected_processor = "faith_emergency"
        
        # プラザ用オートコール
        st.markdown('<div class="sidebar-subcategory">🏪 プラザ用オートコール</div>', unsafe_allow_html=True)
        if st.button("契約者", key="plaza_main", use_container_width=True):
            st.session_state.selected_processor = "plaza_main"
        if st.button("保証人", key="plaza_guarantor", use_container_width=True):
            st.session_state.selected_processor = "plaza_guarantor"
        if st.button("緊急連絡人", key="plaza_contact", use_container_width=True):
            st.session_state.selected_processor = "plaza_contact"
        
        # 📱 SMS送信用CSV加工
        st.markdown('<div class="sidebar-category">📱 SMS送信用CSV加工</div>', unsafe_allow_html=True)
        
        # ミライル用SMS
        st.markdown('<div class="sidebar-subcategory">ミライル用SMS</div>', unsafe_allow_html=True)
        if st.button("ミライル　契約者", key="mirail_sms_contract", use_container_width=True):
            st.session_state.selected_processor = "mirail_sms_contract"
        if st.button("ミライル　保証人", key="mirail_sms_guarantor", use_container_width=True):
            st.session_state.selected_processor = "mirail_sms_guarantor"
        if st.button("ミライル　連絡人", key="mirail_sms_emergencycontact", use_container_width=True):
            st.session_state.selected_processor = "mirail_sms_emergencycontact"
        
        # フェイス用SMS
        st.markdown('<div class="sidebar-subcategory">フェイス用SMS</div>', unsafe_allow_html=True)
        if st.button("フェイス　契約者", key="faith_sms_vacated", use_container_width=True):
            st.session_state.selected_processor = "faith_sms_vacated"
        if st.button("フェイス　保証人", key="faith_sms_guarantor", use_container_width=True):
            st.session_state.selected_processor = "faith_sms_guarantor"
        if st.button("フェイス　連絡人", key="faith_sms_emergency_contact", use_container_width=True):
            st.session_state.selected_processor = "faith_sms_emergency_contact"
        
        # プラザ用SMS
        st.markdown('<div class="sidebar-subcategory">プラザ用SMS</div>', unsafe_allow_html=True)
        if st.button("プラザ　契約者", key="plaza_sms_contract", use_container_width=True):
            st.session_state.selected_processor = "plaza_sms_contract"
        if st.button("プラザ　保証人", key="plaza_sms_guarantor", use_container_width=True):
            st.session_state.selected_processor = "plaza_sms_guarantor"
        if st.button("プラザ　連絡人", key="plaza_sms_contact", use_container_width=True):
            st.session_state.selected_processor = "plaza_sms_contact"
        
        # 📋 新規登録用CSV加工
        st.markdown('<div class="sidebar-category">📋 新規登録用CSV加工</div>', unsafe_allow_html=True)
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
        if st.button("プラザ新規登録", key="plaza_registration", use_container_width=True):
            st.session_state.selected_processor = "plaza_registration"
        
        # 💰 残債の更新用CSV加工
        st.markdown('<div class="sidebar-category">💰 残債の更新用CSV加工</div>', unsafe_allow_html=True)
        if st.button("アーク残債の更新", key="ark_late_payment", use_container_width=True):
            st.session_state.selected_processor = "ark_late_payment"
        if st.button("カプコ残債の更新", key="capco_debt_update", use_container_width=True):
            st.session_state.selected_processor = "capco_debt_update"
        if st.button("プラザ残債の更新", key="plaza_debt_update", use_container_width=True):
            st.session_state.selected_processor = "plaza_debt_update"