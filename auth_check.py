"""Shared auth check for all pages."""
import streamlit as st

ALLOWED_DOMAINS = ("pchglobal.biz", "premiumchoice.biz")


def require_auth():
    """Block page execution unless user is logged in with allowed domain."""
    if not st.user.is_logged_in:
        st.title("Qeneto Dashboard")
        if st.button("Iniciar sesión", type="primary"):
            st.login("microsoft")
        st.stop()

    email = (st.user.email or "").lower()
    domain = email.split("@")[-1] if "@" in email else ""
    if domain not in ALLOWED_DOMAINS:
        st.error("Acceso no autorizado.")
        if st.button("Cerrar sesión"):
            st.logout()
        st.stop()

    with st.sidebar:
        st.caption(f"👤 {st.user.name or email}")
        if st.button("Cerrar sesión", use_container_width=True):
            st.logout()
