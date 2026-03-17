import streamlit as st

pg = st.navigation(
    [
        st.Page("pages/queneto.py", title="Queneto", icon="📊", default=True),
        st.Page("pages/cirad.py",   title="CIRAD",   icon="🥑", url_path="cirad"),
    ],
    position="hidden",
)
pg.run()
