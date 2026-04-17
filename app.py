import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from auth_check import require_auth
require_auth()

import streamlit as st

pg = st.navigation(
    [
        st.Page("pages/queneto.py", title="Queneto", icon="📊", default=True),
        st.Page("pages/cirad.py",   title="CIRAD",   icon="🥑", url_path="cirad"),
    ],
    position="hidden",
)
pg.run()
