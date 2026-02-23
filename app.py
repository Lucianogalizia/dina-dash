# ==========================================================
# app.py — Entry point DINA (Dash)
#
# Arquitectura:
#   - app.py:         layout base + navbar + registro de páginas
#   - pages/*.py:     cada tab es una página independiente
#   - data/*.py:      loaders, parsers, consolidado (sin cambios vs Streamlit)
#   - utils/*.py:     helpers reutilizables
#
# Deploy: Cloud Run (mismo Dockerfile, misma infra)
# ==========================================================

import dash
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc

app = Dash(
    __name__,
    use_pages=True,                          # activa el sistema de páginas
    external_stylesheets=[dbc.themes.DARKLY], # tema oscuro similar al Streamlit actual
    suppress_callback_exceptions=True,
    title="DINA — Cartas de Superficie",
)
server = app.server  # para gunicorn en Cloud Run

# ---------- Navbar ----------
navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("📈 Mediciones",           href="/mediciones")),
        dbc.NavItem(dbc.NavLink("📊 Estadísticas",         href="/estadisticas")),
        dbc.NavItem(dbc.NavLink("🗺️ Mapa",                 href="/mapa")),
        dbc.NavItem(dbc.NavLink("🤖 Diagnósticos",         href="/diagnosticos")),
    ],
    brand="📈 DINA — Interfaz DIN",
    brand_href="/mediciones",
    color="dark",
    dark=True,
    fluid=True,
)

# ---------- Layout principal ----------
app.layout = html.Div([
    navbar,
    dbc.Container(
        dash.page_container,   # cada página se renderiza aquí — SIN recargar el navbar
        fluid=True,
        className="py-3",
    ),
])


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8080)
