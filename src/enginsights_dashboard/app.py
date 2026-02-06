from __future__ import annotations

from dash import Dash, Input, Output, dcc, html, dash_table
import plotly.express as px

from .data_loader import load_prs, load_teams
from .summary_engine import ScopeSelection, SummaryEngine


def _load_engine() -> tuple[SummaryEngine | None, str | None]:
    try:
        prs_df = load_prs()
        teams_df = load_teams()
        return SummaryEngine(prs_df, teams_df), None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


engine, data_error = _load_engine()

if engine:
    author_options = [
        {"label": author, "value": author} for author in engine.available_authors()
    ]
    team_options = [
        {"label": team, "value": team} for team in engine.available_teams()
    ]
    repo_options = [
        {"label": repo, "value": repo} for repo in engine.available_repos()
    ]
else:
    author_options = []
    team_options = []
    repo_options = []

app = Dash(__name__)
app.title = "Engineering Effectiveness Dashboard"

app.layout = html.Div(
    style={"fontFamily": "Helvetica, Arial, sans-serif", "padding": "24px"},
    children=[
        html.H1("Engineering Effectiveness Dashboard (V2)"),
        html.Div(
            style={"display": "flex", "gap": "16px", "flexWrap": "wrap"},
            children=[
                html.Div(
                    style={"minWidth": "220px"},
                    children=[
                        html.Label("View Mode"),
                        dcc.Dropdown(
                            id="scope-select",
                            options=[
                                {"label": "Org", "value": "org"},
                                {"label": "Team", "value": "team"},
                                {"label": "Individual", "value": "individual"},
                            ],
                            value="org",
                            clearable=False,
                        ),
                    ],
                ),
                html.Div(
                    style={"minWidth": "240px"},
                    children=[
                        html.Label("Team"),
                        dcc.Dropdown(
                            id="team-select",
                            options=team_options,
                            placeholder="Select team",
                            searchable=True,
                        ),
                    ],
                ),
                html.Div(
                    style={"minWidth": "240px"},
                    children=[
                        html.Label("User"),
                        dcc.Dropdown(
                            id="user-select",
                            options=author_options,
                            placeholder="Select user",
                            searchable=True,
                        ),
                    ],
                ),
                html.Div(
                    style={"minWidth": "240px"},
                    children=[
                        html.Label("Org Group By"),
                        dcc.Dropdown(
                            id="org-group-select",
                            options=[
                                {"label": "Team", "value": "team"},
                                {"label": "Repository", "value": "repository"},
                            ],
                            value="team",
                            clearable=False,
                        ),
                    ],
                ),
            ],
        ),
        html.Hr(),
        dcc.Tabs(
            id="tabs",
            value="exec",
            children=[
                dcc.Tab(label="Executive Summary", value="exec"),
                dcc.Tab(label="Team Comparison", value="team"),
                dcc.Tab(label="Contributor Deep-Dive", value="contrib"),
            ],
        ),
        html.Div(id="tab-content", style={"marginTop": "16px"}),
        html.Div(
            id="data-error",
            style={"color": "crimson", "marginTop": "16px"},
            children=data_error or "",
        ),
    ],
)


@app.callback(
    Output("team-select", "disabled"),
    Output("user-select", "disabled"),
    Input("scope-select", "value"),
)
def toggle_scope_inputs(scope: str) -> tuple[bool, bool]:
    if scope == "team":
        return False, True
    if scope == "individual":
        return True, False
    return True, True


@app.callback(
    Output("tab-content", "children"),
    Input("tabs", "value"),
    Input("scope-select", "value"),
    Input("team-select", "value"),
    Input("user-select", "value"),
    Input("org-group-select", "value"),
)
def render_tab(
    tab: str,
    scope: str,
    selected_team: str | None,
    selected_user: str | None,
    org_group_by: str,
):
    if engine is None:
        return html.Div("No data loaded. Add data/prs.ipc and data/teams.csv.")

    selection = ScopeSelection(
        scope=scope, selected_user=selected_user, selected_team=selected_team
    )
    scoped_df = engine.scoped_df(selection)

    if scoped_df.is_empty():
        return html.Div("No data for the selected scope.")

    kpi_df = engine.aggregate(scoped_df, None)
    kpi_cards = _render_kpis(kpi_df)

    if tab == "exec":
        group_by = org_group_by
    elif tab == "team":
        group_by = "team"
    else:
        group_by = "author"

    if group_by not in scoped_df.columns:
        return html.Div([kpi_cards, html.Div(f"Missing column: {group_by}")])

    agg_df = engine.aggregate(scoped_df, group_by)
    chart = _render_bar_chart(agg_df, group_by)
    table = _render_table(agg_df)

    return html.Div([kpi_cards, chart, table])


def _render_kpis(kpi_df):
    row = kpi_df.row(0, named=True)
    items = [
        ("Total Merged PRs", row.get("total_merged_prs")),
        ("Median Lead Time (hrs)", _fmt(row.get("lead_time_median_hrs"))),
        ("Median Review Latency (hrs)", _fmt(row.get("review_latency_median_hrs"))),
        ("Avg Code Churn", _fmt(row.get("code_churn_avg"))),
    ]
    return html.Div(
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
        children=[
            html.Div(
                style={
                    "padding": "12px 16px",
                    "border": "1px solid #ddd",
                    "borderRadius": "8px",
                    "minWidth": "180px",
                },
                children=[html.Div(label), html.Div(str(value))],
            )
            for label, value in items
        ],
    )


def _render_bar_chart(agg_df, group_by: str):
    pdf = agg_df.to_pandas()
    fig = px.bar(
        pdf,
        x=group_by,
        y="total_merged_prs",
        title="Merged PRs",
    )
    fig.update_layout(margin={"l": 20, "r": 20, "t": 40, "b": 20})
    return dcc.Graph(figure=fig)


def _render_table(agg_df):
    pdf = agg_df.to_pandas()
    return dash_table.DataTable(
        data=pdf.to_dict("records"),
        columns=[{"name": col, "id": col} for col in pdf.columns],
        page_size=10,
        style_table={"overflowX": "auto"},
    )


def _fmt(value):
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.2f}"
    return value


def main() -> None:
    app.run_server(debug=True)


if __name__ == "__main__":
    main()
