"""Dashboard for school policy term analysis: State Overview and District Detail."""
import json
import logging
import math
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import dash
from dash import dcc, html, Input, Output, State
from dash.dependencies import ALL, MATCH
from dash import dash_table
import pandas as pd
import plotly.graph_objects as go

from config import OUTPUT_DIR, RESULTS_CSV
from dashboard_data import (
    STATE_NAMES,
    aggregate_by_state,
    build_district_records,
    get_district_by_id,
    get_state_districts,
    keyword_breakdown,
    load_all_states_data,
)

logger = logging.getLogger(__name__)

# Color scheme constants
COLOR_SCHOOL = '#2E86AB'  # Blue
COLOR_DISTRICT = '#f77f00'  # Orange
COLOR_TOTAL = '#06a77d'  # Green
COLOR_NEUTRAL = '#6c757d'  # Gray
COLOR_BG_SCHOOL = '#e3f2fd'  # Light blue
COLOR_BG_DISTRICT = '#fff3e0'  # Light orange
COLOR_BG_TOTAL = '#e8f5e9'  # Light green


# ============================================================================
# DATA LAYER
# ============================================================================

def load_all_florida_data(csv_path: Optional[Path] = None) -> List[Dict]:
    """Load CSV, filter for Florida, prepare for storage."""
    # Check for updated summaries CSV first, then fall back to regular results CSV
    if csv_path is None:
        updated_csv = OUTPUT_DIR / "florida_with_ai_summary_updated.csv"
        if updated_csv.exists():
            csv_path = updated_csv
            logger.info(f"Using updated summaries CSV: {updated_csv}")
        else:
            csv_path = RESULTS_CSV
    
    if not csv_path.exists():
        logger.warning(f"Results CSV not found: {csv_path}")
        return []
    
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} results from CSV")
        
        # Filter for Florida
        fl_df = df[df['State'] == 'FL'].copy()
        logger.info(f"Florida schools: {len(fl_df)}")
        
        if fl_df.empty:
            return []
        
        # Convert to list of dicts, handling NaN values
        records = fl_df.to_dict('records')
        
        # Convert NaN/NA values to None for JSON serialization
        for record in records:
            for key, value in record.items():
                try:
                    if pd.isna(value):
                        record[key] = None
                    elif value is pd.NA or value is pd.NaT:
                        record[key] = None
                except (TypeError, ValueError):
                    if value is None or value == '':
                        record[key] = None
        
        return records
    except Exception as e:
        logger.error(f"Error loading data: {e}", exc_info=True)
        return []


def get_schools_with_ai_summaries(all_data: List[Dict]) -> List[Dict]:
    """Filter schools that have AI summaries."""
    return [
        s for s in all_data
        if safe_extract(s, 'AI Summary') and safe_extract(s, 'AI Summary') != 'None'
    ]


def categorize_schools_by_term_location(schools_data: List[Dict]) -> tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Categorize schools into three groups based on where terms appear:
    1. District-only: Terms only on district pages (no school pages)
    2. School-only: Terms only on school pages (no district pages)
    3. Both: Terms on both school and district pages
    
    Returns:
        (district_only, school_only, both)
    """
    district_only = []
    school_only = []
    both = []
    
    for school in schools_data:
        school_pages = safe_extract(school, 'School Pages With Terms', 0) or 0
        school_occurrences = safe_extract(school, 'School Total Occurrences', 0) or 0
        district_pages = safe_extract(school, 'District Pages With Terms', 0) or 0
        district_occurrences = safe_extract(school, 'District Total Occurrences', 0) or 0
        
        has_school_terms = school_pages > 0 or school_occurrences > 0
        has_district_terms = district_pages > 0 or district_occurrences > 0
        
        if has_district_terms and not has_school_terms:
            district_only.append(school)
        elif has_school_terms and not has_district_terms:
            school_only.append(school)
        elif has_school_terms and has_district_terms:
            both.append(school)
    
    return district_only, school_only, both


def get_districts_district_only(all_data: List[Dict]) -> List[Dict]:
    """Get districts where terms appear only on district pages, not school pages."""
    # Group by district
    districts = {}
    
    for record in all_data:
        district_name = safe_extract(record, 'District Name', '')
        if not district_name:
            continue
        
        if district_name not in districts:
            districts[district_name] = {
                'District Name': district_name,
                'District Pages With Terms': 0,
                'District Total Occurrences': 0,
                'District Terms Found': set(),
                'District Page URLs': set(),
                'Schools': [],
                'Has School Terms': False
            }
        
        # Check district-level data
        district_pages = safe_extract(record, 'District Pages With Terms', 0) or 0
        district_occurrences = safe_extract(record, 'District Total Occurrences', 0) or 0
        district_terms = safe_extract(record, 'District Terms Found', '')
        district_urls = safe_extract(record, 'District Page URLs', '')
        
        if district_pages > 0:
            districts[district_name]['District Pages With Terms'] = max(
                districts[district_name]['District Pages With Terms'], district_pages
            )
        if district_occurrences > 0:
            districts[district_name]['District Total Occurrences'] += district_occurrences
        
        if district_terms and district_terms != 'None':
            districts[district_name]['District Terms Found'].update(
                [t.strip() for t in str(district_terms).split(',') if t.strip()]
            )
        
        if district_urls and district_urls != 'None':
            districts[district_name]['District Page URLs'].update(
                parse_url_list(district_urls)
            )
        
        # Check if this school has school-level terms
        school_pages = safe_extract(record, 'School Pages With Terms', 0) or 0
        school_occurrences = safe_extract(record, 'School Total Occurrences', 0) or 0
        
        if school_pages > 0 or school_occurrences > 0:
            districts[district_name]['Has School Terms'] = True
        
        districts[district_name]['Schools'].append(record)
    
    # Filter for districts with district-only terms (no school terms)
    district_only = []
    for district_name, district_data in districts.items():
        has_district_terms = (
            district_data['District Pages With Terms'] > 0 or
            district_data['District Total Occurrences'] > 0
        )
        
        if has_district_terms and not district_data['Has School Terms']:
            # Convert sets to strings for display
            district_data['District Terms Found'] = ', '.join(sorted(district_data['District Terms Found']))
            district_data['District Page URLs'] = ', '.join(sorted(district_data['District Page URLs']))
            district_data['Total Schools'] = len(district_data['Schools'])
            district_only.append(district_data)
    
    return district_only


def safe_extract(data: Dict, key: str, default=None):
    """Safely extract value from dictionary with proper null handling."""
    if not data or key not in data:
        return default
    
    value = data.get(key, default)
    
    # Handle pandas NaN
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass
    
    # Handle None and empty string
    if value is None or value == '':
        return default
    
    return value


def parse_url_list(url_str: str) -> List[str]:
    """Parse comma-separated URL string into list of URLs."""
    if not url_str or pd.isna(url_str):
        return []
    if isinstance(url_str, str):
        urls = [u.strip() for u in url_str.split(',') if u.strip()]
        return urls
    return []


def format_term_list(term_str: str) -> str:
    """Format term list string for display."""
    if not term_str or pd.isna(term_str):
        return 'None'
    return str(term_str)


# ============================================================================
# UI COMPONENTS
# ============================================================================

def create_summary_stats(district_only: List[Dict], school_only: List[Dict], both: List[Dict]) -> html.Div:
    """Create summary statistics panel."""
    total_district_only = len(district_only)
    total_school_only = len(school_only)
    total_both = len(both)
    total_all = total_district_only + total_school_only + total_both
    
    return html.Div([
        html.H3("Summary Statistics", style={'marginBottom': '20px'}),
        html.P([
            html.Strong("Total Schools with AI Summaries: "), f"{total_all}"
        ], style={'fontSize': '16px', 'margin': '10px 0'}),
        html.P([
            html.Strong("District-Only: "), f"{total_district_only}",
            html.Span(" (terms only on district pages)", style={'color': '#666', 'fontSize': '14px'})
        ], style={'fontSize': '16px', 'margin': '10px 0'}),
        html.P([
            html.Strong("School-Only: "), f"{total_school_only}",
            html.Span(" (terms only on school pages)", style={'color': '#666', 'fontSize': '14px'})
        ], style={'fontSize': '16px', 'margin': '10px 0'}),
        html.P([
            html.Strong("Both: "), f"{total_both}",
            html.Span(" (terms on both school and district pages)", style={'color': '#666', 'fontSize': '14px'})
        ], style={'fontSize': '16px', 'margin': '10px 0'})
    ], style={'padding': '20px', 'backgroundColor': '#f5f5f5', 'borderRadius': '5px', 'marginBottom': '20px'})


def create_districts_list(districts_data: List[Dict]) -> html.Div:
    """Create districts list table showing districts with district-only term appearances."""
    if not districts_data:
        return html.Div("No districts with district-only term appearances found.")
    
    table_rows = []
    for idx, district in enumerate(districts_data):
        district_name = safe_extract(district, 'District Name', 'Unknown')
        district_pages = safe_extract(district, 'District Pages With Terms', 0)
        district_occurrences = safe_extract(district, 'District Total Occurrences', 0)
        district_terms = format_term_list(safe_extract(district, 'District Terms Found', ''))
        total_schools = safe_extract(district, 'Total Schools', 0)
        
        table_rows.append(html.Tr([
            html.Td(district_name),
            html.Td(f"{total_schools}"),
            html.Td(district_terms[:100] + '...' if len(district_terms) > 100 else district_terms),
            html.Td(f"{district_pages}"),
            html.Td(f"{district_occurrences}")
        ]))
    
    return html.Div([
        html.H3(f"Districts with District-Only Term Appearances ({len(districts_data)})"),
        html.P("These districts have terms appearing only on district pages, not on individual school pages.",
               style={'fontSize': '14px', 'color': '#666', 'marginBottom': '15px'}),
        html.Div([
        html.Table([
            html.Thead([
                html.Tr([
                    html.Th("District Name"),
                        html.Th("Total Schools"),
                        html.Th("Terms Found"),
                        html.Th("District Pages"),
                        html.Th("Occurrences")
                ])
            ]),
                html.Tbody(table_rows)
            ], style={
                'width': '100%',
                'border': '1px solid #ddd',
                'borderCollapse': 'collapse',
                'fontSize': '14px'
            })
        ], style={})
    ])


def create_schools_list(schools_data: List[Dict], category_name: str, description: str, category_key: str) -> html.Div:
    """Create clickable schools list table."""
    if not schools_data:
        return html.Div(f"No schools found in {category_name} category.")
    
    table_rows = []
    for idx, school in enumerate(schools_data):
        school_name = safe_extract(school, 'School Name', 'Unknown')
        district_name = safe_extract(school, 'District Name', 'Unknown')
        terms = format_term_list(safe_extract(school, 'Terms Found', ''))
        occurrences = safe_extract(school, 'Total Occurrences', 0)
        
        # Create a unique key combining category and index
        btn_id = f"{category_key}-{idx}"
        
        table_rows.append(html.Tr([
            html.Td(
                html.Button(
                    school_name,
                    id={'type': 'school-btn', 'index': btn_id},
                    style={
                        'cursor': 'pointer',
                        'color': COLOR_SCHOOL,
                        'textDecoration': 'underline',
                        'background': 'none',
                        'border': 'none',
                        'padding': '0',
                        'textAlign': 'left',
                        'fontSize': 'inherit',
                        'fontWeight': 'normal'
                    },
                    n_clicks=0
                )
            ),
            html.Td(district_name),
            html.Td(terms[:100] + '...' if len(terms) > 100 else terms),
            html.Td(f"{occurrences}")
        ]))
    
    return html.Div([
        html.H3(f"{category_name} ({len(schools_data)})"),
        html.P(description, style={'fontSize': '14px', 'color': '#666', 'marginBottom': '15px'}),
        html.Div([
            html.Table([
                html.Thead([
                    html.Tr([
                        html.Th("School Name"),
                        html.Th("District"),
                        html.Th("Terms Found"),
                        html.Th("Occurrences")
                    ])
                ]),
                html.Tbody(table_rows)
            ], style={
                'width': '100%',
                'border': '1px solid #ddd',
                'borderCollapse': 'collapse',
                'fontSize': '14px'
            })
        ], style={})
    ])


def create_school_detail_page(school_data: Dict) -> html.Div:
    """Create comprehensive school detail page with all information."""
    school_name = safe_extract(school_data, 'School Name', 'Unknown School')
    district_name = safe_extract(school_data, 'District Name', 'Unknown District')
    ai_summary = safe_extract(school_data, 'AI Summary', 'No summary available')
    
    # Extract term occurrence data
    school_pages = safe_extract(school_data, 'School Pages With Terms', 0)
    district_pages = safe_extract(school_data, 'District Pages With Terms', 0)
    school_occurrences = safe_extract(school_data, 'School Total Occurrences', 0)
    district_occurrences = safe_extract(school_data, 'District Total Occurrences', 0)
    total_occurrences = safe_extract(school_data, 'Total Occurrences', 0)
    
    # Extract terms found
    school_terms = format_term_list(safe_extract(school_data, 'School Terms Found', ''))
    district_terms = format_term_list(safe_extract(school_data, 'District Terms Found', ''))
    
    # Parse URLs
    school_urls_str = safe_extract(school_data, 'School Page URLs', '')
    district_urls_str = safe_extract(school_data, 'District Page URLs', '')
    school_urls = parse_url_list(school_urls_str)
    district_urls = parse_url_list(district_urls_str)
    
    sections = []
    
    # Header Section
    sections.append(html.Div([
        html.Button(
            '← Back to Schools',
            id='back-to-schools',
            n_clicks=0,
            style={
                'padding': '10px 20px',
                'backgroundColor': COLOR_NEUTRAL,
                'color': 'white',
                'border': 'none',
                'borderRadius': '5px',
                'cursor': 'pointer',
                'marginBottom': '20px'
            }
        ),
        html.H2(school_name, style={'marginTop': '10px', 'marginBottom': '5px'}),
        html.P([
            html.Strong("District: "), district_name
        ], style={'fontSize': '18px', 'color': '#666'})
    ]))
    
    # Statistics Cards Section
    sections.append(html.Div([
        html.H3("Term Occurrence Summary", style={'marginTop': '30px', 'marginBottom': '15px'}),
        html.Div([
            html.Div([
                html.H4(f"{school_pages}", style={'margin': '0', 'fontSize': '32px', 'color': COLOR_SCHOOL}),
                html.P("School Pages", style={'margin': '5px 0', 'color': '#666'})
            ], style={
                'flex': '1', 'textAlign': 'center', 'padding': '15px',
                'backgroundColor': COLOR_BG_SCHOOL, 'borderRadius': '5px', 'margin': '0 5px'
            }),
            html.Div([
                html.H4(f"{district_pages}", style={'margin': '0', 'fontSize': '32px', 'color': COLOR_DISTRICT}),
                html.P("District Pages", style={'margin': '5px 0', 'color': '#666'})
            ], style={
                'flex': '1', 'textAlign': 'center', 'padding': '15px',
                'backgroundColor': COLOR_BG_DISTRICT, 'borderRadius': '5px', 'margin': '0 5px'
            }),
            html.Div([
                html.H4(f"{total_occurrences}", style={'margin': '0', 'fontSize': '32px', 'color': COLOR_TOTAL}),
                html.P("Total Occurrences", style={'margin': '5px 0', 'color': '#666'})
            ], style={
                'flex': '1', 'textAlign': 'center', 'padding': '15px',
                'backgroundColor': COLOR_BG_TOTAL, 'borderRadius': '5px', 'margin': '0 5px'
            })
        ], style={'display': 'flex', 'gap': '10px', 'marginBottom': '20px'}),
        html.Div([
            html.P([
                html.Strong("School Occurrences: "), f"{school_occurrences}",
                html.Br(),
                html.Strong("District Occurrences: "), f"{district_occurrences}"
            ], style={'fontSize': '14px', 'color': '#666'})
        ])
    ]))
    
    # Terms Found Section
    sections.append(html.Div([
        html.H3("Terms Found", style={'marginTop': '30px', 'marginBottom': '15px'}),
        html.Div([
            html.Div([
                html.H4("School Pages", style={'color': COLOR_SCHOOL, 'marginBottom': '10px'}),
                html.P(school_terms, style={'padding': '10px', 'backgroundColor': COLOR_BG_SCHOOL, 'borderRadius': '5px'})
            ], style={'flex': '1', 'margin': '0 5px'}),
            html.Div([
                html.H4("District Pages", style={'color': COLOR_DISTRICT, 'marginBottom': '10px'}),
                html.P(district_terms, style={'padding': '10px', 'backgroundColor': COLOR_BG_DISTRICT, 'borderRadius': '5px'})
            ], style={'flex': '1', 'margin': '0 5px'})
        ], style={'display': 'flex', 'gap': '10px', 'marginBottom': '20px'})
    ]))
    
    # AI Summary Section
    sections.append(html.Div([
        html.H3("AI Summary", style={'marginTop': '30px', 'marginBottom': '15px'}),
        html.Div(
            ai_summary,
            style={
                'padding': '20px',
                'backgroundColor': '#f9f9f9',
                'borderRadius': '5px',
                'whiteSpace': 'pre-wrap',
                'fontSize': '14px',
                'lineHeight': '1.8',
            }
        )
    ]))
    
    # Links Section
    sections.append(html.Div([
        html.H3("Links to Pages with Terms", style={'marginTop': '30px', 'marginBottom': '15px'}),
        html.Div([
            html.Div([
                html.H4("School Page Links", style={'color': COLOR_SCHOOL, 'marginBottom': '10px'}),
                html.Ul([
                    html.Li(html.A(url, href=url, target='_blank', style={'color': COLOR_SCHOOL}))
                    for url in school_urls[:20]
                ] + ([html.Li(f"... and {len(school_urls) - 20} more")] if len(school_urls) > 20 else []),
                    style={'listStyle': 'none', 'padding': '0'})
            ], style={
                'flex': '1', 'margin': '0 5px', 'padding': '15px',
                'backgroundColor': COLOR_BG_SCHOOL, 'borderRadius': '5px'
            }),
            html.Div([
                html.H4("District Page Links", style={'color': COLOR_DISTRICT, 'marginBottom': '10px'}),
                html.Ul([
                    html.Li(html.A(url, href=url, target='_blank', style={'color': COLOR_DISTRICT}))
                    for url in district_urls[:20]
                ] + ([html.Li(f"... and {len(district_urls) - 20} more")] if len(district_urls) > 20 else []),
                    style={'listStyle': 'none', 'padding': '0'})
            ], style={
                'flex': '1', 'margin': '0 5px', 'padding': '15px',
                'backgroundColor': COLOR_BG_DISTRICT, 'borderRadius': '5px'
            })
        ], style={'display': 'flex', 'gap': '10px'})
    ]))
    
    return html.Div(sections, style={'padding': '20px', 'maxWidth': '1200px', 'margin': '0 auto'})


# ============================================================================
# STATE OVERVIEW & DISTRICT DETAIL
# ============================================================================

def _ai_summary_to_html(raw: Optional[str]) -> str:
    """Convert AI summary markdown to safe HTML for display in the iframe."""
    if not raw or not str(raw).strip():
        return ""
    text = str(raw).strip()
    try:
        import markdown
        import bleach
        html_content = markdown.markdown(
            text,
            extensions=["extra", "nl2br", "sane_lists"],
            extension_configs={"nl2br": {"enabled": True}},
        )
        allowed_tags = list(
            set(bleach.ALLOWED_TAGS)
            | {"p", "br", "h1", "h2", "h3", "h4", "ul", "ol", "li", "strong", "em", "blockquote", "pre", "code", "hr", "span", "div"}
        )
        return bleach.clean(html_content, tags=allowed_tags, strip=True)
    except Exception:
        # Fallback: escape HTML and convert basic markdown to tags
        import re
        escaped = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
        escaped = re.sub(r"\*(.+?)\*", r"<em>\1</em>", escaped)
        escaped = re.sub(r"__(.+?)__", r"<strong>\1</strong>", escaped)
        escaped = re.sub(r"_(.+?)_", r"<em>\1</em>", escaped)
        escaped = escaped.replace("\n", "<br>")
        return escaped


def _build_state_map_figure(state_aggregates: Dict, national_total: float) -> go.Figure:
    """Build US choropleth heatmap using log-scale hits for better visual differentiation."""
    all_state_codes = list(STATE_NAMES.keys())
    state_hits = [state_aggregates.get(s, {}).get("totalKeywordHits", 0) for s in all_state_codes]
    state_shares = [state_aggregates.get(s, {}).get("stateShare", 0) for s in all_state_codes]
    state_names = [STATE_NAMES.get(s, s) for s in all_state_codes]
    total_dists = [state_aggregates.get(s, {}).get("totalDistricts", 0) for s in all_state_codes]
    dists_with_kw = [state_aggregates.get(s, {}).get("districtsWithKeywords", 0) for s in all_state_codes]

    # Log1p transform: spreads out low values so small-hit states are visible
    z_log = [math.log1p(h) if (h or 0) > 0 else 0 for h in state_hits]
    max_z = max((v for v in z_log if v > 0), default=1.0)

    # Hover text with formatted numbers
    text = []
    for i, (n, h, sh, dk, td) in enumerate(zip(state_names, state_hits, state_shares, dists_with_kw, total_dists)):
        if h > 0 or td > 0:
            pct_dists = f"{100 * dk / td:.0f}%" if td else "–"
            text.append(
                f"<b>{n}</b><br>"
                f"Keyword hits: <b>{h:,}</b><br>"
                f"Share of national hits: {sh:.1%}<br>"
                f"Districts w/ keywords: {dk}/{td} ({pct_dists})"
            )
        else:
            text.append(f"<b>{n}</b><br>No data yet")

    # Colorscale: grey for 0, then warm multi-hue yellow→orange→red→crimson
    # Much better visual differentiation than a single-hue blue ramp
    grey_pos = 0.001 / max_z if max_z > 0 else 0.001
    colorscale = [
        [0,           "#e2e8f0"],   # neutral grey — no data
        [grey_pos,    "#fef9c3"],   # pale yellow — very low
        [0.12,        "#fde68a"],   # yellow
        [0.25,        "#fbbf24"],   # amber
        [0.42,        "#f97316"],   # orange
        [0.60,        "#ef4444"],   # red
        [0.78,        "#b91c1c"],   # dark red
        [0.90,        "#7f1d1d"],   # deep crimson
        [1.0,         "#450a0a"],   # near-black red — peak states
    ]

    # Colorbar ticks: show real hit values (back-transform from log)
    n_ticks = 6
    tick_log_vals = [max_z * i / (n_ticks - 1) for i in range(n_ticks)]
    tick_text = [str(int(round(math.expm1(v)))) for v in tick_log_vals]

    fig = go.Figure(data=go.Choropleth(
        locations=all_state_codes,
        z=z_log,
        locationmode="USA-states",
        text=text,
        hoverinfo="text",
        colorscale=colorscale,
        zmin=0,
        zmax=max_z,
        showscale=True,
        colorbar={
            "title": {"text": "KW Hits", "font": {"size": 11, "color": "#64748b"}},
            "tickvals": tick_log_vals,
            "ticktext": tick_text,
            "tickfont": {"size": 10, "color": "#64748b"},
            "thickness": 12,
            "len": 0.72,
            "bgcolor": "rgba(255,255,255,0.92)",
            "bordercolor": "#e2e8f0",
            "borderwidth": 1,
            "outlinewidth": 0,
        },
        marker_line_color="#ffffff",
        marker_line_width=1.2,
    ))
    fig.update_layout(
        title={
            "text": "Keyword hits by state  <span style='font-size:10px;color:#94a3b8;font-weight:400'>(log scale — click state to filter)</span>",
            "font": {"size": 12, "color": "#334155", "family": "Inter, system-ui, sans-serif"},
            "x": 0.01,
            "xanchor": "left",
        },
        geo_scope="usa",
        margin=dict(l=0, r=0, t=32, b=0),
        height=460,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_geos(
        visible=False,
        bgcolor="rgba(0,0,0,0)",
        lakecolor="#93c5fd",
        oceancolor="#dbeafe",
        landcolor="#f1f5f9",
        showocean=False,
        showlakes=True,
    )
    return fig


def _build_state_summary_panel(state_code, state_aggregates, state_districts, keyword_breakdown_list):
    if not state_code:
        return html.Div(
            "Click a state on the map or choose one from the dropdown.",
            className="empty-state empty-state--small"
        )
    agg = state_aggregates.get(state_code, {})
    total_districts = agg.get("totalDistricts", 0)
    districts_with_success = agg.get("districtsWithSuccess", 0)
    districts_with_keywords = agg.get("districtsWithKeywords", 0)
    total_hits = agg.get("totalKeywordHits", 0)
    state_name = STATE_NAMES.get(state_code, state_code)
    coverage_pct = f"{100 * districts_with_keywords / total_districts:.0f}%" if total_districts else "–"

    cards = html.Div([
        html.Div([
            html.Div(f"{total_districts:,}", className="dashboard-card__value", style={"color": "#334155"}),
            html.Div("Total Districts", className="dashboard-card__label"),
        ], className="dashboard-card"),
        html.Div([
            html.Div(f"{districts_with_success:,}", className="dashboard-card__value", style={"color": COLOR_TOTAL}),
            html.Div("Scrape Successful", className="dashboard-card__label"),
        ], className="dashboard-card"),
        html.Div([
            html.Div(f"{districts_with_keywords:,}", className="dashboard-card__value", style={"color": COLOR_DISTRICT}),
            html.Div(coverage_pct, className="dashboard-card__sub", style={"color": COLOR_DISTRICT}),
            html.Div("Districts w/ Keywords", className="dashboard-card__label"),
        ], className="dashboard-card"),
        html.Div([
            html.Div(f"{total_hits:,}", className="dashboard-card__value", style={"color": COLOR_NEUTRAL}),
            html.Div("Total Keyword Hits", className="dashboard-card__label"),
        ], className="dashboard-card"),
    ], className="dashboard-cards dashboard-cards--state")

    charts = [cards]
    if keyword_breakdown_list:
        kw_labels = [x["keyword"] for x in keyword_breakdown_list]
        raw_counts = [x["count"] for x in keyword_breakdown_list]
        raw_total = sum(raw_counts)
        if raw_total > 0 and total_hits > 0:
            scaled = [round(c * total_hits / raw_total) for c in raw_counts]
            diff = total_hits - sum(scaled)
            if diff != 0 and scaled:
                idx = 0 if diff > 0 else max(i for i, s in enumerate(scaled) if s > 0)
                scaled[idx] = scaled[idx] + diff
            kw_values = scaled
        else:
            kw_values = raw_counts
        # Color bars by intensity
        max_v = max(kw_values) if kw_values else 1
        bar_colors = [
            f"rgba(29,78,216,{max(0.3, v/max_v)})" for v in kw_values
        ]
        fig_bar = go.Figure(data=[go.Bar(
            x=kw_values, y=kw_labels, orientation="h",
            marker_color=bar_colors,
            text=kw_values, textposition="outside",
            hovertemplate="%{y}: %{x} hits<extra></extra>",
        )])
        fig_bar.update_layout(
            margin=dict(l=160, r=50, t=4, b=4),
            height=max(160, min(380, 32 * len(kw_labels))),
            bargap=0.45,
            xaxis=dict(title="", showgrid=True, gridcolor="#f1f5f9", zeroline=False,
                       tickfont=dict(size=10, color="#94a3b8")),
            yaxis=dict(autorange="reversed", tickfont=dict(size=11, color="#334155",
                       family="Inter, system-ui, sans-serif"), title=""),
            showlegend=False,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        charts.append(html.Div([
            html.P("Keyword breakdown", style={
                "margin": "16px 0 6px", "fontWeight": "700", "fontSize": "10px",
                "textTransform": "uppercase", "letterSpacing": "0.10em", "color": "#64748b",
            }),
            dcc.Graph(figure=fig_bar, config={"displayModeBar": False}),
        ], style={"width": "100%"}))

    return html.Div([
        html.Div(f"{state_name}", className="dashboard-panel-title"),
        html.Div(charts, style={"display": "flex", "flexWrap": "wrap", "gap": "12px"}),
    ], className="dashboard-panel")


def _build_district_table(state_districts):
    if not state_districts:
        return html.Div("No districts in this state.", className="empty-state empty-state--small")
    urls_with_kw = [sum(1 for u in (r.get("urls") or []) if (u.get("totalHits") or 0) > 0) for r in state_districts]
    has_hits_list = [(r.get("totalKeywordHits") or 0) > 0 for r in state_districts]
    # Ensure all values are JSON-serializable (no None id, int not numpy.int64)
    data = []
    for i, r in enumerate(state_districts):
        raw_id = r.get("id")
        data.append({
            "id": str(raw_id) if raw_id is not None else "",
            "District Name": str(r.get("districtName") or ""),
            "Number of Keywords": int(r.get("totalKeywordHits") or 0),
            "URLs with Keywords": int(urls_with_kw[i]) if i < len(urls_with_kw) else 0,
            "View": "View details" if has_hits_list[i] else "",
            "_has_hits": bool(has_hits_list[i]),
        })
    columns = [
        {"name": "District Name", "id": "District Name"},
        {"name": "Keyword hits", "id": "Number of Keywords", "type": "numeric"},
        {"name": "URLs w/ hits", "id": "URLs with Keywords", "type": "numeric"},
        {"name": "", "id": "View"},
    ]
    # Row styling: subtle highlight for clickable rows; outline selected row; "View" column styled like a link
    style_cond = []
    for col_id in ["District Name", "Number of Keywords", "URLs with Keywords", "View"]:
        style_cond.append({"if": {"filter_query": "{Number of Keywords} > 0", "column_id": col_id}, "cursor": "pointer", "backgroundColor": "rgba(247, 127, 0, 0.06)"})
        style_cond.append({"if": {"state": "active", "filter_query": "{Number of Keywords} > 0", "column_id": col_id}, "backgroundColor": "rgba(247, 127, 0, 0.12)"})
    # Outline the selected row: left edge on first column, right edge on last column
    style_cond.append({"if": {"state": "active", "filter_query": "{Number of Keywords} > 0", "column_id": "District Name"}, "borderLeft": "3px solid " + COLOR_DISTRICT})
    style_cond.append({"if": {"state": "active", "filter_query": "{Number of Keywords} > 0", "column_id": "View"}, "borderRight": "3px solid " + COLOR_DISTRICT})
    style_cond.append({"if": {"column_id": "View", "filter_query": "{View} ne ''"}, "color": COLOR_DISTRICT, "textDecoration": "underline", "fontWeight": "500"})
    return html.Div([
        html.P("Click any row to see district details and AI summary in the panel.",
               className="dashboard-table-caption"),
        html.Div([
            dash_table.DataTable(
                id="district-datatable",
                columns=columns,
                data=data,
                sort_action="native",
                filter_action="native",
                row_selectable=False,
                page_action="none",
                fixed_rows={"headers": True},
                style_table={
                    "border": "1px solid #e2e8f0",
                    "borderRadius": "12px",
                    "overflowX": "visible",
                    "boxShadow": "0 1px 3px rgba(0,0,0,0.06)",
                },
                style_cell={
                    "textAlign": "left",
                    "padding": "10px 14px",
                    "fontSize": "13px",
                    "border": "1px solid #f1f5f9",
                    "minWidth": "80px",
                    "fontFamily": "var(--font-family)",
                    "color": "#334155",
                },
                style_header={
                    "backgroundColor": "#f8fafc",
                    "fontWeight": "700",
                    "padding": "10px 14px",
                    "border": "1px solid #e2e8f0",
                    "fontSize": "11px",
                    "textTransform": "uppercase",
                    "letterSpacing": "0.06em",
                    "color": "#64748b",
                },
                style_data_conditional=style_cond,
            ),
        ], className="dashboard-table-wrap"),
    ])


def _build_national_summary(state_aggregates, national_total):
    """National-level metric cards with coverage %."""
    total_districts = sum((a.get("totalDistricts") or 0) for a in (state_aggregates or {}).values())
    districts_with_success = sum((a.get("districtsWithSuccess") or 0) for a in (state_aggregates or {}).values())
    districts_with_keywords = sum((a.get("districtsWithKeywords") or 0) for a in (state_aggregates or {}).values())
    total_hits = national_total or 0
    scrape_pct = f"{100 * districts_with_success / total_districts:.0f}%" if total_districts else "–"
    kw_pct = f"{100 * districts_with_keywords / total_districts:.0f}%" if total_districts else "–"
    return html.Div([
        html.Div("National Overview", className="dashboard-panel-title"),
        html.Div([
            html.Div([
                html.Div(f"{total_districts:,}", className="dashboard-card__value", style={"color": "#334155"}),
                html.Div("Total Districts", className="dashboard-card__label"),
            ], className="dashboard-card"),
            html.Div([
                html.Div(f"{districts_with_success:,}", className="dashboard-card__value", style={"color": COLOR_TOTAL}),
                html.Div(scrape_pct, className="dashboard-card__sub", style={"color": COLOR_TOTAL}),
                html.Div("Scrape Successful", className="dashboard-card__label"),
            ], className="dashboard-card"),
            html.Div([
                html.Div(f"{districts_with_keywords:,}", className="dashboard-card__value", style={"color": COLOR_DISTRICT}),
                html.Div(kw_pct, className="dashboard-card__sub", style={"color": COLOR_DISTRICT}),
                html.Div("Districts w/ Keywords", className="dashboard-card__label"),
            ], className="dashboard-card"),
            html.Div([
                html.Div(f"{total_hits:,}", className="dashboard-card__value", style={"color": COLOR_NEUTRAL}),
                html.Div("Total Keyword Hits", className="dashboard-card__label"),
            ], className="dashboard-card"),
        ], className="dashboard-cards"),
    ], className="dashboard-summary-block")


def _layout_state_overview(state_aggregates, national_total):
    fig = _build_state_map_figure(state_aggregates, national_total)
    national_summary_block = _build_national_summary(state_aggregates, national_total)
    state_codes = sorted(state_aggregates.keys()) if state_aggregates else []
    state_dropdown_options = [{"label": "Select a state...", "value": ""}] + [
        {"label": f"{STATE_NAMES.get(c, c)} ({c})", "value": c} for c in state_codes
    ]
    _table_placeholder = html.Div(
        "Select a state from the dropdown (or click the map) to see districts below.",
        className="empty-state",
    )
    left_column = html.Div([
        html.Div([
            html.H1("School Policy Term Dashboard"),
            html.Span("National Overview", className="breadcrumb"),
        ], className="dashboard-header"),
        national_summary_block,
        # State selector row
        html.Div([
            html.Label("Filter by state", style={"fontWeight": "600", "fontSize": "12px",
                                                  "textTransform": "uppercase", "letterSpacing": "0.06em",
                                                  "color": "#334155", "whiteSpace": "nowrap"}),
            dcc.Dropdown(
                id="state-dropdown",
                options=state_dropdown_options,
                value=None,
                clearable=True,
                placeholder="Select a state  —  or click the map",
                style={"flex": "1", "minWidth": "200px", "fontSize": "14px"},
            ),
        ], className="state-select-row"),
        # Map + state summary side by side
        html.Div([
            html.Div([dcc.Graph(id="state-map", figure=fig, config={"displayModeBar": False})],
                     className="dashboard-map-wrap"),
            html.Div(
                id="state-summary-panel",
                children=html.Div("Select a state to see its summary.", className="empty-state empty-state--small"),
                style={"flex": "1", "minWidth": "260px"},
            ),
        ], className="map-and-summary-row"),
        # District table section
        html.Div([
            html.Div([
                html.H3(id="districts-heading", children="Districts",
                        style={"margin": "0", "fontSize": "1rem", "fontWeight": "700",
                               "color": "#0f172a", "letterSpacing": "-0.01em"}),
                html.Span(id="districts-count",
                          style={"fontSize": "12px", "color": "#64748b", "marginLeft": "8px"}),
            ], style={"display": "flex", "alignItems": "baseline", "marginBottom": "8px"}),
            html.Div(
                id="district-table-container",
                children=_table_placeholder,
                style={"minHeight": "320px", "flex": "1"},
            ),
        ], style={"flex": "1 1 400px", "display": "flex", "flexDirection": "column", "minHeight": "360px"}),
        dcc.Store(id="selected-state", data=None),
        dcc.Store(id="selected-district-id", data=None),
    ], style={"flex": "1", "minWidth": "400px", "display": "flex", "flexDirection": "column"})

    right_panel = html.Div(
        id="district-detail-panel",
        children=html.Div([
            html.Div("📋", style={"fontSize": "2rem", "marginBottom": "12px"}),
            html.P(["Select a district from the table to see details, keyword breakdown, and AI summary."],
                   style={"margin": "0", "color": "#64748b", "lineHeight": "1.6"}),
        ], className="empty-state empty-state--small", style={"flexDirection": "column"}),
        className="dashboard-side-panel",
    )
    return html.Div([
        html.Div([left_column, right_panel],
                 style={"display": "flex", "flexWrap": "wrap", "gap": "24px", "alignItems": "flex-start"}),
    ], className="dashboard-main")


def _district_detail_content(record, show_back_link=True):
    """Shared district detail body. Used by full page and side panel."""
    name = record.get("districtName") or "Unknown"
    state = record.get("state") or ""
    state_full = STATE_NAMES.get(state, state)
    total_hits = record.get("totalKeywordHits") or 0
    urls_list = record.get("urls") or []
    urls_with_hits = [u for u in urls_list if (u.get("totalHits") or 0) > 0]
    urls_scanned = len(urls_list)
    keyword_counts = record.get("keywordCounts") or {}
    terms_with_count = [{"keyword": k, "count": v} for k, v in keyword_counts.items()
                        if v and (int(v) if isinstance(v, (int, float)) else 1) > 0]
    terms_with_count.sort(key=lambda x: -(x.get("count") or 0))
    ai_raw = record.get("aiSummary")
    ai_html = _ai_summary_to_html(ai_raw) if ai_raw else ""
    sections = []

    # Header
    sections.append(html.Div([
        html.H2(name, style={"margin": "0 0 8px", "fontSize": "1.3rem", "fontWeight": "800",
                              "color": "#0f172a", "letterSpacing": "-0.02em", "lineHeight": "1.2"}),
        html.Span(state_full, style={
            "display": "inline-flex", "alignItems": "center", "padding": "3px 10px",
            "background": "#eff6ff", "borderRadius": "9999px", "fontSize": "12px",
            "fontWeight": "600", "color": "#1d4ed8",
        }),
    ], style={"paddingBottom": "16px", "borderBottom": "2px solid #e2e8f0", "marginBottom": "16px"}))

    # Metric mini-cards
    sections.append(html.Div([
        html.Div("Metrics", className="section-title"),
        html.Div([
            html.Div([
                html.Div(str(len(urls_with_hits)), style={"fontSize": "22px", "fontWeight": "800", "color": COLOR_DISTRICT}),
                html.Div("URLs w/ keywords", style={"fontSize": "11px", "color": "#64748b", "marginTop": "3px",
                                                     "textTransform": "uppercase", "letterSpacing": "0.05em"}),
            ], style={"flex": "1", "textAlign": "center", "padding": "12px 8px",
                      "background": "#fffbeb", "borderRadius": "10px", "border": "1px solid #fde68a"}),
            html.Div([
                html.Div(str(urls_scanned), style={"fontSize": "22px", "fontWeight": "800", "color": "#64748b"}),
                html.Div("URLs scanned", style={"fontSize": "11px", "color": "#64748b", "marginTop": "3px",
                                                 "textTransform": "uppercase", "letterSpacing": "0.05em"}),
            ], style={"flex": "1", "textAlign": "center", "padding": "12px 8px",
                      "background": "#f8fafc", "borderRadius": "10px", "border": "1px solid #e2e8f0"}),
            html.Div([
                html.Div(str(total_hits), style={"fontSize": "22px", "fontWeight": "800", "color": COLOR_TOTAL}),
                html.Div("Total hits", style={"fontSize": "11px", "color": "#64748b", "marginTop": "3px",
                                               "textTransform": "uppercase", "letterSpacing": "0.05em"}),
            ], style={"flex": "1", "textAlign": "center", "padding": "12px 8px",
                      "background": "#ecfdf5", "borderRadius": "10px", "border": "1px solid #a7f3d0"}),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "4px"}),
    ]))

    # Keyword chips
    if terms_with_count:
        chips = [
            html.Span([
                t["keyword"],
                html.Span(f" ×{t['count']}", className="keyword-chip__count"),
            ], className="keyword-chip")
            for t in terms_with_count
        ]
        sections.append(html.Div([
            html.Div("Keywords found", className="section-title"),
            html.Div(chips, className="keyword-chips"),
        ]))

    # AI Summary
    if ai_html:
        sections.append(html.Div([
            html.Div("AI Summary", className="section-title"),
            html.Div(
                dcc.Markdown(ai_html, dangerously_allow_html=True),
                className="dashboard-ai-summary-box",
            ),
        ]))
    elif ai_raw:
        sections.append(html.Div([
            html.Div("AI Summary", className="section-title"),
            html.Div(str(ai_raw), className="dashboard-ai-summary-box"),
        ]))
    else:
        sections.append(html.Div([
            html.Div("AI Summary", className="section-title"),
            html.Div("No AI summary available for this district.",
                     className="empty-state empty-state--small"),
        ]))

    # Source links
    if urls_with_hits:
        cap = 50
        shown = urls_with_hits[:cap]
        link_items = [html.Li(html.A(u.get("url", ""), href=u.get("url", "#"),
                                     target="_blank")) for u in shown]
        link_children = [html.Div("Pages with keywords", className="section-title")]
        if len(urls_with_hits) > cap:
            link_children.append(html.P(f"Showing first {cap} of {len(urls_with_hits)} links.",
                                        style={"fontSize": "12px", "color": "#64748b", "marginBottom": "8px"}))
        link_children.append(html.Ul(link_items, className="dashboard-links-list"))
        sections.append(html.Div(link_children))

    ai_engine_sources = record.get("aiEngineSources") or []
    if ai_engine_sources:
        ai_source_items = [html.Li(html.A(url, href=url, target="_blank")) for url in ai_engine_sources]
        sections.append(html.Div([
            html.Div("AI Research Sources", className="section-title"),
            html.Ul(ai_source_items, className="dashboard-links-list"),
        ]))
    return sections


def _layout_district_detail(record):
    state_code = (record.get("state") or "").strip().upper()[:2]
    state_name = STATE_NAMES.get(state_code, state_code)
    district_name = record.get("districtName") or "District"
    header = html.Div([
        html.H1("School Policy Term Dashboard"),
        html.Span([
            dcc.Link("State Overview", href="/", className="dashboard-link", style={"color": "rgba(255,255,255,0.7)"}),
            html.Span(" › ", style={"color": "rgba(255,255,255,0.4)", "margin": "0 4px"}),
            html.Span(state_name, style={"color": "rgba(255,255,255,0.7)"}),
            html.Span(" › ", style={"color": "rgba(255,255,255,0.4)", "margin": "0 4px"}),
            html.Span(district_name, style={"color": "#fff", "fontWeight": "600"}),
        ], className="breadcrumb"),
    ], className="dashboard-header")
    sections = _district_detail_content(record, show_back_link=True)
    return html.Div([header] + sections, className="dashboard-page")


def create_app() -> dash.Dash:
    """Create and configure the Dash application."""
    app = dash.Dash(__name__, suppress_callback_exceptions=True)
    
    app.server = app.server
    
    csv_rows = load_all_states_data()
    district_records = build_district_records(csv_rows)
    state_aggregates = aggregate_by_state(district_records)
    national_total = sum(r.get("totalKeywordHits") or 0 for r in district_records)
    
    # Legacy Florida data (for existing tabs)
    all_florida_data = load_all_florida_data()
    
    # Filter for schools with AI summaries
    schools_with_ai = get_schools_with_ai_summaries(all_florida_data)
    
    # Categorize schools by where terms appear
    district_only, school_only, both = categorize_schools_by_term_location(schools_with_ai)
    
    # Routing: pathname -> page-content (dashboard-root loads assets/dashboard.css)
    app.layout = html.Div([
        dcc.Location(id="url", refresh=False),
        dcc.Loading(
            id="page-loading",
            type="default",
            children=html.Div(id="page-content", className="dashboard-main"),
        ),
        dcc.Store(id="district-records-store", data=district_records),
        dcc.Store(id="state-aggregates-store", data=state_aggregates),
        dcc.Store(id="national-total-store", data=national_total),
    ], className="dashboard-root")
    
    # ========================================================================
    # CALLBACKS
    # ========================================================================

    @app.callback(
        Output('page-content', 'children'),
        Input('url', 'pathname'),
        State('district-records-store', 'data'),
        State('state-aggregates-store', 'data'),
        State('national-total-store', 'data'),
    )
    def render_page(pathname, records, state_agg, national_tot):
        if pathname is None:
            pathname = '/'
        records = records or []
        state_agg = state_agg or {}
        national_tot = national_tot or 0
        try:
            if pathname and pathname.startswith('/dashboard/district/'):
                district_id = pathname.replace('/dashboard/district/', '').strip('/')
                record = get_district_by_id(district_id, records)
                if record:
                    return _layout_district_detail(record)
                return html.Div('District not found.', className='empty-state empty-state--small')
            return _layout_state_overview(state_agg, national_tot)
        except Exception as e:
            logger.exception("render_page failed for pathname=%s", pathname)
            return html.Div([
                html.H1("School Policy Term Dashboard"),
                html.P("Something went wrong loading this page.", style={"color": "#c00", "marginTop": "16px"}),
                html.Pre(str(e), style={"fontSize": "12px", "marginTop": "8px", "whiteSpace": "pre-wrap"}),
            ], className="dashboard-main", style={"padding": "20px"})

    _table_placeholder = html.Div(
        "Select a state from the dropdown (or click the map) to see districts. Click a row or 'View' to open details and AI summary.",
        className="empty-state",
    )

    @app.callback(
        [
            Output('selected-state', 'data'),
            Output('state-summary-panel', 'children'),
            Output('district-table-container', 'children'),
            Output('districts-heading', 'children'),
            Output('districts-count', 'children'),
        ],
        Input('state-dropdown', 'value'),
        [State('district-records-store', 'data'), State('state-aggregates-store', 'data'), State('selected-state', 'data')],
    )
    def on_state_select(state_code, records, state_agg, selected_state):
        """Update summary and table when state is selected from dropdown (single reliable path)."""
        records = records or []
        state_agg = state_agg or {}
        empty_summary = html.Div("Select a state above or on the map.", className="empty-state empty-state--small")
        # Normalize: dropdown can send None, "", or (rarely) a list
        if isinstance(state_code, list):
            state_code = state_code[0] if state_code else None
        if state_code is None or not str(state_code).strip():
            # Spurious null from re-render: keep current selection and don't clear UI
            if selected_state and str(selected_state).strip():
                return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
            return None, empty_summary, _table_placeholder, "Districts", ""
        state_code = str(state_code).strip().upper()[:2]
        store_output = state_code  # store always updated from dropdown; no sync callback (was causing cycle)
        try:
            state_districts = get_state_districts(state_code, records)
            kw_breakdown = keyword_breakdown(state_districts)
            summary = _build_state_summary_panel(state_code, state_agg, state_districts, kw_breakdown)
            districts_with_hits = [r for r in state_districts if (r.get("totalKeywordHits") or 0) > 0]
            state_name = STATE_NAMES.get(state_code, state_code)
            heading = f"Districts in {state_name}"
            if not districts_with_hits:
                table = html.Div("No districts with keyword hits in this state.", className="empty-state empty-state--small")
                count_text = f"Showing {len(state_districts)} districts (none with keyword hits)"
            else:
                table = _build_district_table(districts_with_hits)
                count_text = f"Showing {len(districts_with_hits)} districts"
            return store_output, summary, table, heading, count_text
        except Exception as e:
            logger.exception("on_state_select failed for state_code=%s", state_code)
            err_msg = html.Div([
                html.P("Could not load state summary.", style={"color": "#c00", "marginBottom": "8px"}),
                html.P(str(e), style={"fontSize": "12px", "color": "#666"}),
            ], style={"padding": "16px", "border": "1px solid #fcc", "borderRadius": "8px", "backgroundColor": "#fff5f5"})
            return store_output, err_msg, _table_placeholder, "Districts", ""

    # Removed sync_dropdown_to_store: it created a static cycle (selected-state -> state-dropdown -> selected-state).
    # Dropdown is set by user selection and by on_map_click_sync_dropdown; store is set by on_state_select only.

    @app.callback(
        Output('state-dropdown', 'value', allow_duplicate=True),
        Input('state-map', 'clickData'),
        prevent_initial_call=True,
    )
    def on_map_click_sync_dropdown(click_data):
        """Sync dropdown to map click so selecting on map updates dropdown (and thus summary + table)."""
        if not click_data or not click_data.get('points'):
            return dash.no_update
        pt = click_data['points'][0]
        raw_location = pt.get('location')
        if raw_location is None or not str(raw_location).strip():
            return dash.no_update
        return str(raw_location).strip().upper()[:2]

    def _build_district_panel_children(record, selected_state_code, district_id):
        """Build the right-panel content for a selected district."""
        state_name = STATE_NAMES.get(selected_state_code or "", selected_state_code or "State")
        district_name = record.get("districtName") or "District"
        close_btn = html.Button(
            "✕ Close",
            id="close-district-panel",
            className="dashboard-btn dashboard-btn--neutral",
            style={"fontSize": "12px", "padding": "5px 12px"},
        )
        open_full_link = dcc.Link(
            "Open full page ↗",
            href=f"/dashboard/district/{district_id}",
            target="_blank",
            style={"fontSize": "13px", "color": COLOR_DISTRICT, "fontWeight": "600",
                   "textDecoration": "none", "marginLeft": "auto"},
        )
        action_bar = html.Div(
            [close_btn, open_full_link],
            className="panel-action-bar",
        )
        sections = _district_detail_content(record, show_back_link=False)
        return html.Div([action_bar] + sections)

    _placeholder_panel = html.Div([
        html.Div("📋", style={"fontSize": "2rem", "marginBottom": "12px"}),
        html.P(
            "Select a district from the table to view details, keyword breakdown, and AI summary.",
            style={"margin": "0", "color": "#64748b", "lineHeight": "1.6"},
        ),
    ], className="empty-state empty-state--small", style={"flexDirection": "column"})

    @app.callback(
        [Output('selected-district-id', 'data'), Output('district-detail-panel', 'children', allow_duplicate=True)],
        Input('district-datatable', 'active_cell'),
        [
            State('district-datatable', 'derived_viewport_indices'),
            State('district-datatable', 'data'),
            State('district-records-store', 'data'),
            State('selected-state', 'data'),
        ],
        prevent_initial_call=True,
    )
    def on_table_click(active_cell, viewport_indices, table_data, records, selected_state_code):
        # active_cell.row is the index in the *displayed* (sorted/filtered) table.
        # derived_viewport_indices[i] = original data index of the row at display position i.
        # Use it so the clicked row always resolves to the correct district after sort/filter.
        if not active_cell or not table_data or not records:
            return dash.no_update, dash.no_update
        row_idx = active_cell.get('row')
        if row_idx is None:
            return dash.no_update, dash.no_update
        # Resolve display index to original data row
        if viewport_indices and 0 <= row_idx < len(viewport_indices):
            original_idx = viewport_indices[row_idx]
        else:
            original_idx = row_idx
        if original_idx < 0 or original_idx >= len(table_data):
            return dash.no_update, dash.no_update
        row = table_data[original_idx]
        if not row.get('_has_hits'):
            return dash.no_update, dash.no_update
        district_id = row.get('id')
        if not district_id:
            return dash.no_update, dash.no_update
        record = get_district_by_id(district_id, records or [])
        if not record:
            return district_id, html.Div("District not found.", className="empty-state empty-state--small")
        panel_content = _build_district_panel_children(record, selected_state_code, district_id)
        return district_id, panel_content

    @app.callback(
        Output('district-detail-panel', 'children'),
        Input('selected-district-id', 'data'),
        [State('district-records-store', 'data'), State('selected-state', 'data')],
        prevent_initial_call=True,
    )
    def fill_district_panel_from_store(selected_district_id, records, selected_state_code):
        """Primary panel updater: runs when selected-district-id changes. Initial placeholder is in layout."""
        records = records or []
        if not selected_district_id:
            return _placeholder_panel
        record = get_district_by_id(selected_district_id, records)
        if not record:
            return html.Div("District not found.", className="empty-state empty-state--small")
        return _build_district_panel_children(record, selected_state_code, selected_district_id)

    @app.callback(
        [Output('selected-district-id', 'data', allow_duplicate=True), Output('district-detail-panel', 'children', allow_duplicate=True)],
        Input('close-district-panel', 'n_clicks'),
        prevent_initial_call=True,
    )
    def close_district_panel(n_clicks):
        if n_clicks:
            return None, _placeholder_panel
        return dash.no_update, dash.no_update

    return app


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def run_app(host: str = '127.0.0.1', port: int = 8051, debug: bool = True):
    """Run the Dash application."""
    app = create_app()
    logger.info(f"Starting Florida schools app on http://{host}:{port}")
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    import sys
    from utils.logging_config import setup_logging
    
    setup_logging()
    run_app()
