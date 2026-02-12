"""Minimal Florida dashboard for school policy term analysis."""
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, List
import dash
from dash import dcc, html, Input, Output, State
from dash.dependencies import ALL, MATCH
from config import RESULTS_CSV, OUTPUT_DIR

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
        ], style={'maxHeight': '600px', 'overflowY': 'auto'})
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
        ], style={'maxHeight': '600px', 'overflowY': 'auto'})
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
                'maxHeight': '600px',
                'overflowY': 'auto'
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
# APP SETUP
# ============================================================================

def create_app() -> dash.Dash:
    """Create and configure the Dash application."""
    app = dash.Dash(__name__, suppress_callback_exceptions=True)
    
    # For Render deployment, expose the server
    app.server = app.server
    
    # Load all Florida data
    all_florida_data = load_all_florida_data()
    
    # Filter for schools with AI summaries
    schools_with_ai = get_schools_with_ai_summaries(all_florida_data)
    
    # Categorize schools by where terms appear
    district_only, school_only, both = categorize_schools_by_term_location(schools_with_ai)
    
    # App layout
    app.layout = html.Div([
        html.H1("Florida Schools: Restorative Justice Analysis",
                style={'textAlign': 'center', 'marginBottom': '30px'}),
        
        html.Div(id='summary-stats'),
        
        html.Div([
            dcc.Tabs(id='main-tabs', value='district-only-tab', children=[
                dcc.Tab(label='District-Only', value='district-only-tab'),
                dcc.Tab(label='School-Only', value='school-only-tab'),
                dcc.Tab(label='Both', value='both-tab')
            ])
        ]),
        
        html.Div(id='tab-content', style={'marginTop': '20px'}),
        
        html.Div(id='school-detail', style={'display': 'none'}),
        
        # Hidden back button (needs to exist in layout for callback)
        html.Button(id='back-to-schools', n_clicks=0, style={'display': 'none'}),
        
        dcc.Store(id='district-only-store', data=district_only),
        dcc.Store(id='school-only-store', data=school_only),
        dcc.Store(id='both-store', data=both),
        dcc.Store(id='all-schools-store', data=schools_with_ai)
    ], style={'padding': '20px'})
    
    # ========================================================================
    # CALLBACKS
    # ========================================================================
    
    @app.callback(
        Output('summary-stats', 'children'),
        [Input('district-only-store', 'data'),
         Input('school-only-store', 'data'),
         Input('both-store', 'data')]
    )
    def update_summary_stats(district_only, school_only, both):
        """Update summary statistics."""
        if not district_only:
            district_only = []
        if not school_only:
            school_only = []
        if not both:
            both = []
        
        stats = create_summary_stats(district_only, school_only, both)
        return stats
    
    @app.callback(
        Output('tab-content', 'children'),
        [Input('main-tabs', 'value')],
        [State('district-only-store', 'data'),
         State('school-only-store', 'data'),
         State('both-store', 'data')]
    )
    def update_tab_content(active_tab, district_only, school_only, both):
        """Update tab content based on selection."""
        if not district_only:
            district_only = []
        if not school_only:
            school_only = []
        if not both:
            both = []
        
        if active_tab == 'district-only-tab':
            return create_schools_list(
                district_only,
                "District-Only",
                "Schools where terms appear only on district pages, not on school pages.",
                "district-only"
            )
        elif active_tab == 'school-only-tab':
            return create_schools_list(
                school_only,
                "School-Only",
                "Schools where terms appear only on school pages, not on district pages.",
                "school-only"
            )
        elif active_tab == 'both-tab':
            return create_schools_list(
                both,
                "Both",
                "Schools where terms appear on both school pages and district pages.",
                "both"
            )
        else:
            return html.Div("Select a tab")
    
    @app.callback(
        [Output('school-detail', 'children'),
         Output('school-detail', 'style'),
         Output('tab-content', 'style', allow_duplicate=True)],
        [Input({'type': 'school-btn', 'index': ALL}, 'n_clicks')],
        [State('district-only-store', 'data'),
         State('school-only-store', 'data'),
         State('both-store', 'data'),
         State({'type': 'school-btn', 'index': ALL}, 'id')],
        prevent_initial_call=True
    )
    def handle_school_click(n_clicks_list, district_only, school_only, both, button_ids):
        """Handle school button click and display detail page."""
        ctx = dash.callback_context
        if not ctx.triggered:
            return html.Div(), {'display': 'none'}, dash.no_update
        
        # Get the triggered component
        triggered = ctx.triggered[0]
        triggered_prop_id = triggered['prop_id']
        
        # Parse the triggered ID - format: '{"type":"school-btn","index":"district-only-0"}.n_clicks'
        try:
            import json
            if '.n_clicks' in triggered_prop_id:
                json_str = triggered_prop_id.split('.n_clicks')[0]
                button_id = json.loads(json_str)
                btn_index_str = button_id.get('index', '')
                
                # Parse the index string: "category-index"
                if '-' in btn_index_str:
                    category_key, idx_str = btn_index_str.rsplit('-', 1)
                    school_index = int(idx_str)
                else:
                    logger.error(f"Invalid button index format: {btn_index_str}")
                    return html.Div(), {'display': 'none'}, dash.no_update
            else:
                # Fallback: find from button_ids
                if not button_ids or not n_clicks_list:
                    return html.Div(), {'display': 'none'}, dash.no_update
                
                clicked_idx = None
                for i, clicks in enumerate(n_clicks_list):
                    if clicks and clicks > 0:
                        clicked_idx = i
                        break
                
                if clicked_idx is None or clicked_idx >= len(button_ids):
                    return html.Div(), {'display': 'none'}, dash.no_update
                
                button_id = button_ids[clicked_idx]
                btn_index_str = button_id.get('index', '')
                
                if '-' in btn_index_str:
                    category_key, idx_str = btn_index_str.rsplit('-', 1)
                    school_index = int(idx_str)
                else:
                    return html.Div(), {'display': 'none'}, dash.no_update
        except (json.JSONDecodeError, KeyError, IndexError, AttributeError, ValueError) as e:
            logger.error(f"Error parsing button ID: {triggered_prop_id}, {e}")
            return html.Div(), {'display': 'none'}, dash.no_update
        
        # Get the appropriate data store based on category key
        if category_key == 'district-only':
            schools_data = district_only or []
        elif category_key == 'school-only':
            schools_data = school_only or []
        elif category_key == 'both':
            schools_data = both or []
        else:
            logger.warning(f"Unknown category key: {category_key}")
            return html.Div(), {'display': 'none'}, dash.no_update
        
        if school_index >= len(schools_data):
            logger.warning(f"School index {school_index} out of range for {category_key} (len={len(schools_data)})")
            return html.Div(), {'display': 'none'}, dash.no_update
        
        # Get school data
        school_data = schools_data[school_index]
        
        # Create detail page
        detail_page = create_school_detail_page(school_data)
        
        # Show detail, hide tab content
        return detail_page, {'display': 'block'}, {'display': 'none'}
    
    @app.callback(
        [Output('school-detail', 'style', allow_duplicate=True),
         Output('tab-content', 'style', allow_duplicate=True)],
        [Input('back-to-schools', 'n_clicks')],
        prevent_initial_call=True
    )
    def handle_back_button(n_clicks):
        """Handle back button click to return to tab content."""
        if n_clicks and n_clicks > 0:
            return {'display': 'none'}, {'display': 'block'}
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
