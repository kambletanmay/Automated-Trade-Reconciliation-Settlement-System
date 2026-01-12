import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date, timedelta
from typing import Dict, List

# Configuration
API_BASE_URL = "http://localhost:8000/api"

st.set_page_config(
    page_title="Trade Reconciliation Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .critical { color: #d32f2f; font-weight: bold; }
    .high { color: #f57c00; font-weight: bold; }
    .medium { color: #fbc02d; font-weight: bold; }
    .low { color: #388e3c; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# Helper functions
def fetch_data(endpoint: str, params: dict = None) -> dict:
    """Fetch data from API"""
    try:
        response = requests.get(f"{API_BASE_URL}/{endpoint}", params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return {}

def post_data(endpoint: str, data: dict) -> dict:
    """Post data to API"""
    try:
        response = requests.post(f"{API_BASE_URL}/{endpoint}", json=data)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error posting data: {e}")
        return {}

# Sidebar
with st.sidebar:
    st.title("📊 Reconciliation System")
    
    page = st.radio(
        "Navigation",
        ["Dashboard", "Reconciliation Runs", "Breaks Management", "Reports", "Settings"]
    )
    
    st.divider()
    
    # Date selector
    selected_date = st.date_input(
        "Select Date",
        value=date.today() - timedelta(days=1)
    )
    
    st.divider()
    
    # Quick stats
    st.subheader("Quick Stats")
    summary = fetch_data("breaks/dashboard/summary")
    
    if summary:
        st.metric("Open Breaks", summary.get('by_status', {}).get('open', 0))
        st.metric("Critical Breaks", summary.get('by_severity', {}).get('critical', 0))
        st.metric("SLA Breaches", summary.get('sla', {}).get('breached', 0))

# Main content
if page == "Dashboard":
    st.title("📈 Reconciliation Dashboard")
    
    # Fetch dashboard summary
    summary = fetch_data("breaks/dashboard/summary")
    
    # Top metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Open Breaks",
            summary.get('by_status', {}).get('open', 0) + summary.get('by_status', {}).get('assigned', 0),
            delta=None
        )
    
    with col2:
        st.metric(
            "Critical Breaks",
            summary.get('by_severity', {}).get('critical', 0),
            delta=None
        )
    
    with col3:
        breach_rate = summary.get('sla', {}).get('breach_rate', 0) * 100
        st.metric(
            "SLA Breach Rate",
            f"{breach_rate:.1f}%",
            delta=f"{breach_rate - 5:.1f}%",
            delta_color="inverse"
        )
    
    with col4:
        resolved = summary.get('by_status', {}).get('resolved', 0)
        st.metric(
            "Resolved Today",
            resolved,
            delta=None
        )
    
    # Charts row 1
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Breaks by Severity")
        severity_data = summary.get('by_severity', {})
        
        if severity_data:
            fig = go.Figure(data=[go.Pie(
                labels=list(severity_data.keys()),
                values=list(severity_data.values()),
                marker=dict(colors=['#d32f2f', '#f57c00', '#fbc02d', '#388e3c'])
            )])
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Breaks by Status")
        status_data = summary.get('by_status', {})
        
        if status_data:
            fig = px.bar(
                x=list(status_data.keys()),
                y=list(status_data.values()),
                labels={'x': 'Status', 'y': 'Count'}
            )
            fig.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    # Aging analysis
    st.subheader("Break Aging Analysis")
    aging_data = summary.get('aging', {})
    
    if aging_data:
        df_aging = pd.DataFrame({
            'Age Range': list(aging_data.keys()),
            'Count': list(aging_data.values())
        })
        
        fig = px.bar(
            df_aging,
            x='Age Range',
            y='Count',
            color='Count',
            color_continuous_scale='Reds'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Recent breaks table
    st.subheader("Recent High-Priority Breaks")
    breaks = fetch_data("breaks", params={
        'severity': 'CRITICAL',
        'status': 'OPEN',
        'limit': 10
    })
    
    if breaks:
        df_breaks = pd.DataFrame(breaks)
        st.dataframe(
            df_breaks[['id', 'break_type', 'severity', 'status', 'created_at']],
            use_container_width=True
        )

elif page == "Reconciliation Runs":
    st.title("🔄 Reconciliation Runs")
    
    # Run new reconciliation
    with st.expander("▶️ Trigger New Reconciliation"):
        col1, col2 = st.columns([3, 1])
        
        with col1:
            run_date = st.date_input("Trade Date", value=date.today() - timedelta(days=1))
            force_rerun = st.checkbox("Force Rerun")
        
        with col2:
            if st.button("Run Reconciliation", type="primary"):
                with st.spinner("Running reconciliation..."):
                    result = post_data("reconciliation/run", {
                        'trade_date': str(run_date),
                        'force_rerun': force_rerun
                    })
                    
                    if result.get('status') == 'COMPLETED':
                        st.success("Reconciliation completed successfully!")
                    else:
                        st.error(f"Reconciliation failed: {result.get('error')}")
    
    # Recent runs
    st.subheader("Recent Reconciliation Runs")
    
    runs = fetch_data("reconciliation/runs", params={'limit': 20})
    
    if runs:
        df_runs = pd.DataFrame(runs)
        
        # Format columns
        df_runs['match_rate'] = (df_runs['matched_trades'] / 
                                  (df_runs['total_internal_trades'] + df_runs['total_external_trades']) * 100).round(2)
        
        # Display with formatting
        st.dataframe(
            df_runs[[
                'id', 'trade_date', 'status', 'total_internal_trades',
                'total_external_trades', 'matched_trades', 'match_rate',
                'new_breaks', 'auto_resolved_breaks', 'duration_seconds'
            ]].rename(columns={
                'id': 'Run ID',
                'trade_date': 'Trade Date',
                'status': 'Status',
                'total_internal_trades': 'Internal',
                'total_external_trades': 'External',
                'matched_trades': 'Matched',
                'match_rate': 'Match %',
                'new_breaks': 'Breaks',
                'auto_resolved_breaks': 'Auto-Resolved',
                'duration_seconds': 'Duration (s)'
            }),
            use_container_width=True
        )
        
        # Trend chart
        st.subheader("Match Rate Trend")
        fig = px.line(
            df_runs.sort_values('trade_date'),
            x='trade_date',
            y='match_rate',
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)

elif page == "Breaks Management":
    st.title("🔧 Breaks Management")
    
    # Filters
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        filter_status = st.selectbox("Status", ["All", "OPEN", "ASSIGNED", "RESOLVED"])
    with col2:
        filter_severity = st.selectbox("Severity", ["All", "CRITICAL", "HIGH", "MEDIUM", "LOW"])
    with col3:
        filter_type = st.text_input("Break Type")
    with col4:
        filter_assignee = st.text_input("Assigned To")
    
    # Build params
    params = {'limit': 100}
    if filter_status != "All":
        params['status'] = filter_status
    if filter_severity != "All":
        params['severity'] = filter_severity
    if filter_type:
        params['break_type'] = filter_type
    if filter_assignee:
        params['assigned_to'] = filter_assignee
    
    # Fetch breaks
    breaks = fetch_data("breaks", params=params)
    
    if breaks:
        df_breaks = pd.DataFrame(breaks)
        
        # Display table
        st.dataframe(
            df_breaks[[
                'id', 'break_type', 'severity', 'status',
                'assigned_to', 'created_at'
            ]],
            use_container_width=True
        )
        
        # Break details and actions
        st.subheader("Break Details & Actions")
        
        selected_break_id = st.selectbox(
            "Select Break ID",
            options=df_breaks['id'].tolist()
        )
        
        if selected_break_id:
            break_detail = df_breaks[df_breaks['id'] == selected_break_id].iloc[0]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Break Information:**")
                st.write(f"ID: {break_detail['id']}")
                st.write(f"Type: {break_detail['break_type']}")
                st.write(f"Severity: {break_detail['severity']}")
                st.write(f"Status: {break_detail['status']}")
                st.write(f"Expected: {break_detail.get('expected_value', 'N/A')}")
                st.write(f"Actual: {break_detail.get('actual_value', 'N/A')}")
                st.write(f"Difference: {break_detail.get('difference', 'N/A')}")
            
            with col2:
                st.write("**Actions:**")
                
                # Assign
                assignee = st.text_input("Assign to")
                if st.button("Assign"):
                    result = post_data(f"breaks/{selected_break_id}/assign", {
                        'assigned_to': assignee,
                        'user': 'current_user'
                    })
                    st.success("Break assigned successfully")
                
                # Resolve
                resolution_type = st.selectbox(
                    "Resolution Type",
                    ["ACCEPT_EXTERNAL", "ACCEPT_INTERNAL", "AMEND"]
                )
                resolution_notes = st.text_area("Resolution Notes")
                
                if st.button("Resolve Break", type="primary"):
                    result = post_data(f"breaks/{selected_break_id}/resolve", {
                        'resolution_type': resolution_type,
                        'notes': resolution_notes,
                        'user': 'current_user'
                    })
                    st.success("Break resolved successfully")
                    st.rerun()

elif page == "Reports":
    st.title("📑 Reports")
    
    tab1, tab2, tab3 = st.tabs(["Daily Report", "Monthly Summary", "Pattern Analysis"])
    
    with tab1:
        st.subheader("Daily Reconciliation Report")
        
        report_date = st.date_input("Select Date", value=date.today() - timedelta(days=1))
        
        if st.button("Generate Report"):
            report = fetch_data("reports/daily-reconciliation", params={'report_date': str(report_date)})
            
            if report:
                # Reconciliation summar
