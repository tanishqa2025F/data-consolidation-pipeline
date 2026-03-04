"""
reporting.py
Auto-generates an HTML summary report with embedded charts.

Replaces the manual Monday morning dashboard update process — 
instead of manually downloading Excel files and updating Power BI,
this script produces a clean report automatically.
"""

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for server/script use
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import os
import base64
from io import BytesIO
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Professional color palette
COLORS = ["#1B2A4A", "#2CA58D", "#E8614D", "#F4A940", "#6C5CE7", "#636E72", "#00B894", "#D63031"]


def fig_to_base64(fig):
    """Convert matplotlib figure to base64 string for embedding in HTML."""
    buffer = BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight", facecolor="white")
    buffer.seek(0)
    img_str = base64.b64encode(buffer.read()).decode()
    plt.close(fig)
    return img_str


def create_revenue_by_service_chart(df):
    """Bar chart: Total revenue by service type."""
    if "revenue" not in df.columns or "service_type" not in df.columns:
        return None
    
    revenue_by_service = (
        df.groupby("service_type")["revenue"]
        .sum()
        .sort_values(ascending=True)
    )
    
    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(revenue_by_service.index, revenue_by_service.values, color=COLORS[:len(revenue_by_service)])
    ax.set_xlabel("Total Revenue ($)")
    ax.set_title("Revenue by Service Type", fontsize=14, fontweight="bold", pad=15)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    
    # Add value labels on bars
    for bar, val in zip(bars, revenue_by_service.values):
        ax.text(val + revenue_by_service.max() * 0.01, bar.get_y() + bar.get_height()/2,
                f"${val:,.0f}", va="center", fontsize=9)
    
    return fig_to_base64(fig)


def create_monthly_trend_chart(df):
    """Line chart: Monthly order volume and revenue trend."""
    if "date" not in df.columns:
        return None
    
    df_with_dates = df.dropna(subset=["date"]).copy()
    df_with_dates["month"] = pd.to_datetime(df_with_dates["date"]).dt.to_period("M")
    
    monthly = df_with_dates.groupby("month").agg(
        order_count=("date", "count"),
        total_revenue=("revenue", "sum")
    ).reset_index()
    monthly["month_str"] = monthly["month"].astype(str)
    
    fig, ax1 = plt.subplots(figsize=(10, 5))
    
    # Order volume (bars)
    ax1.bar(monthly["month_str"], monthly["order_count"], color=COLORS[0], alpha=0.7, label="Orders")
    ax1.set_ylabel("Number of Orders", color=COLORS[0])
    ax1.set_xlabel("Month")
    ax1.tick_params(axis="x", rotation=45)
    
    # Revenue trend (line on secondary axis)
    if "revenue" in df.columns and monthly["total_revenue"].sum() > 0:
        ax2 = ax1.twinx()
        ax2.plot(monthly["month_str"], monthly["total_revenue"], color=COLORS[1], 
                linewidth=2.5, marker="o", markersize=6, label="Revenue")
        ax2.set_ylabel("Revenue ($)", color=COLORS[1])
        ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    
    ax1.set_title("Monthly Order Volume & Revenue Trend", fontsize=14, fontweight="bold", pad=15)
    ax1.spines["top"].set_visible(False)
    fig.tight_layout()
    
    return fig_to_base64(fig)


def create_technician_performance_chart(df):
    """Horizontal bar chart: Orders and revenue per technician."""
    if "technician_name" not in df.columns:
        return None
    
    tech_stats = (
        df.groupby("technician_name")
        .agg(
            total_orders=("technician_name", "count"),
            total_revenue=("revenue", "sum")
        )
        .sort_values("total_revenue", ascending=True)
    )
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    
    # Orders by technician
    ax1.barh(tech_stats.index, tech_stats["total_orders"], color=COLORS[0])
    ax1.set_title("Orders by Technician", fontsize=12, fontweight="bold")
    ax1.set_xlabel("Number of Orders")
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    
    # Revenue by technician
    ax2.barh(tech_stats.index, tech_stats["total_revenue"], color=COLORS[1])
    ax2.set_title("Revenue by Technician", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Total Revenue ($)")
    ax2.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    
    fig.suptitle("Technician Performance Overview", fontsize=14, fontweight="bold", y=1.02)
    fig.tight_layout()
    
    return fig_to_base64(fig)


def create_data_quality_chart(metrics):
    """Donut chart: Data completeness by column."""
    null_pcts = {k: v for k, v in metrics["null_percentage"].items() if v > 0}
    
    if not null_pcts:
        return None
    
    fig, ax = plt.subplots(figsize=(8, 5))
    
    cols = list(null_pcts.keys())
    complete_pcts = [100 - v for v in null_pcts.values()]
    null_vals = list(null_pcts.values())
    
    x = range(len(cols))
    width = 0.35
    
    bars1 = ax.bar([i - width/2 for i in x], complete_pcts, width, label="Complete", color=COLORS[1])
    bars2 = ax.bar([i + width/2 for i in x], null_vals, width, label="Missing", color=COLORS[2])
    
    ax.set_ylabel("Percentage (%)")
    ax.set_title("Data Completeness by Column", fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(cols, rotation=45, ha="right")
    ax.legend()
    ax.set_ylim(0, 110)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    
    fig.tight_layout()
    return fig_to_base64(fig)


def generate_html_report(df, metrics, output_path):
    """
    Generate a complete HTML report with KPIs, charts, and data quality summary.
    This replaces the manual reporting process — runs automatically.
    """
    logger.info("Generating automated HTML report...")
    
    # Generate all charts
    revenue_chart = create_revenue_by_service_chart(df)
    trend_chart = create_monthly_trend_chart(df)
    tech_chart = create_technician_performance_chart(df)
    quality_chart = create_data_quality_chart(metrics)
    
    # Calculate KPIs
    total_records = metrics["total_rows"]
    total_revenue = df["revenue"].sum() if "revenue" in df.columns else 0
    unique_techs = df["technician_name"].nunique() if "technician_name" in df.columns else 0
    unique_customers = df["customer_name"].nunique() if "customer_name" in df.columns else 0
    avg_revenue = df["revenue"].mean() if "revenue" in df.columns else 0
    sources = len(metrics.get("sources", {}))
    
    # Source breakdown table rows
    source_rows = ""
    for source, count in metrics.get("sources", {}).items():
        source_rows += f"<tr><td>{source}</td><td>{count:,}</td></tr>"
    
    # Build HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Consolidated Data Report</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f5f6fa; color: #2d3436; }}
        .header {{ background: linear-gradient(135deg, #1B2A4A, #2CA58D); color: white; padding: 40px; text-align: center; }}
        .header h1 {{ font-size: 28px; margin-bottom: 8px; }}
        .header p {{ opacity: 0.85; font-size: 14px; }}
        .container {{ max-width: 1100px; margin: 0 auto; padding: 30px 20px; }}
        .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .kpi-card {{ background: white; border-radius: 10px; padding: 24px; text-align: center; box-shadow: 0 2px 10px rgba(0,0,0,0.08); }}
        .kpi-card .value {{ font-size: 32px; font-weight: 700; color: #1B2A4A; }}
        .kpi-card .label {{ font-size: 13px; color: #636e72; margin-top: 6px; text-transform: uppercase; letter-spacing: 0.5px; }}
        .section {{ background: white; border-radius: 10px; padding: 30px; margin-bottom: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); }}
        .section h2 {{ font-size: 20px; color: #1B2A4A; margin-bottom: 20px; padding-bottom: 10px; border-bottom: 2px solid #2CA58D; }}
        .chart-container {{ text-align: center; margin: 20px 0; }}
        .chart-container img {{ max-width: 100%; border-radius: 8px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        th {{ background: #1B2A4A; color: white; padding: 12px 16px; text-align: left; font-size: 13px; text-transform: uppercase; }}
        td {{ padding: 10px 16px; border-bottom: 1px solid #eee; font-size: 14px; }}
        tr:hover {{ background: #f8f9fa; }}
        .badge {{ display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; }}
        .badge-good {{ background: #d4edda; color: #155724; }}
        .badge-warn {{ background: #fff3cd; color: #856404; }}
        .badge-bad {{ background: #f8d7da; color: #721c24; }}
        .footer {{ text-align: center; padding: 30px; color: #636e72; font-size: 12px; }}
        .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
        @media (max-width: 768px) {{ .two-col {{ grid-template-columns: 1fr; }} }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Consolidated Data Report</h1>
        <p>Auto-generated on {datetime.now().strftime("%B %d, %Y at %I:%M %p")} | 
           {sources} data sources merged | {total_records:,} total records</p>
    </div>
    
    <div class="container">
        <!-- KPI Cards -->
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="value">{total_records:,}</div>
                <div class="label">Total Records</div>
            </div>
            <div class="kpi-card">
                <div class="value">${total_revenue:,.0f}</div>
                <div class="label">Total Revenue</div>
            </div>
            <div class="kpi-card">
                <div class="value">{unique_techs}</div>
                <div class="label">Technicians</div>
            </div>
            <div class="kpi-card">
                <div class="value">{unique_customers}</div>
                <div class="label">Unique Customers</div>
            </div>
            <div class="kpi-card">
                <div class="value">${avg_revenue:,.0f}</div>
                <div class="label">Avg Order Value</div>
            </div>
            <div class="kpi-card">
                <div class="value">{sources}</div>
                <div class="label">Data Sources</div>
            </div>
        </div>
        
        <!-- Revenue Analysis -->
        <div class="section">
            <h2>Revenue by Service Type</h2>
            {"<div class='chart-container'><img src='data:image/png;base64," + revenue_chart + "'/></div>" if revenue_chart else "<p>No revenue data available</p>"}
        </div>
        
        <!-- Monthly Trends -->
        <div class="section">
            <h2>Monthly Trends</h2>
            {"<div class='chart-container'><img src='data:image/png;base64," + trend_chart + "'/></div>" if trend_chart else "<p>No trend data available</p>"}
        </div>
        
        <!-- Technician Performance -->
        <div class="section">
            <h2>Technician Performance</h2>
            {"<div class='chart-container'><img src='data:image/png;base64," + tech_chart + "'/></div>" if tech_chart else "<p>No technician data available</p>"}
        </div>
        
        <!-- Data Quality -->
        <div class="section">
            <h2>Data Quality Summary</h2>
            {"<div class='chart-container'><img src='data:image/png;base64," + quality_chart + "'/></div>" if quality_chart else "<p>All columns 100% complete</p>"}
            
            <h3 style="margin-top: 20px; font-size: 16px;">Source Breakdown</h3>
            <table>
                <thead><tr><th>Data Source</th><th>Record Count</th></tr></thead>
                <tbody>{source_rows}</tbody>
            </table>
        </div>
        
        <!-- Pipeline Info -->
        <div class="section">
            <h2>Pipeline Details</h2>
            <p style="color: #636e72; line-height: 1.8;">
                This report was auto-generated by the data consolidation pipeline.<br>
                <strong>Date range:</strong> {metrics['date_range']['min']} to {metrics['date_range']['max']}<br>
                <strong>Duplicate rows removed:</strong> {metrics['duplicate_rows']}<br>
                <strong>Processing steps:</strong> Schema standardization → Data cleaning → 
                Deduplication → Validation → Report generation
            </p>
        </div>
    </div>
    
    <div class="footer">
        Data Consolidation Pipeline | Built with Python, pandas, matplotlib
    </div>
</body>
</html>"""
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(html)
    
    logger.info(f"Report saved to: {output_path}")
    return output_path
