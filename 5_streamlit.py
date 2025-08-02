import streamlit as st
import xarray as xr
import pandas as pd
import plotly.express as px
import yfinance as yf
import io

st.set_page_config(layout="wide")
st.title("Implied Volatility Dashboard")

@st.cache_data
def load_vix():
    try:
        vix = yf.download("^VIX", start=df["teo"].min(), end=df["teo"].max())
        if vix is not None and not vix.empty:
            vix = vix[["Close"]].rename(columns={"Close": "VIX"})/100
            vix = vix.reset_index()
            vix.columns = ["Date", "VIX"]
            return vix
        else:
            return pd.DataFrame(columns=["Date", "VIX"])
    except Exception as e:
        st.error(f"Error loading VIX data: {e}")
        return pd.DataFrame(columns=["Date", "VIX"])


@st.cache_data
def load_data():
    ds = xr.open_zarr("output/aggregated_iv_cross_with_totals_flat.zarr")
    df = ds.to_dataframe().reset_index().dropna(subset=["weighted_value"])
    df["teo"] = pd.to_datetime(df["teo"])

    # Clean labels
    df["gics_sector"] = df["gics_sector"].str.replace("_", " ")
    df["size_category"] = df["size_category"].str.replace("_", " ")
    df["style"] = df["style"].str.replace("_", " ")

    return df

df = load_data()

# Value type mapping for display
value_type_mapping = {
    "atmVol": "Raw IV",
    "atmCen": "Censored IV"
}

# Sidebar filters
expiry_label = st.sidebar.multiselect("Expiry Label", sorted(df["expiry_label"].unique()), default=["30"])
gics_sector = st.sidebar.multiselect("Sector", sorted(df["gics_sector"].unique()), default=["Total"])
size_category = st.sidebar.multiselect("Size Category", sorted(df["size_category"].unique()), default=["Total"])
style = st.sidebar.multiselect("Style", sorted(df["style"].unique()), default=["Total"])

# Create display names for value types
value_type_display_names = [value_type_mapping.get(vt, vt) for vt in sorted(df["value_type"].unique())]
value_type_original_names = sorted(df["value_type"].unique())
value_type_selection = st.sidebar.multiselect(
    "Value Types", 
    options=value_type_original_names,
    format_func=lambda x: value_type_mapping.get(x, x),
    default=value_type_original_names
)

# Date range selector
min_date = df["teo"].min().date()
max_date = df["teo"].max().date()
date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
    help="Select start and end dates for the analysis"
)

# Resampling option
freq_map = {
    "Daily": "D",
    "Weekly": "W",
    "Monthly": "M",
    "Quarterly": "Q",
    "Yearly": "Y"
}
resample_freq = st.sidebar.selectbox("Time Aggregation", list(freq_map.keys()))
freq = freq_map[resample_freq]

show_vix = st.sidebar.checkbox("Include VIX index", value=False)

chart_type = st.sidebar.radio("Chart Type", ["Line", "Bar"])

# Filter data
filtered = df[
    (df["value_type"].isin(value_type_selection)) &
    (df["expiry_label"].isin(expiry_label)) &
    (df["gics_sector"].isin(gics_sector)) &
    (df["size_category"].isin(size_category)) &
    (df["style"].isin(style)) &
    (df["teo"].dt.date >= date_range[0]) &
    (df["teo"].dt.date <= date_range[1])
]

# Resample data
resampled = (
    filtered
    .groupby(["gics_sector", "size_category", "style", "expiry_label", "value_type", pd.Grouper(key="teo", freq=freq)])
    ["weighted_value"].mean()
    .reset_index()
    .copy()  # Ensure we have a clean DataFrame
)

# Add a legend_label column for proper legend grouping
resampled["legend_label"] = (
    resampled["value_type"].replace(value_type_mapping).astype(str) + ", " +
    resampled["expiry_label"].astype(str) + ", " +
    resampled["gics_sector"].astype(str) + ", " +
    resampled["size_category"].astype(str) + ", " +
    resampled["style"].astype(str)
)

# Determine number of lines (series) to plot
num_lines = resampled['legend_label'].nunique()
if show_vix:
    num_lines += 1

# Base plot arguments
base_args = {
    "data_frame": resampled,
    "x": "teo",
    "y": "weighted_value",
    "color": "legend_label",
    "height": 700,
    "labels": {"teo": "", "weighted_value": ""}
}

facet_by = st.sidebar.selectbox("Facet by (optional)", ["None", "Size Category", "GICS Sector","Style"])

if facet_by == "Size Category":
    base_args["facet_col"] = "size_category"
elif facet_by == "GICS Sector":
    base_args["facet_col"] = "gics_sector"
elif facet_by == "Style":
    base_args["facet_col"] = "style"

# Create chart based on type
if chart_type == "Line":
    plot_args = base_args.copy()
    plot_args["line_group"] = "style"
    fig = px.line(**plot_args)
    for trace in fig.data:
        trace.connectgaps = False
else:  # Bar chart
    plot_args = base_args.copy()
    fig = px.bar(**plot_args)
    fig.update_layout(barmode="group")
fig.update_layout(
    showlegend=True,
    title="",
    legend=dict(
        x=1.2,
        y=1,
        xanchor='left',
        yanchor='top'
    )
)

if show_vix:
    vix = load_vix()
    if not vix.empty:
        vix['Date'] = pd.to_datetime(vix['Date'])
        resampled_dates = pd.to_datetime(resampled['teo'])
        vix_filtered = vix[(vix['Date'] >= resampled_dates.min()) & (vix['Date'] <= resampled_dates.max())]
        if not vix_filtered.empty:
            vix_filtered = vix_filtered.set_index('Date')
            vix_resampled = vix_filtered.resample(freq).mean().reset_index()
            if facet_by != "None":
                fig.add_scatter(
                    x=vix_resampled["Date"],
                    y=vix_resampled["VIX"],
                    name="VIX",
                    yaxis="y2",
                    line=dict(color='red', width=2),
                    row=1,
                    col=1
                )
            else:
                fig.add_scatter(
                    x=vix_resampled["Date"],
                    y=vix_resampled["VIX"],
                    name="VIX",
                    yaxis="y2",
                    line=dict(color='red', width=2)
                )
    fig.update_layout(
        yaxis2=dict(
            title="VIX",
            overlaying='y',
            side='right',
            showgrid=False
        )
        
    )

# Update facet titles
for anno in fig.layout.annotations:
    anno.text = anno.text.replace("gics_sector=", "Sector=")

st.plotly_chart(fig, use_container_width=True)

# Show data table
st.subheader("Data Table")
resampled['teo'] = resampled['teo'].dt.date  # Strip time
cols = ['teo'] + [col for col in resampled.columns if col != 'teo']
st.dataframe(resampled[cols])

@st.cache_data
def convert_to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='IV Data')
    return output.getvalue()


st.download_button(
    label="📥 Download Excel",
    data=convert_to_excel(resampled),
    file_name="iv_dashboard_data.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

with st.expander("ℹ️ Click to view data processing notes"):
    st.markdown("""
    ### 📘 How This Data Was Created

    This dashboard is powered by **SpiderRock’s surface curve historical data**, which provides a detailed snapshot of implied volatility surfaces across U.S. equity options. Each daily file includes key metrics for listed optionable tickers, such as:

    - **Raw IV** (`atmVol`): At-the-money implied volatility  
    - **Censored IV** (`atmCen`): Censored at-the-money implied volatility (adjusted to remove earnings-related volatility)  
    - Additional fields like option counts (`cCnt`, `pCnt`), curve shape (`slope`), and more

    From this dataset, we focus on analyzing **Raw IV** and **Censored IV** across expiries and then aggregating them by **sector**, **size**, and **style** dimensions.

    ### 🧹 Data Filtering & Term Structure Alignment

    To ensure quality and comparability across tickers and dates, we apply several filters:

    - **Maturity filter**: Only include option data with time to expiry between **0.01 and 2.0 years**
    - **Volatility cap**: **Raw IV** must be **below 150%**
    - **Liquidity check**: Must have at least **20 contracts total** (`cCnt + pCnt ≥ 20`)
    - **Bid-ask spread proxy**: The relative width of the volatility surface (`vwidth`) must be **≤ 0.2**

    ### 📐 Expiry Bucketing & Forward Implied Volatility

    Since raw expiries vary by ticker and day, we map each option to a standardized **target expiry bucket**. Specifically:

    - We define target expiries: **30, 60, 90, 120, 180, 270, 360, 540, and 720 days**
    - For each ticker on each date, we group all available options by maturity
    - We then select the expiry whose **actual days to maturity is closest** to each target  
      *(e.g., if a ticker has expiries at 28 and 32 days, it will be matched to the 30-day bucket using the closer of the two)*
    - Only the **closest match per target bucket** is retained to avoid duplication

    This approach ensures a consistent time structure across all assets, enabling robust forward volatility construction.

    We also compute **forward implied volatilities** between pairs of maturities using the standard calendar spread formula:
    """)
    
    st.latex(r"""\sigma_{fwd} = \sqrt{\frac{T_2 \cdot \sigma_2^2 - T_1 \cdot \sigma_1^2}{T_2 - T_1}}""")

    st.markdown("""
    Where \( T_1, T_2 \) are time-to-expiry in years, and \( \sigma_1, \sigma_2 \) are the corresponding **Raw IV** or **Censored IV** values.

    ### 🏷️ Sector, Size, and Style Enrichment

    Each ticker is further enriched with metadata:

    - **GICS Sector**: Inferred via mapping from fundamental industry codes
    - **Size Category**:
        - Large Cap: Market Cap ≥ \$10B  
        - Mid Cap: \$2B–\$10B  
        - Small Cap: < \$2B
    - **Investment Style**: Assigned as the **highest scoring style factor** (e.g., Growth, Value, GARP) on each date and forward-filled over time

    ### 📊 Aggregation & Output

    For each day and each combination of **sector**, **size**, **style**, **expiry**, and **value type** (**Raw IV** or **Censored IV**), we compute a **market cap–weighted average implied volatility**:
    """)

    st.latex(r"""\text{Weighted IV} = \frac{\sum (\text{IV} \times \text{Market Cap})}{\sum \text{Market Cap}}""")

