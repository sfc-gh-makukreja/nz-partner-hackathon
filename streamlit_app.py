import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json
import pydeck as pdk
import numpy as np

# App configuration
st.set_page_config(
    page_title="NZ Flood Zone Analysis",
    page_icon="🌊",
    layout="wide"
)

# Snowflake connection
@st.cache_resource
def init_connection():
    return st.connection('snowflake').session()

# Data loading with caching
@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_flood_zone_data():
    query = """
    SELECT 
        f.reference,
        f.comments,
        b.geometry_type,
        b.coordinate_count,
        f.shape_area_sqm,
        b.geometry_json,
        CASE 
            WHEN b.coordinate_count > 0 THEN ROUND(f.shape_area_sqm / b.coordinate_count, 2)
            ELSE NULL 
        END as area_per_coordinate,
        CASE 
            WHEN b.coordinate_count > 100 THEN 'Highly Complex'
            WHEN b.coordinate_count > 50 THEN 'Complex'
            WHEN b.coordinate_count > 20 THEN 'Moderate'
            WHEN b.coordinate_count = 0 THEN 'No Coordinates'
            ELSE 'Simple'
        END as complexity_level
    FROM nz_partner_hackathon.WAIPUNA_RANGI.waipa_flood_zones f
    JOIN nz_partner_hackathon.WAIPUNA_RANGI.waipa_flood_boundaries b ON f.fid = b.fid
    ORDER BY b.coordinate_count DESC
    """
    session = init_connection()
    return session.sql(query).to_pandas()

@st.cache_data(ttl=600)
def load_summary_stats():
    query = """
    SELECT 
        COUNT(*) as total_zones,
        ROUND(SUM(f.shape_area_sqm) / 1000000, 3) as total_area_km2,
        ROUND(AVG(f.shape_area_sqm), 0) as avg_area_sqm,
        MAX(b.coordinate_count) as max_coordinates,
        ROUND(AVG(b.coordinate_count), 1) as avg_coordinates
    FROM nz_partner_hackathon.WAIPUNA_RANGI.waipa_flood_zones f
    JOIN nz_partner_hackathon.WAIPUNA_RANGI.waipa_flood_boundaries b ON f.fid = b.fid
    """
    
    session = init_connection()
    return session.sql(query).to_pandas()

def main():
    st.title("🌊 Waipa Flood Zone Complexity Analysis")
    st.markdown("Interactive analysis of flood zone geometric complexity and coverage")
    
    # Load data
    with st.spinner("Loading flood zone data..."):
        flood_data = load_flood_zone_data()
        summary_stats = load_summary_stats()
    
    if flood_data.empty:
        st.error("No flood zone data available")
        return
    
    # Summary metrics
    st.subheader("📊 Overview")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Zones", f"{summary_stats.iloc[0]['TOTAL_ZONES']:,}")
    with col2:
        st.metric("Total Area", f"{summary_stats.iloc[0]['TOTAL_AREA_KM2']:.1f} km²")
    with col3:
        st.metric("Avg Zone Size", f"{summary_stats.iloc[0]['AVG_AREA_SQM']:,.0f} m²")
    with col4:
        st.metric("Max Coordinates", f"{summary_stats.iloc[0]['MAX_COORDINATES']:,}")
    with col5:
        st.metric("Avg Coordinates", f"{summary_stats.iloc[0]['AVG_COORDINATES']:.1f}")
    
    # Main analysis sections
    tab1, tab2, tab3, tab4 = st.tabs(["🔍 Complexity Analysis", "📈 Visualizations", "🗺️ Zone Details", "📊 Data Export"])
    
    with tab1:
        complexity_analysis(flood_data)
    
    with tab2:
        create_visualizations(flood_data)
    
    with tab3:
        zone_details(flood_data)
    
    with tab4:
        data_export(flood_data)

def complexity_analysis(flood_data):
    st.subheader("Geometric Complexity Distribution")
    
    # Complexity level distribution
    complexity_dist = flood_data['COMPLEXITY_LEVEL'].value_counts()
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Pie chart of complexity levels
        fig_pie = px.pie(
            values=complexity_dist.values,
            names=complexity_dist.index,
            title="Distribution by Complexity Level",
            color_discrete_map={
                'Simple': '#2E8B57',
                'Moderate': '#FFD700', 
                'Complex': '#FF8C00',
                'Highly Complex': '#DC143C',
                'No Coordinates': '#808080'
            }
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Statistics by complexity level
        complexity_stats = flood_data.groupby('COMPLEXITY_LEVEL').agg({
            'SHAPE_AREA_SQM': ['count', 'mean', 'sum'],
            'COORDINATE_COUNT': 'mean',
            'AREA_PER_COORDINATE': 'mean'
        }).round(2)
        
        st.write("**Statistics by Complexity Level:**")
        st.dataframe(complexity_stats, use_container_width=True)
    
    # Filter controls
    st.subheader("🔍 Filter Analysis")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_complexity = st.multiselect(
            "Select Complexity Levels",
            options=flood_data['COMPLEXITY_LEVEL'].unique(),
            default=flood_data['COMPLEXITY_LEVEL'].unique()
        )
    with col2:
        min_area = st.number_input(
            "Minimum Area (m²)",
            value=0,
            max_value=int(flood_data['SHAPE_AREA_SQM'].max())
        )
    with col3:
        min_coordinates = st.number_input(
            "Minimum Coordinates",
            value=0,
            max_value=int(flood_data['COORDINATE_COUNT'].max())
        )
    
    # Apply filters
    filtered_data = flood_data[
        (flood_data['COMPLEXITY_LEVEL'].isin(selected_complexity)) &
        (flood_data['SHAPE_AREA_SQM'] >= min_area) &
        (flood_data['COORDINATE_COUNT'] >= min_coordinates)
    ]
    
    st.write(f"**Filtered Results: {len(filtered_data)} zones**")
    st.dataframe(filtered_data[['REFERENCE', 'COMPLEXITY_LEVEL', 'COORDINATE_COUNT', 'SHAPE_AREA_SQM', 'AREA_PER_COORDINATE']].head(20))

def create_visualizations(flood_data):
    st.subheader("📈 Interactive Visualizations")
    
    # Scatter plot: Area vs Coordinates
    col1, col2 = st.columns(2)
    
    with col1:
        # Filter out zones with NaN area_per_coordinate for the size parameter
        scatter_data = flood_data.dropna(subset=['AREA_PER_COORDINATE']).copy()
        
        # If we have valid data for sizing
        if not scatter_data.empty:
            fig_scatter = px.scatter(
                scatter_data,
                x='COORDINATE_COUNT',
                y='SHAPE_AREA_SQM',
                color='COMPLEXITY_LEVEL',
                size='AREA_PER_COORDINATE',
                hover_data=['REFERENCE', 'COMMENTS'],
                title="Zone Area vs Coordinate Count (with Efficiency Sizing)",
                labels={
                    'COORDINATE_COUNT': 'Number of Coordinates',
                    'SHAPE_AREA_SQM': 'Area (m²)',
                    'COMPLEXITY_LEVEL': 'Complexity'
                },
                color_discrete_map={
                    'Simple': '#2E8B57',
                    'Moderate': '#FFD700', 
                    'Complex': '#FF8C00',
                    'Highly Complex': '#DC143C',
                    'No Coordinates': '#808080'
                }
            )
        else:
            # Fallback: use area for sizing instead
            fig_scatter = px.scatter(
                flood_data,
                x='COORDINATE_COUNT',
                y='SHAPE_AREA_SQM',
                color='COMPLEXITY_LEVEL',
                size='SHAPE_AREA_SQM',
                hover_data=['REFERENCE', 'COMMENTS'],
                title="Zone Area vs Coordinate Count",
                labels={
                    'COORDINATE_COUNT': 'Number of Coordinates',
                    'SHAPE_AREA_SQM': 'Area (m²)',
                    'COMPLEXITY_LEVEL': 'Complexity'
                },
                color_discrete_map={
                    'Simple': '#2E8B57',
                    'Moderate': '#FFD700', 
                    'Complex': '#FF8C00',
                    'Highly Complex': '#DC143C',
                    'No Coordinates': '#808080'
                }
            )
        
        fig_scatter.update_layout(height=500)
        st.plotly_chart(fig_scatter, use_container_width=True)
        
        # Show count of excluded zones if any
        excluded_count = len(flood_data) - len(scatter_data)
        if excluded_count > 0:
            st.caption(f"ℹ️ {excluded_count} zones with no coordinates excluded from sizing")
    
    with col2:
        # Histogram of coordinate counts
        fig_hist = px.histogram(
            flood_data,
            x='COORDINATE_COUNT',
            color='COMPLEXITY_LEVEL',
            title="Distribution of Coordinate Counts",
            labels={'COORDINATE_COUNT': 'Number of Coordinates'},
            marginal="box",
            color_discrete_map={
                'Simple': '#2E8B57',
                'Moderate': '#FFD700', 
                'Complex': '#FF8C00',
                'Highly Complex': '#DC143C',
                'No Coordinates': '#808080'
            }
        )
        fig_hist.update_layout(height=500)
        st.plotly_chart(fig_hist, use_container_width=True)
    
    # Box plot showing area distribution by complexity
    fig_box = px.box(
        flood_data,
        x='COMPLEXITY_LEVEL',
        y='SHAPE_AREA_SQM',
        title="Area Distribution by Complexity Level",
        labels={
            'COMPLEXITY_LEVEL': 'Complexity Level',
            'SHAPE_AREA_SQM': 'Area (m²)'
        }
    )
    fig_box.update_layout(height=400)
    st.plotly_chart(fig_box, use_container_width=True)
    
    # Area per coordinate efficiency analysis
    st.subheader("🎯 Geometric Efficiency Analysis")
    
    # Calculate efficiency metrics - only for zones with coordinates > 0
    efficiency_data = flood_data[flood_data['AREA_PER_COORDINATE'].notna()].copy()
    
    if not efficiency_data.empty:
        fig_efficiency = px.scatter(
            efficiency_data,
            x='COORDINATE_COUNT',
            y='AREA_PER_COORDINATE',
            color='COMPLEXITY_LEVEL',
            size='SHAPE_AREA_SQM',
            hover_data=['REFERENCE'],
            title="Geometric Efficiency: Area per Coordinate",
            labels={
                'COORDINATE_COUNT': 'Number of Coordinates',
                'AREA_PER_COORDINATE': 'Area per Coordinate (m²/coord)',
                'COMPLEXITY_LEVEL': 'Complexity'
            },
            color_discrete_map={
                'Simple': '#2E8B57',
                'Moderate': '#FFD700', 
                'Complex': '#FF8C00',
                'Highly Complex': '#DC143C'
            }
        )
        
        # Add trend line
        try:
            trend_fig = px.scatter(
                efficiency_data, 
                x='COORDINATE_COUNT', 
                y='AREA_PER_COORDINATE', 
                trendline="ols"
            )
            if len(trend_fig.data) > 1:
                fig_efficiency.add_traces(
                    trend_fig.data[1].update(name="Trend", line_color="red")
                )
        except Exception as e:
            st.caption(f"⚠️ Could not add trend line: {str(e)}")
        
        st.plotly_chart(fig_efficiency, use_container_width=True)
        
        # Show efficiency statistics
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Efficiency Statistics:**")
            st.write(f"• Zones with efficiency data: **{len(efficiency_data)}**")
            st.write(f"• Average efficiency: **{efficiency_data['AREA_PER_COORDINATE'].mean():.1f}** m²/coord")
            st.write(f"• Most efficient: **{efficiency_data['AREA_PER_COORDINATE'].max():.1f}** m²/coord")
            st.write(f"• Least efficient: **{efficiency_data['AREA_PER_COORDINATE'].min():.1f}** m²/coord")
        
        with col2:
            # Show zones excluded from efficiency analysis
            excluded_zones = flood_data[flood_data['AREA_PER_COORDINATE'].isna()]
            if not excluded_zones.empty:
                st.write("**Zones without efficiency data:**")
                st.write(f"• **{len(excluded_zones)}** zones with 0 coordinates")
                st.write(f"• Total area: **{excluded_zones['SHAPE_AREA_SQM'].sum()/10000:.1f}** hectares")
    else:
        st.warning("No zones have coordinate data for efficiency analysis.")

def zone_details(flood_data):
    st.subheader("🗺️ Individual Zone Analysis")
    
    # Zone selector
    selected_zone = st.selectbox(
        "Select a flood zone for detailed analysis:",
        options=flood_data['REFERENCE'].tolist(),
        format_func=lambda x: f"{x} ({flood_data[flood_data['REFERENCE']==x]['COMPLEXITY_LEVEL'].iloc[0]})"
    )
    
    if selected_zone:
        zone_info = flood_data[flood_data['REFERENCE'] == selected_zone].iloc[0]
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Zone Information:**")
            st.write(f"**Reference:** {zone_info['REFERENCE']}")
            st.write(f"**Complexity:** {zone_info['COMPLEXITY_LEVEL']}")
            st.write(f"**Geometry Type:** {zone_info['GEOMETRY_TYPE']}")
            st.write(f"**Coordinates:** {zone_info['COORDINATE_COUNT']:,}")
            st.write(f"**Area:** {zone_info['SHAPE_AREA_SQM']:,.0f} m² ({zone_info['SHAPE_AREA_SQM']/10000:.2f} ha)")
            if pd.notna(zone_info['AREA_PER_COORDINATE']):
                st.write(f"**Efficiency:** {zone_info['AREA_PER_COORDINATE']:.1f} m²/coordinate")
            else:
                st.write("**Efficiency:** N/A (no coordinates)")
        
        with col2:
            if zone_info['COMMENTS']:
                st.write("**Comments:**")
                st.write(zone_info['COMMENTS'])
            else:
                st.write("*No additional comments available*")
            
        # Map visualization
        st.subheader("🗺️ Zone Boundary Map")
        
        if pd.notna(zone_info['GEOMETRY_JSON']) and zone_info['COORDINATE_COUNT'] > 0:
            try:
                # Parse the geometry JSON
                geometry_data = json.loads(zone_info['GEOMETRY_JSON'])
                
                # Default to Waipa District center coordinates
                center_lat, center_lon = -38.0, 175.3
                polygon_coords = []
                
                # Extract polygon coordinates for pydeck
                if geometry_data and 'coordinates' in geometry_data:
                    coords = geometry_data['coordinates']
                    if coords and len(coords) > 0:
                        # Handle Polygon type
                        if geometry_data.get('type') == 'Polygon':
                            ring = coords[0] if coords[0] else []
                            if ring and len(ring) > 2:
                                polygon_coords = [[coord[0], coord[1]] for coord in ring if len(coord) >= 2]
                                if polygon_coords:
                                    # Calculate center
                                    lons = [coord[0] for coord in polygon_coords]
                                    lats = [coord[1] for coord in polygon_coords]
                                    center_lon = sum(lons) / len(lons)
                                    center_lat = sum(lats) / len(lats)
                        
                        # Handle MultiPolygon type (use first polygon)
                        elif geometry_data.get('type') == 'MultiPolygon':
                            if coords[0] and len(coords[0]) > 0:
                                ring = coords[0][0] if coords[0][0] else []
                                if ring and len(ring) > 2:
                                    polygon_coords = [[coord[0], coord[1]] for coord in ring if len(coord) >= 2]
                                    if polygon_coords:
                                        lons = [coord[0] for coord in polygon_coords]
                                        lats = [coord[1] for coord in polygon_coords]
                                        center_lon = sum(lons) / len(lons)
                                        center_lat = sum(lats) / len(lats)
                
                if polygon_coords:
                    # Create polygon data for pydeck
                    polygon_data = pd.DataFrame([{
                        'coordinates': [polygon_coords],
                        'zone_ref': zone_info['REFERENCE'],
                        'area': zone_info['SHAPE_AREA_SQM'],
                        'complexity': zone_info['COMPLEXITY_LEVEL']
                    }])
                    
                    # Create center point data
                    center_data = pd.DataFrame([{
                        'lat': center_lat,
                        'lon': center_lon,
                        'zone_ref': zone_info['REFERENCE']
                    }])
                    
                    # Create the pydeck chart
                    st.pydeck_chart(
                        pdk.Deck(
                            map_style=None,  # Use Streamlit theme
                            initial_view_state=pdk.ViewState(
                                latitude=center_lat,
                                longitude=center_lon,
                                zoom=14,
                                pitch=0,
                            ),
                            layers=[
                                pdk.Layer(
                                    "PolygonLayer",
                                    data=polygon_data,
                                    get_polygon="coordinates",
                                    get_fill_color="[255, 107, 107, 80]",
                                    get_line_color="[255, 0, 0, 255]",
                                    line_width_min_pixels=2,
                                    pickable=True,
                                    auto_highlight=True,
                                ),
                                pdk.Layer(
                                    "ScatterplotLayer",
                                    data=center_data,
                                    get_position="[lon, lat]",
                                    get_color="[255, 0, 0, 200]",
                                    get_radius=50,
                                    pickable=True,
                                ),
                            ],
                            tooltip={
                                'html': '<b>Flood Zone: {zone_ref}</b><br/>Area: {area:,} m²<br/>Complexity: {complexity}',
                                'style': {'backgroundColor': 'steelblue', 'color': 'white'}
                            }
                        ),
                        height=500
                    )
                    
                    # Show zone summary below map
                    st.info(f"📍 **{zone_info['REFERENCE']}** - {zone_info['COMPLEXITY_LEVEL']} flood zone covering {zone_info['SHAPE_AREA_SQM']:,.0f} m²")
                
                else:
                    st.warning("Could not parse polygon coordinates for visualization")
                    st.write("**Geometry type:**", geometry_data.get('type', 'Unknown'))
                    with st.expander("Debug: View raw coordinates"):
                        st.json(geometry_data.get('coordinates', [])[:10] if geometry_data.get('coordinates') else [])
                
            except Exception as e:
                st.error(f"Error creating map visualization: {str(e)}")
                st.write("**Debug info:**")
                st.code(zone_info['GEOMETRY_JSON'][:200] + "..." if len(zone_info['GEOMETRY_JSON']) > 200 else zone_info['GEOMETRY_JSON'])
        else:
            st.warning("No geometry data available for this zone")
        
        # Expandable section for raw geometry data
        if pd.notna(zone_info['GEOMETRY_JSON']):
            with st.expander("View Raw Geometry Data"):
                st.json(zone_info['GEOMETRY_JSON'])

def data_export(flood_data):
    st.subheader("📊 Data Export & Insights")
    
    # Export options
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Export Options:**")
        
        # Full dataset
        csv_full = flood_data.to_csv(index=False)
        st.download_button(
            label="📄 Download Full Dataset (CSV)",
            data=csv_full,
            file_name=f"waipa_flood_zones_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
            mime='text/csv'
        )
        
        # Summary by complexity
        summary_export = flood_data.groupby('COMPLEXITY_LEVEL').agg({
            'SHAPE_AREA_SQM': ['count', 'sum', 'mean'],
            'COORDINATE_COUNT': 'mean',
            'AREA_PER_COORDINATE': 'mean'
        }).round(2)
        
        csv_summary = summary_export.to_csv()
        st.download_button(
            label="📊 Download Summary Stats (CSV)",
            data=csv_summary,
            file_name=f"flood_zone_complexity_summary_{datetime.now().strftime('%Y%m%d')}.csv",
            mime='text/csv'
        )
    
    with col2:
        st.write("**Key Insights:**")
        
        total_zones = len(flood_data)
        highly_complex = len(flood_data[flood_data['COMPLEXITY_LEVEL'] == 'Highly Complex'])
        avg_coordinates = flood_data['COORDINATE_COUNT'].mean()
        total_area_km2 = flood_data['SHAPE_AREA_SQM'].sum() / 1_000_000
        
        st.write(f"• **{total_zones}** total flood zones analyzed")
        st.write(f"• **{highly_complex}** ({highly_complex/total_zones*100:.1f}%) highly complex zones")
        st.write(f"• **{avg_coordinates:.1f}** average coordinates per zone")
        st.write(f"• **{total_area_km2:.2f} km²** total flood-prone area")
        
        # Most complex zone
        if flood_data['COORDINATE_COUNT'].max() > 0:
            most_complex = flood_data.loc[flood_data['COORDINATE_COUNT'].idxmax()]
            st.write(f"• Most complex zone: **{most_complex['REFERENCE']}** ({most_complex['COORDINATE_COUNT']:,} coordinates)")
        
        # Zones without coordinates
        no_coords = len(flood_data[flood_data['COORDINATE_COUNT'] == 0])
        if no_coords > 0:
            st.write(f"• **{no_coords}** zones have no coordinate data")

if __name__ == "__main__":
    main()