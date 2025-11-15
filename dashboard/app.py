"""
Streamlit dashboard for data visualization.

This module creates an interactive dashboard for visualizing
the scraped book data with advanced analytics.

@module app
@author Jeffrey Dabo
@date 2025
"""
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
from loguru import logger

from src.database.connection import db_manager
from src.scraper.books_scraper import BooksScraper


# Page configuration
st.set_page_config(
    page_title="Books Data Pipeline Dashboard",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
    }
    .big-font {
        font-size: 20px !important;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)


def init_database():
    """
    Initialize database connection.

    @returns {None}
    """
    try:
        db_manager.initialize()
        db_manager.create_tables()
    except Exception as e:
        st.error(f"Database initialization failed: {e}")


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    """
    Load data from database with caching.

    @returns {DataFrame} Pandas DataFrame with book data
    """
    try:
        books = db_manager.get_all_books()
        if not books:
            return pd.DataFrame()

        df = pd.DataFrame(books)

        # Data type conversions and cleaning
        if 'scraped_at' in df.columns:
            df['scraped_at'] = pd.to_datetime(df['scraped_at'])
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'])

        # Extract stock quantity from availability
        df['stock_quantity'] = df['availability'].str.extract(r'(\d+)').astype(float)
        df['in_stock'] = df['availability'].str.contains('In stock', case=False, na=False)

        # Clean category
        df['category'] = df['category'].fillna('Unknown')

        # Add price bins for analysis
        df['price_range'] = pd.cut(df['price'],
                                   bins=[0, 20, 40, 60, 80, 100],
                                   labels=['£0-20', '£20-40', '£40-60', '£60-80', '£80+'])

        return df
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return pd.DataFrame()


def scrape_data(max_pages=None, by_category=False):
    """
    Trigger scraping process.

    @param {int|None} max_pages - Maximum pages to scrape
    @param {bool} by_category - Whether to scrape by category
    @returns {int} Number of books scraped
    """
    try:
        with st.spinner("Scraping data... This may take a while."):
            scraper = BooksScraper()
            books = scraper.scrape(max_pages=max_pages, by_category=by_category)
            scraper.close()

            if books:
                inserted = db_manager.insert_books_bulk(books)
                st.success(f"Scraped {len(books)} books, inserted {inserted} new records")
                st.cache_data.clear()  # Clear cache to reload data
                return len(books)
            else:
                st.warning("No books were scraped")
                return 0
    except Exception as e:
        st.error(f"Scraping failed: {e}")
        return 0


def show_price_analysis(df):
    """
    Show price analysis section.

    @param {DataFrame} df - Book data
    @returns {None}
    """
    st.subheader("Price Analysis")

    col1, col2 = st.columns(2)

    with col1:
        # Price distribution histogram
        fig = px.histogram(
            df,
            x='price',
            nbins=50,
            title='Price Distribution',
            labels={'price': 'Price (£)', 'count': 'Number of Books'},
            color_discrete_sequence=['#1f77b4']
        )
        fig.add_vline(x=df['price'].mean(), line_dash="dash", line_color="red",
                      annotation_text=f"Mean: £{df['price'].mean():.2f}")
        fig.add_vline(x=df['price'].median(), line_dash="dash", line_color="green",
                      annotation_text=f"Median: £{df['price'].median():.2f}")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Price range breakdown
        price_range_counts = df['price_range'].value_counts().sort_index()
        fig = px.bar(
            x=price_range_counts.index,
            y=price_range_counts.values,
            title='Books by Price Range',
            labels={'x': 'Price Range', 'y': 'Number of Books'},
            color=price_range_counts.values,
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        # Box plot by rating - FIXED
        fig = px.box(
            df,
            x='rating',
            y='price',
            title='Price Distribution by Rating',
            labels={'rating': 'Rating', 'price': 'Price (£)'},
            color='rating',  # Use discrete color
            color_discrete_sequence=px.colors.sequential.Viridis
        )
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        # Price statistics by category (top 10)
        top_categories = df['category'].value_counts().head(10).index
        df_top = df[df['category'].isin(top_categories)]

        category_stats = df_top.groupby('category')['price'].agg(['mean', 'min', 'max']).reset_index()
        category_stats = category_stats.sort_values('mean', ascending=False)

        fig = go.Figure()
        fig.add_trace(go.Bar(
            name='Average',
            x=category_stats['category'],
            y=category_stats['mean'],
            marker_color='lightblue'
        ))
        fig.update_layout(
            title='Average Price by Category (Top 10)',
            xaxis_title='Category',
            yaxis_title='Price (£)',
            xaxis_tickangle=-45,
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)


def show_rating_analysis(df):
    """
    Show rating analysis section.

    @param {DataFrame} df - Book data
    @returns {None}
    """
    st.subheader("Rating Analysis")

    col1, col2 = st.columns(2)

    with col1:
        # Rating distribution
        rating_counts = df['rating'].value_counts().sort_index()
        fig = px.bar(
            x=rating_counts.index,
            y=rating_counts.values,
            title='Rating Distribution',
            labels={'x': 'Rating', 'y': 'Number of Books'},
            text=rating_counts.values,
            color=rating_counts.values,
            color_continuous_scale='YlOrRd'
        )
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Rating percentage
        fig = px.pie(
            values=rating_counts.values,
            names=[f"{i}" for i in rating_counts.index],
            title='Rating Distribution (%)',
            hole=0.4,
            color_discrete_sequence=px.colors.sequential.RdBu
        )
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        # Scatter: Price vs Rating - FIXED
        sample_df = df.sample(min(500, len(df)))  # Sample for performance
        fig = px.scatter(
            sample_df,
            x='rating',
            y='price',
            title='Price vs Rating Correlation',
            labels={'rating': 'Rating', 'price': 'Price (£)'},
            trendline='ols',
            opacity=0.6,
            color='price',  # Color by price instead
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        # Average price by rating
        rating_price = df.groupby('rating')['price'].mean().reset_index()
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=rating_price['rating'],
            y=rating_price['price'],
            mode='lines+markers',
            marker=dict(
                size=12,
                color=rating_price['price'],
                colorscale='Viridis',
                showscale=True
            ),
            line=dict(width=3, color='rgba(100, 100, 250, 0.5)')
        ))
        fig.update_layout(
            title='Average Price by Rating',
            xaxis_title='Rating',
            yaxis_title='Average Price (£)',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)


def show_advanced_analytics(df):
    """
    Show advanced analytics and correlations.

    @param {DataFrame} df - Book data
    @returns {None}
    """
    st.subheader("🔬 Advanced Analytics")

    # Correlation analysis
    st.markdown("#### Price Correlation Analysis")

    numeric_cols = ['price', 'rating', 'price_excl_tax', 'price_incl_tax', 'tax']
    available_cols = [col for col in numeric_cols if col in df.columns and df[col].notna().any()]

    if len(available_cols) >= 2:
        corr_matrix = df[available_cols].corr()

        fig = px.imshow(
            corr_matrix,
            text_auto='.2f',
            title='Correlation Heatmap',
            color_continuous_scale='RdBu_r',
            aspect='auto',
            labels=dict(color="Correlation")
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        # Top 10 most expensive books
        st.markdown("#### Top 10 Most Expensive Books")
        top_expensive = df.nlargest(10, 'price')[['title', 'price', 'category', 'rating']].copy()
        top_expensive['title'] = top_expensive['title'].str[:50] + '...'
        st.dataframe(
            top_expensive.style.background_gradient(subset=['price'], cmap='Reds'),
            use_container_width=True,
            hide_index=True
        )

    with col2:
        # Top 10 highest rated books
        st.markdown("#### Top Rated Books (5 Stars)")
        top_rated = df[df['rating'] == 5].nlargest(10, 'price')[['title', 'price', 'category', 'rating']].copy()
        if not top_rated.empty:
            top_rated['title'] = top_rated['title'].str[:50] + '...'
            st.dataframe(
                top_rated.style.background_gradient(subset=['price'], cmap='Greens'),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No 5-star rated books found")

    # Value for money analysis
    st.markdown("#### Best Value Books (High Rating, Low Price)")
    df['value_score'] = df['rating'] / (df['price'] + 1)  # +1 to avoid division by zero
    best_value = df.nlargest(15, 'value_score')[['title', 'price', 'rating', 'category', 'value_score']].copy()

    # Truncate long titles
    best_value['title_short'] = best_value['title'].str[:30] + '...'

    fig = px.scatter(
        best_value,
        x='price',
        y='rating',
        size='value_score',
        hover_data=['title', 'category'],
        title='Best Value Books (Bubble size = Value Score)',
        labels={'price': 'Price (£)', 'rating': 'Rating'},
        color='category',
        size_max=60
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

    # Show table
    display_cols = ['title', 'price', 'rating', 'category', 'value_score']
    best_value_display = best_value[display_cols].copy()
    best_value_display['title'] = best_value_display['title'].str[:60] + '...'
    st.dataframe(
        best_value_display.style.background_gradient(subset=['value_score'], cmap='Greens'),
        use_container_width=True,
        hide_index=True
    )


def show_category_analysis(df):
    """
    Show category analysis section.

    @param {DataFrame} df - Book data
    @returns {None}
    """
    st.subheader("Category Analysis")

    col1, col2 = st.columns(2)

    with col1:
        # Category distribution
        category_counts = df['category'].value_counts().head(15)
        fig = px.bar(
            x=category_counts.values,
            y=category_counts.index,
            orientation='h',
            title='Top 15 Categories by Book Count',
            labels={'x': 'Number of Books', 'y': 'Category'},
            color=category_counts.values,
            color_continuous_scale='Teal',
            text=category_counts.values
        )
        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            height=500
        )
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Category pie chart
        top_10_counts = category_counts[:10]
        fig = px.pie(
            values=top_10_counts.values,
            names=top_10_counts.index,
            title='Top 10 Categories Distribution',
            hole=0.4
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    # Category insights table
    st.markdown("#### Category Statistics")

    category_stats = df.groupby('category').agg({
        'price': ['mean', 'min', 'max'],
        'rating': 'mean',
        'in_stock': 'sum',
        'id': 'count'
    }).round(2)

    category_stats.columns = ['Avg Price', 'Min Price', 'Max Price', 'Avg Rating', 'In Stock', 'Total Books']
    category_stats = category_stats.sort_values('Total Books', ascending=False).head(20)
    category_stats = category_stats.reset_index()

    st.dataframe(
        category_stats.style.background_gradient(subset=['Total Books'], cmap='Blues'),
        use_container_width=True,
        hide_index=True
    )


def show_availability_analysis(df):
    """
    Show availability and stock analysis.

    @param {DataFrame} df - Book data
    @returns {None}
    """
    st.subheader("Availability & Stock Analysis")

    col1, col2 = st.columns(2)

    with col1:
        # Stock status
        stock_status = df['in_stock'].value_counts()
        labels = ['In Stock' if x else 'Out of Stock' for x in stock_status.index]
        fig = px.pie(
            values=stock_status.values,
            names=labels,
            title='Stock Status',
            color_discrete_sequence=['#2ecc71', '#e74c3c'],
            hole=0.3
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Stock quantity distribution
        df_with_qty = df[df['stock_quantity'].notna()]
        if not df_with_qty.empty:
            fig = px.histogram(
                df_with_qty,
                x='stock_quantity',
                nbins=20,
                title='Stock Quantity Distribution',
                labels={'stock_quantity': 'Stock Quantity', 'count': 'Number of Books'},
                color_discrete_sequence=['#3498db']
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No stock quantity data available")

    # Category-wise stock analysis
    st.markdown("#### Stock by Category (Top 15)")

    category_stock = df.groupby('category').agg({
        'in_stock': 'sum',
        'id': 'count'
    }).reset_index()
    category_stock.columns = ['Category', 'In Stock', 'Total']
    category_stock['Out of Stock'] = category_stock['Total'] - category_stock['In Stock']
    category_stock = category_stock.sort_values('Total', ascending=False).head(15)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name='In Stock',
        x=category_stock['Category'],
        y=category_stock['In Stock'],
        marker_color='lightgreen',
        text=category_stock['In Stock'],
        textposition='inside'
    ))
    fig.add_trace(go.Bar(
        name='Out of Stock',
        x=category_stock['Category'],
        y=category_stock['Out of Stock'],
        marker_color='lightcoral',
        text=category_stock['Out of Stock'],
        textposition='inside'
    ))
    fig.update_layout(
        barmode='stack',
        xaxis_title='Category',
        yaxis_title='Number of Books',
        xaxis_tickangle=-45,
        height=500
    )
    st.plotly_chart(fig, use_container_width=True)


def show_overview_metrics(df):
    """
    Display overview metrics.

    @param {DataFrame} df - Book data
    @returns {None}
    """
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "Total Books",
            f"{len(df):,}",
            help="Total number of books in database"
        )

    with col2:
        avg_price = df['price'].mean()
        st.metric(
            "Avg Price",
            f"£{avg_price:.2f}",
            help="Average book price"
        )

    with col3:
        avg_rating = df['rating'].mean()
        st.metric(
            "Avg Rating",
            f"{avg_rating:.1f}/5",
            help="Average book rating"
        )

    with col4:
        in_stock = int(df['in_stock'].sum())
        stock_pct = (in_stock / len(df) * 100)
        st.metric(
            "In Stock",
            f"{in_stock:,} ({stock_pct:.1f}%)",
            help="Books currently in stock"
        )

    with col5:
        categories = df['category'].nunique()
        st.metric(
            "Categories",
            f"{categories}",
            help="Number of unique categories"
        )


def show_time_analysis(df):
    """
    Show time-based analysis.

    @param {DataFrame} df - Book data
    @returns {None}
    """
    if 'scraped_at' not in df.columns or df['scraped_at'].isna().all():
        st.info("No time data available for analysis")
        return

    st.subheader("Time-based Analysis")

    col1, col2 = st.columns(2)

    with col1:
        # Books scraped over time
        df['scrape_date'] = df['scraped_at'].dt.date
        daily_counts = df.groupby('scrape_date').size().reset_index(name='count')

        fig = px.line(
            daily_counts,
            x='scrape_date',
            y='count',
            title='Books Scraped Over Time',
            labels={'scrape_date': 'Date', 'count': 'Number of Books'},
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Cumulative books
        daily_counts['cumulative'] = daily_counts['count'].cumsum()

        fig = px.area(
            daily_counts,
            x='scrape_date',
            y='cumulative',
            title='Cumulative Books Scraped',
            labels={'scrape_date': 'Date', 'cumulative': 'Total Books'}
        )
        st.plotly_chart(fig, use_container_width=True)


def show_data_table(df):
    """
    Show interactive data table.

    @param {DataFrame} df - Book data
    @returns {None}
    """
    st.subheader("Book Data Explorer")

    # Filters
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        categories = ['All'] + sorted(df['category'].unique().tolist())
        selected_category = st.selectbox("Category", categories)

    with col2:
        ratings = ['All'] + sorted(df['rating'].unique().tolist())
        selected_rating = st.selectbox("Rating", ratings)

    with col3:
        price_min = st.number_input("Min Price (£)", value=0.0, step=1.0)

    with col4:
        price_max = st.number_input("Max Price (£)", value=float(df['price'].max()), step=1.0)

    # Apply filters
    filtered_df = df.copy()

    if selected_category != 'All':
        filtered_df = filtered_df[filtered_df['category'] == selected_category]

    if selected_rating != 'All':
        filtered_df = filtered_df[filtered_df['rating'] == selected_rating]

    filtered_df = filtered_df[
        (filtered_df['price'] >= price_min) &
        (filtered_df['price'] <= price_max)
    ]

    st.write(f"Showing {len(filtered_df)} of {len(df)} books")

    # Column selection
    available_cols = ['title', 'price', 'rating', 'category', 'availability', 'upc', 'url']
    display_cols = st.multiselect(
        "Select columns to display",
        options=[col for col in available_cols if col in filtered_df.columns],
        default=['title', 'price', 'rating', 'category', 'availability']
    )

    if display_cols:
        # Display table
        st.dataframe(
            filtered_df[display_cols],
            use_container_width=True,
            height=400
        )

        # Download button
        csv = filtered_df[display_cols].to_csv(index=False)
        st.download_button(
            label="Download Filtered Data (CSV)",
            data=csv,
            file_name=f"books_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )


def main():
    """
    Main dashboard function.

    @returns {None}
    """
    # Initialize database
    init_database()

    # Header
    st.title("Books Data Pipeline Dashboard")
    st.markdown("### Comprehensive Analytics for Scraped Book Data")
    st.markdown("---")

    # Sidebar
    with st.sidebar:
        st.header("Controls")

        # Scraping section
        with st.expander("Data Collection", expanded=False):
            st.subheader("Scrape New Data")

            scrape_mode = st.radio(
                "Scraping Mode",
                ["By Category", "Sequential Pages", "With Details"]
            )

            max_pages = st.number_input(
                "Max pages",
                min_value=1,
                max_value=50,
                value=5,
                help="Number of pages to scrape"
            )

            if st.button("Start Scraping", use_container_width=True):
                if scrape_mode == "By Category":
                    scrape_data(max_pages=max_pages, by_category=True)
                else:
                    include_details = scrape_mode == "With Details"
                    scrape_data(max_pages=max_pages)
                st.rerun()

        st.markdown("---")

        # Refresh data
        if st.button("Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        # Database management
        with st.expander("Database Management", expanded=False):
            if st.button("Clear Database", use_container_width=True):
                if st.checkbox("I understand this will delete all data"):
                    db_manager.drop_tables()
                    db_manager.create_tables()
                    st.success("Database cleared")
                    st.cache_data.clear()
                    st.rerun()

        st.markdown("---")
        st.markdown("### Dashboard Info")
        st.info("""
        This dashboard provides comprehensive analytics for the scraped book data including:
        - Price analysis
        - Category insights
        - Rating correlations
        - Stock availability
        - Advanced analytics
        """)

    # Load data
    df = load_data()

    if df.empty:
        st.info("No data available. Use the sidebar to scrape data!")
        return

    # Overview metrics
    show_overview_metrics(df)
    st.markdown("---")

    # Tabs for different analyses
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "Overview",
        "Price Analysis",
        "Categories",
        "Ratings",
        "Availability",
        "Advanced Analytics",
        "Data Table"
    ])

    with tab1:
        st.markdown("### Quick Overview")

        col1, col2 = st.columns(2)

        with col1:
            # Top categories
            st.markdown("#### Top 10 Categories")
            top_cats = df['category'].value_counts().head(10)
            fig = px.bar(
                y=top_cats.index,
                x=top_cats.values,
                orientation='h',
                color=top_cats.values,
                color_continuous_scale='Greens'
            )
            fig.update_layout(showlegend=False, xaxis_title='Count', yaxis_title='')
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Price vs Rating
            st.markdown("#### Price vs Rating")
            avg_by_rating = df.groupby('rating')['price'].mean()
            fig = px.bar(
                x=avg_by_rating.index,
                y=avg_by_rating.values,
                color=avg_by_rating.values,
                color_continuous_scale='Viridis'
            )
            fig.update_layout(xaxis_title='Rating', yaxis_title='Avg Price (£)')
            st.plotly_chart(fig, use_container_width=True)

        # # Recent additions
        # if 'created_at' in df.columns:
        #     st.markdown("#### Recently Added Books")
        #     recent = df.nlargest(10, 'created_at')[['title', 'price', 'category', 'rating']]
        #     st.dataframe(recent, use_container_width=True, hide_index=True)

    with tab2:
        show_price_analysis(df)

    with tab3:
        show_category_analysis(df)

    with tab4:
        show_rating_analysis(df)

    with tab5:
        show_availability_analysis(df)

    with tab6:
        show_advanced_analytics(df)
        show_time_analysis(df)

    with tab7:
        show_data_table(df)


if __name__ == "__main__":
    main()


# """
# Streamlit dashboard for data visualization.
#
# This module creates an interactive dashboard for visualizing
# the scraped book data with advanced analytics.
#
# @module app
# @author Jeffrey Dabo
# @date 2025
# """
# import sys
# from pathlib import Path
#
# # Add project root to Python path
# project_root = Path(__file__).resolve().parent.parent
# if str(project_root) not in sys.path:
#     sys.path.insert(0, str(project_root))
#
# import streamlit as st
# import pandas as pd
# import plotly.express as px
# import plotly.graph_objects as go
# import numpy as np
# from datetime import datetime
# from loguru import logger
#
# from src.database.connection import db_manager
# from src.scraper.books_scraper import BooksScraper
#
# # Page configuration - MUST BE FIRST STREAMLIT COMMAND
# st.set_page_config(
#     page_title="Books Data Pipeline Dashboard",
#     page_icon="📚",
#     layout="wide",
#     initial_sidebar_state="expanded"
# )
#
#
# def init_database():
#     """Initialize database connection."""
#     try:
#         db_manager.initialize()
#         db_manager.create_tables()
#     except Exception as e:
#         st.error(f"Database initialization failed: {e}")
#
#
# @st.cache_data(ttl=300)
# def load_data():
#     """Load data from database with caching."""
#     try:
#         books = db_manager.get_all_books()
#         if not books:
#             return pd.DataFrame()
#
#         df = pd.DataFrame(books)
#
#         # Data cleaning
#         if 'scraped_at' in df.columns:
#             df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')
#         if 'created_at' in df.columns:
#             df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
#
#         # Extract stock quantity
#         df['stock_quantity'] = df['availability'].str.extract(r'(\d+)', expand=False).astype(float)
#         df['in_stock'] = df['availability'].str.contains('In stock', case=False, na=False)
#
#         # Clean category
#         df['category'] = df['category'].fillna('Unknown').astype(str)
#
#         # Add price bins
#         df['price_range'] = pd.cut(
#             df['price'],
#             bins=[0, 20, 40, 60, 80, 100],
#             labels=['£0-20', '£20-40', '£40-60', '£60-80', '£80+'],
#             include_lowest=True
#         )
#
#         # Ensure numeric types
#         df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0).astype(int)
#         df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
#
#         return df
#     except Exception as e:
#         st.error(f"Failed to load data: {e}")
#         logger.error(f"Data loading error: {e}")
#         return pd.DataFrame()
#
#
# def scrape_data(max_pages=None, by_category=False):
#     """Trigger scraping process."""
#     try:
#         with st.spinner("🔄 Scraping data..."):
#             scraper = BooksScraper()
#             books = scraper.scrape(max_pages=max_pages, by_category=by_category)
#             scraper.close()
#
#             if books:
#                 inserted = db_manager.insert_books_bulk(books)
#                 st.success(f"✅ Scraped {len(books)} books, inserted {inserted} new records")
#                 st.cache_data.clear()
#                 return len(books)
#             else:
#                 st.warning("⚠️ No books were scraped")
#                 return 0
#     except Exception as e:
#         st.error(f"❌ Scraping failed: {e}")
#         logger.error(f"Scraping error: {e}")
#         return 0
#
#
# def safe_plotly_chart(fig, **kwargs):
#     """Safely render plotly chart with error handling."""
#     try:
#         st.plotly_chart(fig, **kwargs)
#     except Exception as e:
#         st.error(f"Chart rendering error: {str(e)[:200]}")
#         logger.error(f"Plotly chart error: {e}")
#
#
# def show_overview_metrics(df):
#     """Display overview metrics."""
#     col1, col2, col3, col4, col5 = st.columns(5)
#
#     with col1:
#         st.metric("📚 Total Books", f"{len(df):,}")
#
#     with col2:
#         avg_price = df['price'].mean()
#         st.metric("💰 Avg Price", f"£{avg_price:.2f}")
#
#     with col3:
#         avg_rating = df['rating'].mean()
#         st.metric("⭐ Avg Rating", f"{avg_rating:.1f}/5")
#
#     with col4:
#         in_stock = int(df['in_stock'].sum())
#         stock_pct = (in_stock / len(df) * 100) if len(df) > 0 else 0
#         st.metric("📦 In Stock", f"{in_stock:,} ({stock_pct:.1f}%)")
#
#     with col5:
#         categories = df['category'].nunique()
#         st.metric("🏷️ Categories", f"{categories}")
#
#
# def show_price_analysis(df):
#     """Show price analysis section."""
#     st.subheader("💵 Price Analysis")
#
#     col1, col2 = st.columns(2)
#
#     with col1:
#         # Price distribution
#         fig = px.histogram(
#             df,
#             x='price',
#             nbins=30,
#             title='Price Distribution',
#             labels={'price': 'Price (£)'}
#         )
#         fig.add_vline(x=df['price'].mean(), line_dash="dash", line_color="red")
#         safe_plotly_chart(fig, use_container_width=True)
#
#     with col2:
#         # Price range breakdown
#         price_range_counts = df['price_range'].value_counts().sort_index()
#         fig = px.bar(
#             x=price_range_counts.index.astype(str),
#             y=price_range_counts.values,
#             title='Books by Price Range',
#             labels={'x': 'Price Range', 'y': 'Count'}
#         )
#         safe_plotly_chart(fig, use_container_width=True)
#
#
# def show_category_analysis(df):
#     """Show category analysis section."""
#     st.subheader("🏷️ Category Analysis")
#
#     col1, col2 = st.columns(2)
#
#     with col1:
#         # Top categories
#         category_counts = df['category'].value_counts().head(15)
#         fig = px.bar(
#             y=category_counts.index,
#             x=category_counts.values,
#             orientation='h',
#             title='Top 15 Categories',
#             labels={'x': 'Count', 'y': 'Category'}
#         )
#         safe_plotly_chart(fig, use_container_width=True)
#
#     with col2:
#         # Pie chart
#         top_10 = category_counts.head(10)
#         fig = px.pie(
#             values=top_10.values,
#             names=top_10.index,
#             title='Top 10 Categories',
#             hole=0.4
#         )
#         safe_plotly_chart(fig, use_container_width=True)
#
#
# def show_rating_analysis(df):
#     """Show rating analysis section."""
#     st.subheader("⭐ Rating Analysis")
#
#     col1, col2 = st.columns(2)
#
#     with col1:
#         # Rating distribution
#         rating_counts = df['rating'].value_counts().sort_index()
#         fig = px.bar(
#             x=rating_counts.index,
#             y=rating_counts.values,
#             title='Rating Distribution',
#             labels={'x': 'Rating', 'y': 'Count'},
#             text=rating_counts.values
#         )
#         fig.update_traces(textposition='outside')
#         safe_plotly_chart(fig, use_container_width=True)
#
#     with col2:
#         # Rating pie chart
#         fig = px.pie(
#             values=rating_counts.values,
#             names=[f"{i} ⭐" for i in rating_counts.index],
#             title='Rating Distribution (%)',
#             hole=0.4
#         )
#         safe_plotly_chart(fig, use_container_width=True)
#
#
# def show_data_table(df):
#     """Show interactive data table."""
#     st.subheader("📋 Book Data Explorer")
#
#     # Filters
#     col1, col2, col3, col4 = st.columns(4)
#
#     with col1:
#         categories = ['All'] + sorted(df['category'].unique().tolist())
#         selected_category = st.selectbox("Category", categories, key='cat_filter')
#
#     with col2:
#         ratings = ['All'] + sorted([int(x) for x in df['rating'].unique() if pd.notna(x)])
#         selected_rating = st.selectbox("Rating", ratings, key='rating_filter')
#
#     with col3:
#         price_min = st.number_input("Min Price (£)", value=0.0, step=1.0, key='price_min')
#
#     with col4:
#         price_max = st.number_input("Max Price (£)", value=float(df['price'].max()), step=1.0, key='price_max')
#
#     # Apply filters
#     filtered_df = df.copy()
#
#     if selected_category != 'All':
#         filtered_df = filtered_df[filtered_df['category'] == selected_category]
#
#     if selected_rating != 'All':
#         filtered_df = filtered_df[filtered_df['rating'] == selected_rating]
#
#     filtered_df = filtered_df[
#         (filtered_df['price'] >= price_min) &
#         (filtered_df['price'] <= price_max)
#         ]
#
#     st.info(f"📊 Showing {len(filtered_df):,} of {len(df):,} books")
#
#     # Column selection
#     default_cols = ['title', 'price', 'rating', 'category', 'availability']
#     available_cols = [col for col in default_cols if col in filtered_df.columns]
#
#     display_cols = st.multiselect(
#         "Select columns to display",
#         options=filtered_df.columns.tolist(),
#         default=available_cols,
#         key='col_select'
#     )
#
#     if display_cols:
#         # Limit display for performance
#         display_df = filtered_df[display_cols].head(1000).copy()
#
#         # Truncate long text
#         if 'title' in display_df.columns:
#             display_df['title'] = display_df['title'].str[:100]
#
#         st.dataframe(
#             display_df,
#             use_container_width=True,
#             height=400
#         )
#
#         # Download button
#         csv = filtered_df[display_cols].to_csv(index=False)
#         st.download_button(
#             label="📥 Download CSV",
#             data=csv,
#             file_name=f"books_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
#             mime="text/csv"
#         )
#
#
# def main():
#     """Main dashboard function."""
#     # Initialize
#     init_database()
#
#     # Header
#     st.title("📚 Books Data Pipeline Dashboard")
#     st.markdown("### Comprehensive Analytics for Scraped Book Data")
#     st.markdown("---")
#
#     # Sidebar
#     with st.sidebar:
#         st.header("⚙️ Controls")
#
#         # Scraping
#         with st.expander("🔄 Data Collection"):
#             scrape_mode = st.radio(
#                 "Mode",
#                 ["By Category", "Sequential Pages"],
#                 key='scrape_mode'
#             )
#
#             max_pages = st.number_input(
#                 "Max pages",
#                 min_value=1,
#                 max_value=50,
#                 value=5,
#                 key='max_pages'
#             )
#
#             if st.button("🔄 Start Scraping", key='start_scrape'):
#                 by_category = scrape_mode == "By Category"
#                 scrape_data(max_pages=max_pages, by_category=by_category)
#                 st.rerun()
#
#         st.markdown("---")
#
#         # Refresh
#         if st.button("🔃 Refresh Data", key='refresh_btn'):
#             st.cache_data.clear()
#             st.rerun()
#
#         # Database management
#         with st.expander("🗄️ Database"):
#             confirm = st.checkbox("I understand this deletes all data", key='confirm_delete')
#             if st.button("🗑️ Clear Database", key='clear_db', disabled=not confirm):
#                 db_manager.drop_tables()
#                 db_manager.create_tables()
#                 st.success("Database cleared")
#                 st.cache_data.clear()
#                 st.rerun()
#
#     # Load data
#     df = load_data()
#
#     if df.empty:
#         st.info("👋 No data available. Use the sidebar to scrape data!")
#         return
#
#     # Metrics
#     show_overview_metrics(df)
#     st.markdown("---")
#
#     # Tabs
#     tab1, tab2, tab3, tab4 = st.tabs([
#         "📊 Overview",
#         "💵 Price Analysis",
#         "🏷️ Categories",
#         "📋 Data Table"
#     ])
#
#     with tab1:
#         st.markdown("### 📈 Quick Overview")
#
#         col1, col2 = st.columns(2)
#
#         with col1:
#             top_cats = df['category'].value_counts().head(10)
#             fig = px.bar(
#                 y=top_cats.index,
#                 x=top_cats.values,
#                 orientation='h',
#                 title='Top 10 Categories'
#             )
#             safe_plotly_chart(fig, use_container_width=True)
#
#         with col2:
#             avg_by_rating = df.groupby('rating')['price'].mean()
#             fig = px.bar(
#                 x=avg_by_rating.index,
#                 y=avg_by_rating.values,
#                 title='Average Price by Rating'
#             )
#             safe_plotly_chart(fig, use_container_width=True)
#
#     with tab2:
#         show_price_analysis(df)
#
#     with tab3:
#         show_category_analysis(df)
#         show_rating_analysis(df)
#
#     with tab4:
#         show_data_table(df)
#
#
# if __name__ == "__main__":
#     try:
#         main()
#     except Exception as e:
#         st.error(f"Application error: {e}")
#         logger.error(f"Main error: {e}")














































# """
# Streamlit dashboard for data visualization.
#
# This module creates an interactive dashboard for visualizing
# the scraped book data with advanced analytics.
#
# @module app
# @author Jeffrey Dabo
# @date 2025
# """
# import sys
# from pathlib import Path
#
# # Add project root to Python path
# project_root = Path(__file__).resolve().parent.parent
# if str(project_root) not in sys.path:
#     sys.path.insert(0, str(project_root))
#
# import streamlit as st
# import pandas as pd
# import plotly.express as px
# import plotly.graph_objects as go
# import numpy as np
# from datetime import datetime
# from loguru import logger
#
# from src.database.connection import db_manager
# from src.scraper.books_scraper import BooksScraper
#
# # Page configuration - MUST BE FIRST
# st.set_page_config(
#     page_title="Books Data Pipeline Dashboard",
#     page_icon="📚",
#     layout="wide",
#     initial_sidebar_state="expanded"
# )
#
# # Custom CSS
# st.markdown("""
#     <style>
#     .main {
#         padding: 0rem 1rem;
#     }
#     .stMetric {
#         background-color: #f0f2f6;
#         padding: 10px;
#         border-radius: 5px;
#     }
#     div[data-testid="stMetricValue"] {
#         font-size: 24px;
#     }
#     </style>
#     """, unsafe_allow_html=True)
#
#
# def init_database():
#     """Initialize database connection."""
#     try:
#         db_manager.initialize()
#         db_manager.create_tables()
#     except Exception as e:
#         st.error(f"Database initialization failed: {e}")
#         logger.error(f"Database init error: {e}")
#
#
# @st.cache_data(ttl=300)
# def load_data():
#     """Load data from database with caching."""
#     try:
#         books = db_manager.get_all_books()
#         if not books:
#             return pd.DataFrame()
#
#         df = pd.DataFrame(books)
#
#         # Data type conversions
#         if 'scraped_at' in df.columns:
#             df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')
#         if 'created_at' in df.columns:
#             df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
#
#         # Extract stock quantity
#         df['stock_quantity'] = df['availability'].str.extract(r'(\d+)', expand=False).astype(float)
#         df['in_stock'] = df['availability'].str.contains('In stock', case=False, na=False)
#
#         # Clean category
#         df['category'] = df['category'].fillna('Unknown').astype(str)
#
#         # Add price bins
#         df['price_range'] = pd.cut(
#             df['price'],
#             bins=[0, 20, 40, 60, 80, 100],
#             labels=['£0-20', '£20-40', '£40-60', '£60-80', '£80+'],
#             include_lowest=True
#         )
#
#         # Ensure numeric types
#         df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0).astype(int)
#         df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
#
#         return df
#     except Exception as e:
#         st.error(f"Failed to load data: {e}")
#         logger.error(f"Data loading error: {e}")
#         return pd.DataFrame()
#
#
# def scrape_data(max_pages=None, by_category=False):
#     """Trigger scraping process."""
#     try:
#         with st.spinner("🔄 Scraping data... This may take a while."):
#             scraper = BooksScraper()
#             books = scraper.scrape(max_pages=max_pages, by_category=by_category)
#             scraper.close()
#
#             if books:
#                 inserted = db_manager.insert_books_bulk(books)
#                 st.success(f"✅ Scraped {len(books)} books, inserted {inserted} new records")
#                 st.cache_data.clear()
#                 return len(books)
#             else:
#                 st.warning("⚠️ No books were scraped")
#                 return 0
#     except Exception as e:
#         st.error(f"❌ Scraping failed: {e}")
#         logger.error(f"Scraping error: {e}")
#         return 0
#
#
# def show_overview_metrics(df):
#     """Display overview metrics."""
#     col1, col2, col3, col4, col5 = st.columns(5)
#
#     with col1:
#         st.metric("📚 Total Books", f"{len(df):,}")
#
#     with col2:
#         avg_price = df['price'].mean()
#         st.metric("💰 Avg Price", f"£{avg_price:.2f}")
#
#     with col3:
#         avg_rating = df['rating'].mean()
#         st.metric("⭐ Avg Rating", f"{avg_rating:.1f}/5")
#
#     with col4:
#         in_stock = int(df['in_stock'].sum())
#         stock_pct = (in_stock / len(df) * 100) if len(df) > 0 else 0
#         st.metric("📦 In Stock", f"{in_stock:,} ({stock_pct:.1f}%)")
#
#     with col5:
#         categories = df['category'].nunique()
#         st.metric("🏷️ Categories", f"{categories}")
#
#
# def show_price_analysis(df):
#     """Show price analysis section."""
#     st.subheader("💵 Price Analysis")
#
#     col1, col2 = st.columns(2)
#
#     with col1:
#         try:
#             # Price distribution histogram
#             fig = px.histogram(
#                 df,
#                 x='price',
#                 nbins=50,
#                 title='Price Distribution',
#                 labels={'price': 'Price (£)', 'count': 'Number of Books'}
#             )
#             fig.add_vline(
#                 x=df['price'].mean(),
#                 line_dash="dash",
#                 line_color="red",
#                 annotation_text=f"Mean: £{df['price'].mean():.2f}",
#                 annotation_position="top"
#             )
#             fig.add_vline(
#                 x=df['price'].median(),
#                 line_dash="dash",
#                 line_color="green",
#                 annotation_text=f"Median: £{df['price'].median():.2f}",
#                 annotation_position="bottom"
#             )
#             st.plotly_chart(fig, use_container_width=True, key='price_dist')
#         except Exception as e:
#             st.error(f"Error rendering price distribution: {str(e)[:100]}")
#
#     with col2:
#         try:
#             # Price range breakdown
#             price_range_counts = df['price_range'].value_counts().sort_index()
#             fig = px.bar(
#                 x=price_range_counts.index.astype(str),
#                 y=price_range_counts.values,
#                 title='Books by Price Range',
#                 labels={'x': 'Price Range', 'y': 'Number of Books'},
#                 text=price_range_counts.values
#             )
#             fig.update_traces(textposition='outside')
#             st.plotly_chart(fig, use_container_width=True, key='price_range')
#         except Exception as e:
#             st.error(f"Error rendering price range: {str(e)[:100]}")
#
#     col3, col4 = st.columns(2)
#
#     with col3:
#         try:
#             # Box plot by rating
#             fig = px.box(
#                 df,
#                 x='rating',
#                 y='price',
#                 title='Price Distribution by Rating',
#                 labels={'rating': 'Rating', 'price': 'Price (£)'}
#             )
#             st.plotly_chart(fig, use_container_width=True, key='price_box')
#         except Exception as e:
#             st.error(f"Error rendering box plot: {str(e)[:100]}")
#
#     with col4:
#         try:
#             # Price by category (top 10)
#             top_categories = df['category'].value_counts().head(10).index
#             df_top = df[df['category'].isin(top_categories)]
#             category_stats = df_top.groupby('category')['price'].mean().sort_values(ascending=False).reset_index()
#
#             fig = px.bar(
#                 category_stats,
#                 x='category',
#                 y='price',
#                 title='Average Price by Category (Top 10)',
#                 labels={'category': 'Category', 'price': 'Avg Price (£)'}
#             )
#             fig.update_layout(xaxis_tickangle=-45)
#             st.plotly_chart(fig, use_container_width=True, key='price_category')
#         except Exception as e:
#             st.error(f"Error rendering category price: {str(e)[:100]}")
#
#
# def show_category_analysis(df):
#     """Show category analysis section."""
#     st.subheader("🏷️ Category Analysis")
#
#     col1, col2 = st.columns(2)
#
#     with col1:
#         try:
#             # Category distribution
#             category_counts = df['category'].value_counts().head(15)
#             fig = px.bar(
#                 y=category_counts.index,
#                 x=category_counts.values,
#                 orientation='h',
#                 title='Top 15 Categories by Book Count',
#                 labels={'x': 'Number of Books', 'y': 'Category'},
#                 text=category_counts.values
#             )
#             fig.update_traces(textposition='outside')
#             fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
#             st.plotly_chart(fig, use_container_width=True, key='cat_dist')
#         except Exception as e:
#             st.error(f"Error rendering category distribution: {str(e)[:100]}")
#
#     with col2:
#         try:
#             # Category pie chart
#             top_10_counts = category_counts.head(10)
#             fig = px.pie(
#                 values=top_10_counts.values,
#                 names=top_10_counts.index,
#                 title='Top 10 Categories Distribution',
#                 hole=0.4
#             )
#             fig.update_layout(height=500)
#             st.plotly_chart(fig, use_container_width=True, key='cat_pie')
#         except Exception as e:
#             st.error(f"Error rendering category pie: {str(e)[:100]}")
#
#     # Category statistics table
#     st.markdown("#### 📊 Category Statistics")
#
#     try:
#         category_stats = df.groupby('category').agg({
#             'price': ['mean', 'min', 'max'],
#             'rating': 'mean',
#             'in_stock': 'sum',
#             'id': 'count'
#         }).round(2)
#
#         category_stats.columns = ['Avg Price', 'Min Price', 'Max Price', 'Avg Rating', 'In Stock', 'Total Books']
#         category_stats = category_stats.sort_values('Total Books', ascending=False).head(20).reset_index()
#
#         st.dataframe(
#             category_stats,
#             use_container_width=True,
#             hide_index=True
#         )
#     except Exception as e:
#         st.error(f"Error rendering category stats: {str(e)[:100]}")
#
#
# def show_rating_analysis(df):
#     """Show rating analysis section."""
#     st.subheader("⭐ Rating Analysis")
#
#     col1, col2 = st.columns(2)
#
#     with col1:
#         try:
#             # Rating distribution
#             rating_counts = df['rating'].value_counts().sort_index()
#             fig = px.bar(
#                 x=rating_counts.index,
#                 y=rating_counts.values,
#                 title='Rating Distribution',
#                 labels={'x': 'Rating', 'y': 'Number of Books'},
#                 text=rating_counts.values
#             )
#             fig.update_traces(textposition='outside')
#             st.plotly_chart(fig, use_container_width=True, key='rating_dist')
#         except Exception as e:
#             st.error(f"Error rendering rating distribution: {str(e)[:100]}")
#
#     with col2:
#         try:
#             # Rating pie chart
#             fig = px.pie(
#                 values=rating_counts.values,
#                 names=[f"{int(i)} ⭐" for i in rating_counts.index],
#                 title='Rating Distribution (%)',
#                 hole=0.4
#             )
#             st.plotly_chart(fig, use_container_width=True, key='rating_pie')
#         except Exception as e:
#             st.error(f"Error rendering rating pie: {str(e)[:100]}")
#
#     col3, col4 = st.columns(2)
#
#     with col3:
#         try:
#             # Scatter: Price vs Rating
#             sample_size = min(500, len(df))
#             sample_df = df.sample(sample_size) if len(df) > sample_size else df
#
#             fig = px.scatter(
#                 sample_df,
#                 x='rating',
#                 y='price',
#                 title=f'Price vs Rating Correlation (Sample: {sample_size})',
#                 labels={'rating': 'Rating', 'price': 'Price (£)'},
#                 trendline='ols',
#                 opacity=0.6
#             )
#             st.plotly_chart(fig, use_container_width=True, key='price_rating_scatter')
#         except Exception as e:
#             st.error(f"Error rendering scatter plot: {str(e)[:100]}")
#
#     with col4:
#         try:
#             # Average price by rating
#             rating_price = df.groupby('rating')['price'].mean().reset_index()
#
#             fig = px.line(
#                 rating_price,
#                 x='rating',
#                 y='price',
#                 title='Average Price by Rating',
#                 labels={'rating': 'Rating', 'price': 'Avg Price (£)'},
#                 markers=True
#             )
#             st.plotly_chart(fig, use_container_width=True, key='avg_price_rating')
#         except Exception as e:
#             st.error(f"Error rendering line chart: {str(e)[:100]}")
#
#
# def show_availability_analysis(df):
#     """Show availability and stock analysis."""
#     st.subheader("📦 Availability & Stock Analysis")
#
#     col1, col2 = st.columns(2)
#
#     with col1:
#         try:
#             # Stock status
#             stock_status = df['in_stock'].value_counts()
#             labels = ['In Stock' if x else 'Out of Stock' for x in stock_status.index]
#
#             fig = px.pie(
#                 values=stock_status.values,
#                 names=labels,
#                 title='Stock Status',
#                 color_discrete_sequence=['#2ecc71', '#e74c3c'],
#                 hole=0.3
#             )
#             st.plotly_chart(fig, use_container_width=True, key='stock_status')
#         except Exception as e:
#             st.error(f"Error rendering stock status: {str(e)[:100]}")
#
#     with col2:
#         try:
#             # Stock quantity distribution
#             df_with_qty = df[df['stock_quantity'].notna()]
#             if not df_with_qty.empty:
#                 fig = px.histogram(
#                     df_with_qty,
#                     x='stock_quantity',
#                     nbins=20,
#                     title='Stock Quantity Distribution',
#                     labels={'stock_quantity': 'Stock Quantity', 'count': 'Number of Books'}
#                 )
#                 st.plotly_chart(fig, use_container_width=True, key='stock_qty')
#             else:
#                 st.info("📊 No stock quantity data available")
#         except Exception as e:
#             st.error(f"Error rendering stock quantity: {str(e)[:100]}")
#
#     # Category-wise stock analysis
#     st.markdown("#### 📊 Stock by Category (Top 15)")
#
#     try:
#         category_stock = df.groupby('category').agg({
#             'in_stock': 'sum',
#             'id': 'count'
#         }).reset_index()
#         category_stock.columns = ['Category', 'In Stock', 'Total']
#         category_stock['Out of Stock'] = category_stock['Total'] - category_stock['In Stock']
#         category_stock = category_stock.sort_values('Total', ascending=False).head(15)
#
#         fig = go.Figure()
#         fig.add_trace(go.Bar(
#             name='In Stock',
#             x=category_stock['Category'],
#             y=category_stock['In Stock'],
#             marker_color='lightgreen'
#         ))
#         fig.add_trace(go.Bar(
#             name='Out of Stock',
#             x=category_stock['Category'],
#             y=category_stock['Out of Stock'],
#             marker_color='lightcoral'
#         ))
#         fig.update_layout(
#             barmode='stack',
#             xaxis_title='Category',
#             yaxis_title='Number of Books',
#             xaxis_tickangle=-45,
#             height=500
#         )
#         st.plotly_chart(fig, use_container_width=True, key='cat_stock')
#     except Exception as e:
#         st.error(f"Error rendering category stock: {str(e)[:100]}")
#
#
# def show_advanced_analytics(df):
#     """Show advanced analytics and correlations."""
#     st.subheader("🔬 Advanced Analytics")
#
#     # Correlation heatmap
#     st.markdown("#### 📈 Price Correlation Analysis")
#
#     try:
#         numeric_cols = ['price', 'rating', 'price_excl_tax', 'price_incl_tax', 'tax']
#         available_cols = [col for col in numeric_cols if col in df.columns and df[col].notna().any()]
#
#         if len(available_cols) >= 2:
#             corr_matrix = df[available_cols].corr()
#
#             fig = px.imshow(
#                 corr_matrix,
#                 text_auto='.2f',
#                 title='Correlation Heatmap',
#                 color_continuous_scale='RdBu_r',
#                 aspect='auto'
#             )
#             fig.update_layout(height=500)
#             st.plotly_chart(fig, use_container_width=True, key='corr_heatmap')
#     except Exception as e:
#         st.error(f"Error rendering correlation heatmap: {str(e)[:100]}")
#
#     col1, col2 = st.columns(2)
#
#     with col1:
#         # Top expensive books
#         st.markdown("#### 💎 Top 10 Most Expensive Books")
#         try:
#             top_expensive = df.nlargest(10, 'price')[['title', 'price', 'category', 'rating']].copy()
#             top_expensive['title'] = top_expensive['title'].str[:50]
#             st.dataframe(top_expensive, use_container_width=True, hide_index=True)
#         except Exception as e:
#             st.error(f"Error: {str(e)[:100]}")
#
#     with col2:
#         # Top rated books
#         st.markdown("#### 🌟 Top Rated Books (5 Stars)")
#         try:
#             top_rated = df[df['rating'] == 5].nlargest(10, 'price')[['title', 'price', 'category', 'rating']].copy()
#             if not top_rated.empty:
#                 top_rated['title'] = top_rated['title'].str[:50]
#                 st.dataframe(top_rated, use_container_width=True, hide_index=True)
#             else:
#                 st.info("No 5-star rated books found")
#         except Exception as e:
#             st.error(f"Error: {str(e)[:100]}")
#
#     # Value analysis
#     st.markdown("#### 💡 Best Value Books (High Rating, Low Price)")
#
#     try:
#         df_value = df.copy()
#         df_value['value_score'] = df_value['rating'] / (df_value['price'] + 1)
#         best_value = df_value.nlargest(15, 'value_score')[
#             ['title', 'price', 'rating', 'category', 'value_score']].copy()
#
#         fig = px.scatter(
#             best_value,
#             x='price',
#             y='rating',
#             size='value_score',
#             hover_data=['title', 'category'],
#             title='Best Value Books (Bubble size = Value Score)',
#             labels={'price': 'Price (£)', 'rating': 'Rating'},
#             size_max=60
#         )
#         fig.update_layout(height=500)
#         st.plotly_chart(fig, use_container_width=True, key='value_scatter')
#
#         # Show table
#         best_value['title'] = best_value['title'].str[:60]
#         st.dataframe(best_value, use_container_width=True, hide_index=True)
#     except Exception as e:
#         st.error(f"Error rendering value analysis: {str(e)[:100]}")
#
#
# def show_time_analysis(df):
#     """Show time-based analysis."""
#     if 'scraped_at' not in df.columns or df['scraped_at'].isna().all():
#         st.info("⏰ No time data available for analysis")
#         return
#
#     st.subheader("📅 Time-based Analysis")
#
#     col1, col2 = st.columns(2)
#
#     with col1:
#         try:
#             # Books scraped over time
#             df['scrape_date'] = df['scraped_at'].dt.date
#             daily_counts = df.groupby('scrape_date').size().reset_index(name='count')
#
#             fig = px.line(
#                 daily_counts,
#                 x='scrape_date',
#                 y='count',
#                 title='Books Scraped Over Time',
#                 labels={'scrape_date': 'Date', 'count': 'Number of Books'},
#                 markers=True
#             )
#             st.plotly_chart(fig, use_container_width=True, key='time_line')
#         except Exception as e:
#             st.error(f"Error rendering timeline: {str(e)[:100]}")
#
#     with col2:
#         try:
#             # Cumulative books
#             daily_counts['cumulative'] = daily_counts['count'].cumsum()
#
#             fig = px.area(
#                 daily_counts,
#                 x='scrape_date',
#                 y='cumulative',
#                 title='Cumulative Books Scraped',
#                 labels={'scrape_date': 'Date', 'cumulative': 'Total Books'}
#             )
#             st.plotly_chart(fig, use_container_width=True, key='time_cumulative')
#         except Exception as e:
#             st.error(f"Error rendering cumulative chart: {str(e)[:100]}")
#
#
# def show_data_table(df):
#     """Show interactive data table."""
#     st.subheader("📋 Book Data Explorer")
#
#     # Filters
#     col1, col2, col3, col4 = st.columns(4)
#
#     with col1:
#         categories = ['All'] + sorted(df['category'].unique().tolist())
#         selected_category = st.selectbox("Category", categories, key='cat_filter')
#
#     with col2:
#         ratings = ['All'] + sorted([int(x) for x in df['rating'].unique() if pd.notna(x)])
#         selected_rating = st.selectbox("Rating", ratings, key='rating_filter')
#
#     with col3:
#         price_min = st.number_input("Min Price (£)", value=0.0, step=1.0, key='price_min')
#
#     with col4:
#         price_max = st.number_input("Max Price (£)", value=float(df['price'].max()), step=1.0, key='price_max')
#
#     # Apply filters
#     filtered_df = df.copy()
#
#     if selected_category != 'All':
#         filtered_df = filtered_df[filtered_df['category'] == selected_category]
#
#     if selected_rating != 'All':
#         filtered_df = filtered_df[filtered_df['rating'] == selected_rating]
#
#     filtered_df = filtered_df[
#         (filtered_df['price'] >= price_min) &
#         (filtered_df['price'] <= price_max)
#         ]
#
#     st.info(f"📊 Showing {len(filtered_df):,} of {len(df):,} books")
#
#     # Column selection
#     default_cols = ['title', 'price', 'rating', 'category', 'availability']
#     available_cols = [col for col in default_cols if col in filtered_df.columns]
#
#     display_cols = st.multiselect(
#         "Select columns to display",
#         options=filtered_df.columns.tolist(),
#         default=available_cols,
#         key='col_select'
#     )
#
#     if display_cols:
#         # Display table (limit for performance)
#         display_df = filtered_df[display_cols].head(1000).copy()
#
#         # Truncate long strings
#         for col in display_df.select_dtypes(include=['object']).columns:
#             if col in display_df.columns:
#                 display_df[col] = display_df[col].astype(str).str[:100]
#
#         st.dataframe(
#             display_df,
#             use_container_width=True,
#             height=400
#         )
#
#         # Download button
#         csv = filtered_df[display_cols].to_csv(index=False)
#         st.download_button(
#             label="📥 Download Filtered Data (CSV)",
#             data=csv,
#             file_name=f"books_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
#             mime="text/csv",
#             key='download_csv'
#         )
#
#
# def main():
#     """Main dashboard function."""
#     # Initialize
#     init_database()
#
#     # Header
#     st.title("📚 Books Data Pipeline Dashboard")
#     st.markdown("### Comprehensive Analytics for Scraped Book Data")
#     st.markdown("---")
#
#     # Sidebar
#     with st.sidebar:
#         st.header("⚙️ Controls")
#
#         # Scraping
#         with st.expander("🔄 Data Collection", expanded=False):
#             st.subheader("Scrape New Data")
#
#             scrape_mode = st.radio(
#                 "Scraping Mode",
#                 ["By Category", "Sequential Pages"],
#                 key='scrape_mode'
#             )
#
#             max_pages = st.number_input(
#                 "Max pages",
#                 min_value=1,
#                 max_value=50,
#                 value=5,
#                 key='max_pages'
#             )
#
#             if st.button("🔄 Start Scraping", use_container_width=True, key='start_scrape'):
#                 by_category = scrape_mode == "By Category"
#                 scrape_data(max_pages=max_pages, by_category=by_category)
#                 st.rerun()
#
#         st.markdown("---")
#
#         # Refresh
#         if st.button("🔃 Refresh Data", use_container_width=True, key='refresh_btn'):
#             st.cache_data.clear()
#             st.rerun()
#
#         # Database management
#         with st.expander("🗄️ Database Management", expanded=False):
#             confirm = st.checkbox("I understand this will delete all data", key='confirm_delete')
#             if st.button("🗑️ Clear Database", use_container_width=True, key='clear_db', disabled=not confirm):
#                 db_manager.drop_tables()
#                 db_manager.create_tables()
#                 st.success("Database cleared")
#                 st.cache_data.clear()
#                 st.rerun()
#
#         st.markdown("---")
#         st.markdown("### 📊 Dashboard Info")
#         st.info("""
#         **Features:**
#         - 📈 Price Analysis
#         - 🏷️ Category Insights
#         - ⭐ Rating Correlations
#         - 📦 Stock Availability
#         - 🔬 Advanced Analytics
#         - 📅 Time Trends
#         """)
#
#     # Load data
#     df = load_data()
#
#     if df.empty:
#         st.info("👋 No data available. Use the sidebar to scrape data!")
#         st.markdown("""
#         **Quick Start:**
#         1. Click '🔄 Data Collection' in the sidebar
#         2. Select 'By Category' mode
#         3. Set max pages (e.g., 5)
#         4. Click 'Start Scraping'
#         """)
#         return
#
#     # Metrics
#     show_overview_metrics(df)
#     st.markdown("---")
#
#     # Tabs
#     tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
#         "📊 Overview",
#         "💵 Price Analysis",
#         "🏷️ Categories",
#         "⭐ Ratings",
#         "📦 Availability",
#         "🔬 Advanced Analytics",
#         "📋 Data Table"
#     ])
#
#     with tab1:
#         st.markdown("### 📈 Quick Overview")
#
#         col1, col2 = st.columns(2)
#
#         with col1:
#             try:
#                 top_cats = df['category'].value_counts().head(10)
#                 fig = px.bar(
#                     y=top_cats.index,
#                     x=top_cats.values,
#                     orientation='h',
#                     title='Top 10 Categories',
#                     labels={'x': 'Count', 'y': 'Category'}
#                 )
#                 st.plotly_chart(fig, use_container_width=True, key='overview_cats')
#             except Exception as e:
#                 st.error(f"Error: {str(e)[:100]}")
#
#         with col2:
#             try:
#                 avg_by_rating = df.groupby('rating')['price'].mean()
#                 fig = px.bar(
#                     x=avg_by_rating.index,
#                     y=avg_by_rating.values,
#                     title='Average Price by Rating',
#                     labels={'x': 'Rating', 'y': 'Avg Price (£)'}
#                 )
#                 st.plotly_chart(fig, use_container_width=True, key='overview_rating')
#             except Exception as e:
#                 st.error(f"Error: {str(e)[:100]}")
#
#         # # Recent books
#         # if 'created_at' in df.columns and not df['created_at'].isna().all():
#         #     st.markdown("#### 🆕 Recently Added Books")
#         #     try:
#         #         recent = df.nlargest(10, 'created_at')[['title', 'price', 'category', 'rating']].copy()
#         #         recent['title'] = recent['title'].str[:80]
#         #         st.dataframe(recent, use_container_width=True, hide_index=True)
#         #     except Exception as e:
#         #         st.error(f"Error: {str(e)[:100]}")
#
#     with tab2:
#         show_price_analysis(df)
#
#     with tab3:
#         show_category_analysis(df)
#
#     with tab4:
#         show_rating_analysis(df)
#
#     with tab5:
#         show_availability_analysis(df)
#
#     with tab6:
#         show_advanced_analytics(df)
#         show_time_analysis(df)
#
#     with tab7:
#         show_data_table(df)
#
#
# if __name__ == "__main__":
#     try:
#         main()
#     except Exception as e:
#         st.error(f"💥 Application error: {e}")
#         logger.error(f"Main application error: {e}", exc_info=True)
#         st.info("Try refreshing the page or clearing your browser cache.")