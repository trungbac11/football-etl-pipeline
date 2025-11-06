from contextlib import contextmanager
from datetime import datetime
import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import os
import plotly.graph_objects as go
import plotly.express as px
from dotenv import load_dotenv

env_path = "../env"

load_dotenv(env_path)

PSQL_CONFIG = {
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

def get_connection():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "de_psql"),
        port=PSQL_CONFIG["port"],
        dbname=PSQL_CONFIG["database"],
        user=PSQL_CONFIG["user"],
        password=PSQL_CONFIG["password"]
    )
    return conn

def query_data(table_name):
    conn = get_connection()
    query = f"SELECT * FROM analysis.{table_name}"
    t = pd.read_sql(query, conn)
    conn.close()
    return t

table = ["leagueseason", "teamseason", "playerseason"]

leagues = query_data(table[0])
team = query_data(table[1])
player = query_data(table[2])


image_urls = {
    "Manchester United": "./image/mu.png",
    "Chelsea": "./image/chel.png",
    "Manchester City": "./image/mc.png",
    "Southampton": "./image/sou.png",
    "Arsenal": "./image/ars.png",
    "Crystal Palace": "./image/crys.png",
    "Hull": "./image/hull.png",
    "Sunderland": "./image/sun.png",
    "Burnley": "./image/burnley.png",
    "Aston Villa": "./image/aston.png",
    "Queens Park Rangers": "./image/queen.png",
    "Liverpool": "./image/liv.png",
    "Huddersfield": "./image/hud.png",
    "Bournemouth": "./image/bou.png",
    "Everton": "./image/ever.png",
    "West Bromwich Albion": "./image/wba.png",
    "Newcastle United": "./image/ncs.png",
    "Leicester": "./image/lei.png",
    "West Ham": "./image/wh.png",
    "Swansea": "./image/swan.png",
    "Tottenham": "./image/tot.png",
    "Stoke": "./image/stoke.png",
    "Brighton": "./image/bri.png",
    "Cardiff": "./image/car.png",
    "Leeds": "./image/leed.png",
    "Wolverhampton Wanderers": "./image/wol.png",
    "Sheffield United": "./image/she.png",
    "Fulham": "./image/fullham.png",
    "Watford": "./image/wat.png",
    "Middlesbrough": "./image/mid.png",
}

team = team.sort_values(by=['point', 'goals_difference'], ascending=False)

season_mapping = {
    '2013/2014': 2014,
    '2014/2015': 2015,
    '2015/2016': 2016,
    '2016/2017': 2017,
    '2017/2018': 2018,
    '2018/2019': 2019,
    '2019/2020': 2020
}

st.title("Football Searching App")

page = st.selectbox("Chọn trang:", ["Tổng quan", "Bảng xếp hạng", "Thông tin cầu thủ", "So sánh đội bóng"])

if page == "Tổng quan":
    st.header("📊 Tổng quan các giải đấu")
    
    # Thống kê tổng quan
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        total_leagues = leagues['name'].nunique()
        st.metric("Số giải đấu", total_leagues)
    with col2:
        total_teams = team['name'].nunique()
        st.metric("Số đội bóng", total_teams)
    with col3:
        total_players = player['name'].nunique()
        st.metric("Số cầu thủ", total_players)
    with col4:
        total_seasons = leagues['season'].nunique()
        st.metric("Số mùa giải", total_seasons)
    
    # Biểu đồ Goals per Game
    fig = px.bar(leagues, x="name", y="goalPerGame", color="name", barmode="stack",
                 facet_col="season", 
                 labels={"name": "League", "goalPerGame": "Goals per Game"})
    fig.update_layout(showlegend=False, title='Goals per Game by League and Season')
    st.plotly_chart(fig)

    # Cards statistics
    cards = leagues[['name', 'season', 'yellowCards', 'redCards', 'fouls']]
    
    col1, col2 = st.columns(2)
    with col1:
        fig = px.line(cards, x='season', y='fouls', color='name')
        fig.update_layout(title='Fouls by League', xaxis_title='Season', yaxis_title='Fouls', legend_title='League')
        st.plotly_chart(fig)
        
        fig = px.line(cards, x='season', y='yellowCards', color='name')
        fig.update_layout(title='Yellow Cards by League', xaxis_title='Season', yaxis_title='Yellow Cards', legend_title='League')
        st.plotly_chart(fig)
    
    with col2:
        fig = px.line(cards, x='season', y='redCards', color='name')
        fig.update_layout(title='Red Cards by League', xaxis_title='Season', yaxis_title='Red Cards', legend_title='League')
        st.plotly_chart(fig)
        
        # Hiệu suất tấn công
        if 'shotPerGame' in leagues.columns:
            fig = px.line(leagues, x='season', y='shotPerGame', color='name')
            fig.update_layout(title='Shots per Game by League', xaxis_title='Season', yaxis_title='Shots per Game', legend_title='League')
            st.plotly_chart(fig)

elif page == "Bảng xếp hạng":
    st.header("🏆 Bảng xếp hạng các giải đấu")

    col1, col2 = st.columns(2)
    with col1:
        season_display = st.selectbox("Chọn mùa giải", list(season_mapping.keys()))
    with col2:
        league = st.selectbox("Chọn giải đấu", team['league'].unique())

    selected_year = season_mapping[season_display]
    filtered_data = team[(team['season'] == selected_year) & (team['league'] == league)].copy()
    filtered_data = filtered_data.sort_values(by=['point', 'goals_difference', 'goals'], ascending=[False, False, False])
    filtered_data['rank'] = range(1, len(filtered_data) + 1)

    st.write(f"## Bảng xếp hạng {league} - {season_display}")

    # Hiển thị bảng xếp hạng với styling
    for i, row in filtered_data.iterrows():
        # Xác định màu nền dựa trên vị trí
        bg_color = "#f0f8ff" if row['rank'] <= 4 else "#fff0f5" if row['rank'] > len(filtered_data) - 3 else "#f9f9f9"
        
        with st.container():
            st.markdown(f"""<div style="background-color: {bg_color}; padding: 10px; border-radius: 10px; margin: 5px 0; border: 1px solid #ddd">""", unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns([1, 8, 2])
            with col1:
                st.markdown(f"**#{row['rank']}**")
            with col2:
                col2_1, col2_2 = st.columns([1, 10])
                with col2_1:
                    if row['name'] in image_urls:
                        st.image(image_urls[row['name']], width=40)
                with col2_2:
                    st.write(f"**{row['name']}**")
            with col3:
                st.write(f"**{row['point']} pts**")
            
            # Thống kê chi tiết - Sử dụng các cột có sẵn
            col_stats = st.columns(7)
            with col_stats[0]:
                st.write(f"**{row['match']}**")
                st.caption("Trận")
            with col_stats[1]:
                st.write(f"**{row['win']}**")
                st.caption("Thắng")
            with col_stats[2]:
                st.write(f"**{row['draw']}**")
                st.caption("Hòa")
            with col_stats[3]:
                st.write(f"**{row['lose']}**")
                st.caption("Thua")
            with col_stats[4]:
                st.write(f"**{row['goals']}**")
                st.caption("Bàn")
            with col_stats[5]:
                # Tính goals_against từ goals và goals_difference nếu cần
                goals_against = row['goals'] - row['goals_difference']
                st.write(f"**{goals_against}**")
                st.caption("Thủng lưới")
            with col_stats[6]:
                difference_color = "green" if row['goals_difference'] > 0 else "red" if row['goals_difference'] < 0 else "gray"
                st.write(f"**:{difference_color}[{row['goals_difference']}]**")
                st.caption("HS")
            
            st.markdown("</div>", unsafe_allow_html=True)

    # Phân loại vị trí
    st.subheader("Phân loại giải đấu")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.success("**UEFA Champions League Group Stage:**")
        champions_league_teams = filtered_data.head(4)['name'].tolist()
        for i, team_name in enumerate(champions_league_teams, 1):
            st.write(f"{i}. {team_name}")
    
    with col2:
        st.info("**UEFA Europa League Group Stage:**")
        europa_league_teams = filtered_data.iloc[4:6]['name'].tolist()
        for i, team_name in enumerate(europa_league_teams, 5):
            st.write(f"{i}. {team_name}")
    
    with col3:
        st.error("**Xuống hạng:**")
        relegated_teams = filtered_data.tail(3)['name'].tolist()
        for i, team_name in enumerate(relegated_teams, len(filtered_data)-2):
            st.write(f"{i}. {team_name}")

    # Biểu đồ hiệu suất đội bóng
    st.subheader("Hiệu suất các đội bóng")
    
    top_teams = filtered_data.head(10)
    fig = go.Figure(data=[
        go.Bar(name='Thắng', x=top_teams['name'], y=top_teams['win'], marker_color='#2E8B57'),
        go.Bar(name='Hòa', x=top_teams['name'], y=top_teams['draw'], marker_color='#FFD700'),
        go.Bar(name='Thua', x=top_teams['name'], y=top_teams['lose'], marker_color='#DC143C')
    ])
    fig.update_layout(barmode='stack', title='Kết quả thi đấu (Top 10 đội)')
    st.plotly_chart(fig)

elif page == "Thông tin cầu thủ":
    st.header("👤 Thống kê cầu thủ")
    
    tab1, tab2, tab3 = st.tabs(["Top cầu thủ", "Thống kê chi tiết", "So sánh cầu thủ"])
    
    with tab1:
        st.subheader("Top cầu thủ theo chỉ số")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            top_goals = player.groupby('name')['goals'].sum().nlargest(10)
            fig = px.bar(x=top_goals.values, y=top_goals.index, orientation='h',
                        title='Top 10 cầu thủ ghi bàn nhiều nhất',
                        labels={'x': 'Số bàn thắng', 'y': 'Cầu thủ'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            top_assists = player.groupby('name')['assists'].sum().nlargest(10)
            fig = px.bar(x=top_assists.values, y=top_assists.index, orientation='h',
                        title='Top 10 cầu thủ kiến tạo nhiều nhất',
                        labels={'x': 'Số kiến tạo', 'y': 'Cầu thủ'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col3:
            top_players = player.groupby('name').agg({'goals': 'sum', 'assists': 'sum'})
            top_players['total_contributions'] = top_players['goals'] + top_players['assists']
            top_contributors = top_players.nlargest(10, 'total_contributions')
            fig = px.bar(x=top_contributors['total_contributions'], y=top_contributors.index, orientation='h',
                        title='Top 10 cầu thủ đóng góp nhiều nhất (Bàn + Kiến tạo)',
                        labels={'x': 'Tổng đóng góp', 'y': 'Cầu thủ'})
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Thống kê chi tiết cầu thủ")
        
        players = st.selectbox('Chọn cầu thủ:', player['name'].unique())
        player_data = player[player['name'] == players].sort_values('season')
        
        if not player_data.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Tổng bàn thắng", int(player_data['goals'].sum()))
                st.metric("Tổng kiến tạo", int(player_data['assists'].sum()))
                if 'keyPasses' in player_data.columns:
                    st.metric("Tổng đường chuyền quan trọng", int(player_data['keyPasses'].sum()))
                else:
                    st.metric("Tổng đường chuyền", "N/A")
            
            with col2:
                st.metric("Tổng cú sút", int(player_data['shots'].sum()))
                if player_data['shots'].sum() > 0:
                    conversion_rate = (player_data['goals'].sum() / player_data['shots'].sum()) * 100
                    st.metric("Hiệu suất ghi bàn", f"{conversion_rate:.1f}%")
                else:
                    st.metric("Hiệu suất ghi bàn", "0%")
                st.metric("Số mùa giải", player_data['season'].nunique())
            
            # Biểu đồ hiệu suất qua các mùa
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=player_data['season'], y=player_data['goals'], 
                                   mode='lines+markers', name='Bàn thắng', line=dict(color='red')))
            fig.add_trace(go.Scatter(x=player_data['season'], y=player_data['assists'], 
                                   mode='lines+markers', name='Kiến tạo', line=dict(color='blue')))
            fig.update_layout(title=f'Hiệu suất của {players} qua các mùa giải',
                            xaxis_title='Mùa giải', yaxis_title='Số lượng')
            st.plotly_chart(fig)
            
            # Phân bố cú sút và bàn thắng
            fig = px.bar(player_data, x='season', y=['shots', 'goals'],
                        title=f'Phân bố cú sút và bàn thắng của {players}',
                        labels={'value': 'Số lượng', 'variable': 'Chỉ số'})
            st.plotly_chart(fig)
            
        else:
            st.warning("Không có dữ liệu cho cầu thủ này.")
    
    with tab3:
        st.subheader("So sánh cầu thủ")
        
        col1, col2 = st.columns(2)
        with col1:
            player1 = st.selectbox('Chọn cầu thủ thứ nhất:', player['name'].unique(), key='player1')
        with col2:
            player2 = st.selectbox('Chọn cầu thủ thứ hai:', player['name'].unique(), key='player2')
        
        if player1 and player2:
            p1_data = player[player['name'] == player1].groupby('name').agg({
                'goals': 'sum', 'assists': 'sum', 'shots': 'sum'
            }).reset_index()
            
            p2_data = player[player['name'] == player2].groupby('name').agg({
                'goals': 'sum', 'assists': 'sum', 'shots': 'sum'
            }).reset_index()
            
            comparison_data = pd.concat([p1_data, p2_data])
            
            metrics = ['goals', 'assists', 'shots']
            fig = go.Figure()
            
            for metric in metrics:
                fig.add_trace(go.Bar(name=metric, x=comparison_data['name'], y=comparison_data[metric]))
            
            fig.update_layout(barmode='group', title='So sánh chỉ số cầu thủ')
            st.plotly_chart(fig)

elif page == "So sánh đội bóng":
    st.header("🔍 So sánh đội bóng")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        season_comp = st.selectbox("Mùa giải", list(season_mapping.keys()), key='season_comp')
    with col2:
        league_comp = st.selectbox("Giải đấu", team['league'].unique(), key='league_comp')
    with col3:
        teams = st.multiselect("Chọn các đội để so sánh", 
                              team[(team['season'] == season_mapping[season_comp]) & 
                                   (team['league'] == league_comp)]['name'].unique())
    
    if len(teams) >= 2:
        comparison_data = team[(team['season'] == season_mapping[season_comp]) & 
                              (team['league'] == league_comp) & 
                              (team['name'].isin(teams))]
        
        # Sử dụng các cột có sẵn cho biểu đồ radar
        available_columns = ['point', 'goals', 'goals_difference', 'win', 'draw', 'lose', 'match']
        categories = [col for col in available_columns if col in comparison_data.columns]
        
        if len(categories) >= 3:  # Cần ít nhất 3 categories cho radar chart
            fig = go.Figure()
            
            for i, row in comparison_data.iterrows():
                values = [row[cat] for cat in categories]
                fig.add_trace(go.Scatterpolar(
                    r=values,
                    theta=categories,
                    fill='toself',
                    name=row['name']
                ))
            
            fig.update_layout(
                polar=dict(
                    radialaxis=dict(visible=True)
                ),
                title="So sánh hiệu suất các đội bóng"
            )
            st.plotly_chart(fig)
        else:
            st.warning("Không đủ dữ liệu để tạo biểu đồ radar")
        
        # Biểu đồ cột so sánh
        metrics_to_compare = ['point', 'goals', 'goals_difference', 'win', 'draw', 'lose']
        for metric in metrics_to_compare:
            if metric in comparison_data.columns:
                fig = px.bar(comparison_data, x='name', y=metric, title=f'So sánh {metric}')
                st.plotly_chart(fig)
    
    else:
        st.info("Vui lòng chọn ít nhất 2 đội bóng để so sánh.")