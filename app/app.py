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

st.title("⚽ Statistics of European Leagues ")

page = st.selectbox("Chọn trang:", ["Tổng quan", "Bảng xếp hạng", "Thông tin cầu thủ"])
if page == "Tổng quan":
    cards = leagues[['name', 'season', 'yellowCards', 'redCards', 'fouls']]
    fig = px.bar(leagues, x="name", y="goalPerGame", color="name", barmode="stack",
                    facet_col="season", 
                    labels={"name": "League", "goals/games": "GPG"})
    fig.update_layout(showlegend=False, title='Goals per Game')
    st.plotly_chart(fig)

    fig = px.line(cards, x='season', y='fouls', color='name')
    fig.update_layout(title='Fouls of leagues', xaxis_title='Season', yaxis_title='Fouls', legend_title='League')
    st.plotly_chart(fig)
    
    fig = px.line(cards, x='season', y='redCards', color='name')
    fig.update_layout(title='Red Cards of leagues', xaxis_title='Season', yaxis_title='Red Cards', legend_title='League')
    st.plotly_chart(fig)

    fig = px.line(cards, x='season', y='yellowCards', color='name')
    fig.update_layout(title='Yellow Cards of leagues', xaxis_title='Season', yaxis_title='Yellow Cards', legend_title='League')
    st.plotly_chart(fig)

# Nội dung trang bảng xếp hạng
elif page == "Bảng xếp hạng":

    col1, col2 = st.columns(2)
    with col1:
        season_display = st.selectbox("Chọn mùa giải", list(season_mapping.keys()))
    with col2:
        league = st.selectbox("Chọn giải đấu", team['league'].unique())

    selected_year = season_mapping[season_display]

    filtered_data = team[(team['season'] == selected_year) & (team['league'] == league)]

    st.write(f"Bảng xếp hạng mùa giải {season_display} - {league}")

    for i, row in filtered_data.iterrows():
        col1, col2 = st.columns([1, 10])
        with col1:
            # Hiển thị hình ảnh logo nếu có, nếu không có thì bỏ qua
            if row['name'] in image_urls:
                st.image(image_urls[row['name']], width=50)  # Giảm kích thước hình ảnh
        with col2:
            st.write(f"**{row['name']}**")
            st.write(f"Match: {row['match']} | Win: {row['win']} | Draw: {row['draw']} | Lose: {row['lose']} | Goals: {row['goals']} | Goals diffenrence: {row['goals_difference']} | Points: {row['point']}")
        
    champions_league_teams = filtered_data.head(4)['name'].tolist()
    st.write("**UEFA Champions League Group Stage:**", ", ".join(champions_league_teams))

    # Vòng bảng UEFA Europa
    europa_league_teams = filtered_data.iloc[4:5]['name'].tolist()
    st.write("**UEFA Europa League Group Stage:**", ", ".join(europa_league_teams))

    # Xuống hạng
    relegated_teams = filtered_data.tail(3)['name'].tolist()
    st.write("**Relegation:**", ", ".join(relegated_teams))

    

elif page == "Thông tin cầu thủ":
    
    topPlayer = player.groupby(['name']).agg({'goals': 'sum'}).sort_values('goals', ascending=False).reset_index()
    topPlayer = player[player['name'].isin(topPlayer.name[:5])]

    fig = px.line(topPlayer, x='season',y='goals',color='name')
    fig.update_layout(
                    title='Top score player (season 2014 - 2020)',
                    xaxis_title='Season',
                    yaxis_title='Goals',
                    legend_title='PLayers'
    )
    st.plotly_chart(fig)
    
    players = st.selectbox('Chọn cầu thủ:', player['name'].unique())

    # Lọc dữ liệu theo cầu thủ được chọn
    player_data = player[player['name'] == players]
    
    # Vẽ biểu đồ số bàn thắng qua các mùa
    fig_goals = px.bar(player_data, x='season', y='goals', title=f'Số bàn thắng của {players} qua các mùa')
    st.plotly_chart(fig_goals)
    
    # Vẽ biểu đồ số kiến tạo qua các mùa
    fig_assists = px.bar(player_data, x='season', y='assists', title=f'Số kiến tạo của {players} qua các mùa')
    st.plotly_chart(fig_assists)
    

    fig_timeline = px.line(player_data, x='season', y=['goals', 'assists'],
                       title=f'Bàn thắng và Kiến tạo của {players} qua các mùa giải',
                        markers=True)
    st.plotly_chart(fig_timeline)

    # Biểu đồ phân bố cú sút (Shot Distribution)
    shot_distribution = player_data.melt(id_vars=['season'], value_vars=['goals', 'shots'],
                                        var_name='Type', value_name='Count')
    fig_shot_dist = px.bar(shot_distribution, x='season', y='Count', color='Type', barmode='group',
                        title=f'Phân bố cú sút của {players} qua các mùa giải')
    st.plotly_chart(fig_shot_dist)

        
    # Vẽ biểu đồ số đường chuyền quan trọng (keyPasses)
    fig_keypasses = px.bar(player_data, x='season', y='keyPasses', title=f'Số đường chuyền quan trọng của {players} qua các mùa')
    st.plotly_chart(fig_keypasses)
    
    
    
    
    
    # if 'positionX' in player_data.columns and 'positionY' in player_data.columns:
#     fig_heatmap = px.density_heatmap(player_data, x='positionX', y='positionY', nbinsx=50, nbinsy=50,
#                                      title='Khu vực sút bóng của cầu thủ', labels={'positionX': 'X', 'positionY': 'Y'})
#     fig_heatmap.update_yaxes(scaleanchor="x", scaleratio=1)
#     st.plotly_chart(fig_heatmap)
# else:
#     st.write("Không có dữ liệu vị trí sút bóng để hiển thị Heatmap.")


