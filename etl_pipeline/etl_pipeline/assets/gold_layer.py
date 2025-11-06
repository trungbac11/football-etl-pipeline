from dagster import asset, Output, AssetIn
import pandas as pd


@asset(
    io_manager_key='minio_io_manager',
    ins={
        'silver_teamseason': AssetIn(
            key_prefix=['football', 'silver']
        )
    },
    key_prefix=['football', 'gold'],
    compute_kind='Pandas'
)
def gold_teamseason(silver_teamseason: pd.DataFrame) -> Output[pd.DataFrame]:
    st = silver_teamseason.copy()

    def playerseason(group):
        wins = (group['homeGoals'] > group['awayGoals']).sum()
        draws = (group['homeGoals'] == group['awayGoals']).sum()
        loses = (group['homeGoals'] < group['awayGoals']).sum()
        minus_goals = (group['homeGoals'] - group['awayGoals']).sum()
        total_goals = group['homeGoals'].sum()
        points = wins * 3 + draws
        return pd.Series({'match': len(group), 'win': wins, 'draw': draws, 'lose': loses, 'goals': total_goals, 'goals_difference': minus_goals, 'point': points})

    team_on_season = st.groupby(['name', 'league', 'season','date']).apply(playerseason).reset_index()
    
    return Output(
       team_on_season,
        metadata={
            'table': 'teamseason',
            'records': len(team_on_season)
        }
    )
    
    
@asset(
    io_manager_key='minio_io_manager',
    ins={
        'silver_teamingames': AssetIn(
            key_prefix=['football', 'silver']
        )
    },
    key_prefix=['football', 'gold'],
    compute_kind='Pandas'
)
def gold_leagueseason(silver_teamingames: pd.DataFrame) -> Output[pd.DataFrame]: 
    st = silver_teamingames.copy()

    result = (
        st.groupby(['name', 'season'])
        .agg({'goals': 'sum', 'xGoals': 'sum', 'shots': 'sum', 'shotsOnTarget': 'sum', 'fouls': 'sum', 'yellowCards': 'sum', 'redCards': 'sum','corners': 'sum', 'gameID': 'count'})
        .reset_index()
    )

    result = result.rename(columns={'gameID':'games'})
    result['goalPerGame']= result.goals/result.games
    return Output(
        result,
        metadata={
            'table': 'leagueseason',
            'records': len(result)
        }
    )


@asset(
    io_manager_key='minio_io_manager',
    ins={
        'silver_playerappearances': AssetIn(
            key_prefix=['football', 'silver']
        )
    },
    key_prefix=['football', 'gold'],
    compute_kind='Pandas'
)
def gold_playerseason(silver_playerappearances: pd.DataFrame) -> Output[pd.DataFrame]: 
    st = silver_playerappearances.copy()

    results = (
       st.groupby(['playerID','name','season'])
        .agg({'goals': 'sum','shots': 'sum','xGoals':'sum','xGoalsChain':'sum','xGoalsBuildup':'sum','assists':'sum','keyPasses':'sum','xAssists':'sum','time': 'sum'})
        .reset_index()
    )
    
    return Output(
        results,
        metadata={
            'table': 'playerseason',
            'records': len(results)
        }
    )