from dagster import asset, Output, AssetIn
import pandas as pd

@asset(
    io_manager_key='minio_io_manager',
    required_resource_keys={'minio_io_manager'},
    ins={
        'teamstats': AssetIn(
            key_prefix=['football', 'bronze']
        ),
        'games': AssetIn(
            key_prefix=['football', 'bronze']
        ),
        'leagues': AssetIn(
            key_prefix=['football', 'bronze']
        )
    },
    key_prefix=['football', 'silver'],
    compute_kind='Pandas'
)
def silver_statsTeamOnGames(teamstats: pd.DataFrame, games: pd.DataFrame, leagues: pd.DataFrame) -> Output[pd.DataFrame]:
    tst = teamstats.copy()
    ga = games.copy()
    lg = leagues.copy()

    #drop unsusable columns 
    ga.drop(columns=ga.columns.to_list()[13:], inplace=True)
    
    #create 
    result = pd.merge(tst, ga, on='gameID')
    result = result.merge(lg, on='leagueID', how='left')
    result.drop(columns=['season_y', 'date_y'],inplace=True)
    result = result.rename(columns={'season_x': 'season', 'date_x': 'date'})
    return Output(
        result,
        metadata={
            'table': 'statsTeamOnGames',
            'records': len(result)
        }
    )
    

@asset(
    io_manager_key='minio_io_manager',
    required_resource_keys={'minio_io_manager'},
    ins={
        'appearances': AssetIn(
            key_prefix=['football', 'bronze']
        ),
        'games': AssetIn(
            key_prefix=['football', 'bronze']
        ),
        'players': AssetIn(
            key_prefix=['football', 'bronze']
        )
    },
    key_prefix=['football', 'silver'],
    compute_kind='Pandas'
)
def silver_playerAppearances(appearances: pd.DataFrame, games: pd.DataFrame, players: pd.DataFrame) -> Output[pd.DataFrame]:
    ap = appearances.copy()
    ge = games.copy()
    pl = players.copy()

    #drop unusable column
    ge.drop(columns=ge.columns.to_list()[13:], inplace=True)

    #merge 
    player_appearances = pd.merge(ap, pl, on='playerID', how='left')
    player_appearances = pd.merge(player_appearances, ge, on='gameID', how='left')

    #drop and rename
    player_appearances.drop(columns=['leagueID_y'],inplace=True)
    player_appearances.rename(columns={'leagueID_x': 'leagueID'}, inplace=True)
    return Output(
        player_appearances,
        metadata={
            'table': 'playerAppearances',
            'records': len(player_appearances)
        }
    )

@asset(
    io_manager_key='minio_io_manager',
    required_resource_keys={'minio_io_manager'},
    ins={
        'teams': AssetIn(
            key_prefix=['football', 'bronze']
        ),
        'games': AssetIn(
            key_prefix=['football', 'bronze']
        )
    },
    key_prefix=['football', 'silver'],
    compute_kind='Pandas'
)
def silver_teamsOnSeason(teams: pd.DataFrame, games: pd.DataFrame, leagues: pd.DataFrame) -> Output[pd.DataFrame]:
    tm = teams.copy()
    gm = games.copy()
    le = leagues.copy()
    #drop 
    gm.drop(columns=gm.columns.to_list()[8:], inplace=True)
    
    team_on_seasons = pd.merge(gm, tm, left_on='homeTeamID', right_on='teamID', how='left')
    team_on_seasons = pd.merge(team_on_seasons, le, on='leagueID', how='left')
    
    team_on_seasons.rename(columns={'name_x': 'name', 'name_y':'league'}, inplace=True)
    team_on_seasons.drop(columns={'leagueID', 'understatNotation', 'teamID','gameID'}, inplace=True)
    return Output( 
        team_on_seasons,
        metadata={
            'table': 'teamsOnSeason',
            'records': len(team_on_seasons)
        }
    )
    
@asset(
    io_manager_key='minio_io_manager',
    required_resource_keys={'minio_io_manager'},
    ins={
        'shots': AssetIn(
            key_prefix=['football', 'bronze']
        ),
        'games': AssetIn(
            key_prefix=['football', 'bronze']
        ),
        'players': AssetIn(
            key_prefix=['football', 'bronze']
        )
    },
    key_prefix=['football', 'silver'],
    compute_kind='Pandas'
)
def silver_shotGames(shots: pd.DataFrame, games: pd.DataFrame, players: pd.DataFrame) -> Output[pd.DataFrame]:
    sh = shots.copy()
    gam = games.copy()
    ply = players.copy()
    
    #drop unusable column
    gam.drop(columns=gam.columns.to_list()[13:], inplace=True)
    
    #merge 
    shots_games = pd.merge(sh, ply, left_on='shooterID', right_on='playerID', how='left')
    shots_games = pd.merge(shots_games, gam, on='gameID', how='left')
    
    
    return Output(
        shots_games,
        metadata={
            'table': 'shotGames',
            'records': len(shots_games)
        }
    )    