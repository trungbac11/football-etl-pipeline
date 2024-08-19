from dagster import asset, Output, AssetIn
import pandas as pd

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "bronze_teamstats": AssetIn(
            key_prefix=["football", "bronze"]
        ),
        "bronze_games": AssetIn(
            key_prefix=["football", "bronze"]
        ),
        "bronze_leagues": AssetIn(
            key_prefix=["football", "bronze"]
        )
    },
    key_prefix=["football", "silver"],
    compute_kind="Pandas"
)
def silver_teamingames(bronze_teamstats: pd.DataFrame, bronze_games: pd.DataFrame, bronze_leagues: pd.DataFrame) -> Output[pd.DataFrame]:
    tst = bronze_teamstats.copy()
    ga = bronze_games.copy()
    lg = bronze_leagues.copy()

    #drop unsusable columns 
    ga.drop(columns=ga.columns.to_list()[13:], inplace=True)
    
    #create 
    result = pd.merge(tst, ga, on="gameID")
    result = result.merge(lg, on="leagueID", how="left")
    result.drop(columns=["season_y", "date_y"],inplace=True)
    result = result.rename(columns={"season_x": "season", "date_x": "date"})
    return Output(
        result,
        metadata={
            "table": "teamingames",
            "records": len(result)
        }
    )
    

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "bronze_appearances": AssetIn(
            key_prefix=["football", "bronze"]
        ),
        "bronze_games": AssetIn(
            key_prefix=["football", "bronze"]
        ),
        "bronze_players": AssetIn(
            key_prefix=["football", "bronze"]
        )
    },
    key_prefix=["football", "silver"],
    compute_kind="Pandas"
)
def silver_playerappearances(bronze_appearances: pd.DataFrame, bronze_games: pd.DataFrame, bronze_players: pd.DataFrame) -> Output[pd.DataFrame]:
    ap = bronze_appearances.copy()
    ge = bronze_games.copy()
    pl = bronze_players.copy()

    #drop unusable column
    ge.drop(columns=ge.columns.to_list()[13:], inplace=True)

    #merge 
    player_appearances = pd.merge(ap, pl, on="playerID", how="left")
    player_appearances = pd.merge(player_appearances, ge, on="gameID", how="left")

    #drop and rename
    player_appearances.drop(columns=["leagueID_y"],inplace=True)
    player_appearances.rename(columns={"leagueID_x": "leagueID"}, inplace=True)
    return Output(
        player_appearances,
        metadata={
            "table": "playerappearances",
            "records": len(player_appearances)
        }
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "bronze_teams": AssetIn(
            key_prefix=["football", "bronze"]
        ),
        "bronze_games": AssetIn(
            key_prefix=["football", "bronze"]
        )
    },
    key_prefix=["football", "silver"],
    compute_kind="Pandas"
)
def silver_teamseason(bronze_teams: pd.DataFrame, bronze_games: pd.DataFrame, bronze_leagues: pd.DataFrame) -> Output[pd.DataFrame]:
    tm = bronze_teams.copy()
    gm = bronze_games.copy()
    le = bronze_leagues.copy()
    #drop 
    gm.drop(columns=gm.columns.to_list()[8:], inplace=True)
    
    home_games = pd.merge(gm, tm, left_on="homeTeamID", right_on="teamID", how="left")
    away_games = pd.merge(gm, tm, left_on="awayTeamID", right_on="teamID", how="left")

    team_on_seasons = pd.concat([home_games, away_games], ignore_index=True)
    team_on_seasons = pd.merge(team_on_seasons, le, on="leagueID", how="left")
    
    team_on_seasons.rename(columns={"name_x": "name", "name_y":"league"}, inplace=True)
    team_on_seasons.drop(columns={"leagueID", "understatNotation", "teamID","gameID"}, inplace=True)
    return Output( 
        team_on_seasons,
        metadata={
            "table": "teamseason",
            "records": len(team_on_seasons)
        }
    )
    
@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "bronze_shots": AssetIn(
            key_prefix=["football", "bronze"]
        ),
        "bronze_games": AssetIn(
            key_prefix=["football", "bronze"]
        ),
        "bronze_players": AssetIn(
            key_prefix=["football", "bronze"]
        )
    },
    key_prefix=["football", "silver"],
    compute_kind="Pandas"
)
def silver_shotgames(bronze_shots: pd.DataFrame, bronze_games: pd.DataFrame, bronze_players: pd.DataFrame) -> Output[pd.DataFrame]:
    sh = bronze_shots.copy()
    gam = bronze_games.copy()
    ply = bronze_players.copy()
    
    #drop unusable column
    gam.drop(columns=gam.columns.to_list()[13:], inplace=True)
    
    #merge 
    shots_games = pd.merge(sh, ply, left_on="shooterID", right_on="playerID", how="left")
    shots_games = pd.merge(shots_games, gam, on="gameID", how="left")
    
    
    return Output(
        shots_games,
        metadata={
            "table": "shotgames",
            "records": len(shots_games)
        }
    )    