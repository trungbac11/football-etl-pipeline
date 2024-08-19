from dagster import multi_asset, Output, AssetIn, AssetOut, asset
import pandas as pd

@multi_asset(
    ins={
        "gold_leagueseason": AssetIn(
            key_prefix=["football", "gold"]
        )
    },
    outs={
        "leagueseason": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["leagueseason", "analysis"],
            metadata={
                "columns": [
                    "name",
                    "season",
                    "goals",
                    "xGoals",
                    "shots",
                    "shotsOnTarget",
                    "fouls",
                    "yellowCards",
                    "redCards",
                    "corners",
                    "games",
                    "goalPerGame"
                ]
            }
        ),
    },
    compute_kind="PostgreSQL",
)
def leagueseason(gold_leagueseason: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        gold_leagueseason,
        metadata={
            "schema": "analysis",
            "table": "leagueseason",
            "records": len(gold_leagueseason)
        }
    )


@multi_asset(
    ins={
        "gold_teamseason": AssetIn(
            key_prefix=["football", "gold"]
        )
    },
    outs={
        "teamseason": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["teamseason", "analysis"],
            metadata={
                "columns": [
                    "name",
                    "league",
                    "season",
                    "date",
                    "match",
                    "win",
                    "draw",
                    "lose",
                    "goals",
                    "goals_difference",
                    "point"
                ]
            }
        )
    },
    compute_kind="PostgreSQL",
)
def teamseason(gold_teamseason: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        gold_teamseason,
        metadata={
            "schema": "analysis",
            "table": "teamseason",
            "records": len(gold_teamseason)
        }
    )

@multi_asset(
    ins={
        "gold_playerseason": AssetIn(
            key_prefix=["football", "gold"]
        )
    },
    outs={
        "playerseason": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["playerseason", "analysis"],
            metadata={
                "columns": [
                    "playerID",
                    "name",
                    "season",
                    "goals",
                    "shots",
                    "xGoals",
                    "xGoalsChain",
                    "xGoalsBuildup",
                    "assists",
                    "keyPasses",
                    "xAssists"
                ]
            }
        )
    },
    compute_kind="PostgreSQL",
)
def playerseason(gold_playerseason: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        gold_playerseason,
        metadata={
            "schema": "analysis",
            "table": "playerseason",
            "records": len(gold_playerseason)
        }
    )