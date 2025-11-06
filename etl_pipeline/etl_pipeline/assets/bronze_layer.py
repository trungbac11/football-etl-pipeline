from dagster import asset, Output
import pandas as pd

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    compute_kind="MySQL"
)
def bronze_appearances(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM appearances"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table": "appearances",
            "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    compute_kind="MySQL"
)
def bronze_games(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM games"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table": "games",
            "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    compute_kind="MySQL"
)
def bronze_leagues(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM leagues"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table": "leagues",
            "records count": len(pd_data),
        },
    )
    
@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    compute_kind="MySQL"
)
def bronze_players(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM players"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table": "players",
            "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    compute_kind="MySQL"
)
def bronze_shots(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM shots"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table": "shots",
            "records count": len(pd_data),
        },
    )
    
@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    compute_kind="MySQL"
)
def bronze_teams(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM teams"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table": "teams",
            "records count": len(pd_data),
        },
    )    
    
@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["football", "bronze"],
    compute_kind="MySQL"
)
def bronze_teamstats(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM teamstats"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table": "teamstats",
            "records count": len(pd_data),
        },
    )    
