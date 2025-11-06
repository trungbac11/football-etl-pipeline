CREATE SCHEMA IF NOT EXISTS analysis;


DROP TABLE IF EXISTS analysis.leagueseason CASCADE;
CREATE TABLE analysis.leagueseason(
    name varchar(32),
    season int,
    goals int,
    xGoals float,
    shots int,
    shotsOnTarget int,
    fouls int,
    yellowCards float,
    redCards int,
    corners int,
    games int,
    goalPerGame float
);
DROP TABLE IF EXISTS analysis.playerseason CASCADE;
CREATE TABLE analysis.playerseason(
    playerID int,
    name varchar(32),
    season int,
    goals int,
    shots int,
    xGoals float,
    xGoalsChain float,
    xGoalsBuildup float,
    assists int,
    keyPasses int,
    xAssists float
);

DROP TABLE IF EXISTS analysis.teamseason CASCADE;
CREATE TABLE analysis.teamseason(
    name varchar(32),
    league int,
    season int,
    date timestamp,
    win int,
    draw int,
    lose int,
    goals int,
    goals_difference int,
    point int
);
