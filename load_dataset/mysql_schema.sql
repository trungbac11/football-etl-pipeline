-- Tạo bảng leagues
DROP TABLE IF EXISTS leagues;
CREATE TABLE leagues (
  leagueID int NOT NULL, 
  name varchar(32), 
  understatNotation varchar(32),
  PRIMARY KEY (leagueID)
);

-- Tạo bảng teams
DROP TABLE IF EXISTS teams; 
CREATE TABLE teams (
  teamID int NOT NULL, 
  name varchar(32),
  PRIMARY KEY (teamID)
);

-- Tạo bảng players
DROP TABLE IF EXISTS players;
CREATE TABLE players (
  playerID int NOT NULL,
  name varchar(32),
  PRIMARY KEY (playerID)
);

-- Tạo bảng games
DROP TABLE IF EXISTS games; 
CREATE TABLE games (
  gameID int NOT NULL, 
  leagueID int NOT NULL,
  season int, 
  date timestamp, 
  homeTeamID int NOT NULL, 
  awayTeamID int NOT NULL, 
  homeGoals int4, 
  awayGoals int4, 
  homeProbability float4, 
  drawProbability float4, 
  awayProbability float4, 
  homeGoalsHalfTime int4, 
  awayGoalsHalfTime int4, 
  B365H float4, 
  B365D float4, 
  B365A float4, 
  BWH float4, 
  BWD float4, 
  BWA float4, 
  IWH float4, 
  IWD float4, 
  IWA float4, 
  PSH float4, 
  PSD float4, 
  PSA float4, 
  WHH float4, 
  WHD float4, 
  WHA float4, 
  VCH float4, 
  VCD float4, 
  VCA float4, 
  PSCH float4, 
  PSCD float4, 
  PSCA float4,
  PRIMARY KEY (gameID),
  FOREIGN KEY (leagueID) REFERENCES leagues(leagueID),
  FOREIGN KEY (homeTeamID) REFERENCES teams(teamID),
  FOREIGN KEY (awayTeamID) REFERENCES teams(teamID)
);

-- Tạo bảng teamstats
DROP TABLE IF EXISTS teamstats; 
CREATE TABLE teamstats (
  gameID int, 
  teamID int, 
  season int4, 
  date datetime, 
  location varchar(32), 
  goals int4, 
  xGoals float4, 
  shots int4, 
  shotsOnTarget int4, 
  deep int4, 
  ppda float4, 
  fouls int4, 
  corners int4, 
  yellowCards float4, 
  redCards int4, 
  result varchar(32),
  PRIMARY KEY (gameID, teamID),
  FOREIGN KEY (gameID) REFERENCES games(gameID),
  FOREIGN KEY (teamID) REFERENCES teams(teamID)
);

-- Tạo bảng appearances
DROP TABLE IF EXISTS appearances;
CREATE TABLE appearances (
  gameID int NOT NULL, 
  playerID int NOT NULL, 
  goals int4, 
  ownGoals int4, 
  shots int4, 
  xGoals float4, 
  xGoalsChain float4, 
  xGoalsBuildup float4, 
  assists int4, 
  keyPasses int4, 
  xAssists float4, 
  position varchar(32), 
  positionOrder int4, 
  yellowCard int4, 
  redCard int4, 
  time int4, 
  substituteIn int4, 
  substituteOut int4, 
  leagueID int NOT NULL,
  PRIMARY KEY (gameID, playerID),
  FOREIGN KEY (gameID) REFERENCES games(gameID),
  FOREIGN KEY (playerID) REFERENCES players(playerID),
  FOREIGN KEY (leagueID) REFERENCES leagues(leagueID)
);

-- Tạo bảng shots
DROP TABLE IF EXISTS shots;
CREATE TABLE shots (
  gameID int NOT NULL,
  shooterID int NOT NULL,
  assisterID int,
  minute int,
  situation varchar(32),
  lastAction varchar(32),
  shotType varchar(32),
  shotResult varchar(32),
  xGoal float4,
  positionX float4,
  positionY float4,
  PRIMARY KEY (gameID),
  FOREIGN KEY (gameID) REFERENCES games(gameID),
  FOREIGN KEY (shooterID) REFERENCES players(playerID),
  FOREIGN KEY (assisterID) REFERENCES players(playerID)
);
