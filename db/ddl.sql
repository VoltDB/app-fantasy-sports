CREATE TABLE nfl_player_game_score (
  player_id        INTEGER      NOT NULL,
  game_id          INTEGER      NOT NULL,
  score            INTEGER      NOT NULL,
  PRIMARY KEY (player_id, game_id)
);

CREATE TABLE nfl_contest_small (
  contest_id       INTEGER      NOT NULL,
  game_id          INTEGER      NOT NULL,
  PRIMARY KEY (contest_id)
);
PARTITION TABLE nfl_contest_small ON COLUMN contest_id;

CREATE TABLE nfl_contest_large (
  contest_id       INTEGER      NOT NULL,
  game_id          INTEGER      NOT NULL,
  PRIMARY KEY (contest_id)
);

CREATE TABLE customer (
  customer_id      INTEGER      NOT NULL,
  name             VARCHAR(30)  NOT NULL,
  PRIMARY KEY (customer_id)  
);
PARTITION TABLE customer ON COLUMN customer_id;

CREATE TABLE customer_contest_score (
  customer_id      INTEGER      NOT NULL,
  contest_id       INTEGER      NOT NULL,
  score            INTEGER      NOT NULL,
  rank             INTEGER,
  PRIMARY KEY (customer_id, contest_id)
);
PARTITION TABLE customer_contest_score ON COLUMN customer_id;
CREATE INDEX idx_customer_contest_score ON customer_contest_score (contest_id, score);

-- CREATE TABLE customer_contest_rank (
--   contest_id       INTEGER      NOT NULL,
--   customer_id      INTEGER      NOT NULL,
--   rank             INTEGER      NOT NULL,
--   PRIMARY KEY (customer_id, contest_id)
-- );
-- PARTITION TABLE customer_contest_rank ON COLUMN customer_id;
--CREATE INDEX idx_ranks ON customer_contest_rank (contest_id, score);

CREATE TABLE customer_contest_roster (
  contest_id       INTEGER      NOT NULL,
  customer_id      INTEGER      NOT NULL,
  player_id        INTEGER      NOT NULL,
  PRIMARY KEY (contest_id, customer_id, player_id)
);
PARTITION TABLE customer_contest_roster ON COLUMN customer_id;
CREATE INDEX idx_roster_by_contest ON customer_contest_roster (contest_id, customer_id);

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES procs.jar;

-- Define procedures
CREATE PROCEDURE PARTITION ON TABLE customer COLUMN customer_id FROM CLASS procedures.SelectAllScoresInPartition;
CREATE PROCEDURE PARTITION ON TABLE customer COLUMN customer_id FROM CLASS procedures.SelectContestScoresInPartition;
CREATE PROCEDURE PARTITION ON TABLE customer COLUMN customer_id FROM CLASS procedures.UpsertCustomerScores;

