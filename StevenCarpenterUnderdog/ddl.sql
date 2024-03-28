-- Assuming docker is installed and running the db can be created with the following:
-- docker run --name underdogpg -e POSTGRES_PASSWORD=<some_password> -d -p 5432:5432 postgres
-- docker cp Downloads/entries.csv underdogpg:/entries.csv
-- docker cp Downloads/games.csv underdogpg:/games.csv
-- docker cp Downloads/projections.csv underdogpg:/projections.csv
-- docker cp Downloads/selections.csv underdogpg:/selections.csv
-- docker exec -it underdogpg bash
-- psql -U postgres

-- Now in the psql shell or from your own SQL editor connected via jdbc:postgresql://localhost:5432/postgres
create database underdog;
\c underdog;

DROP TABLE IF EXISTS entries;
CREATE TABLE IF NOT EXISTS entries
(
    entry_id   varchar primary key,
    user_id    varchar,
    fee        double precision,
    payout     double precision,
    created_at timestamp
)
;
\copy entries FROM '/entries.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS selections;
CREATE TYPE higher_or_lower AS ENUM ('higher', 'lower', 'exact'); --if we want to have exact we could use this instead of the boolean which is more performant in the case given
CREATE TABLE IF NOT EXISTS selections
(
    selection_id     varchar primary key,
    entry_id         varchar,
    projection_id    varchar,
    choice_is_higher boolean,
    CONSTRAINT fk_entry_id foreign key (entry_id) references entries (entry_id),
    CONSTRAINT fk_projection_id foreign key (projection_id) references projections (projection_id)
)
;
\copy selections FROM '/selections.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS projections;
CREATE TABLE IF NOT EXISTS projections
(
    projection_id    varchar primary key,
    game_id          varchar,
    projection_value double precision,
    projection_name  varchar,
    CONSTRAINT fk_game_id foreign key (game_id) references games (game_id)
)
;
\copy projections FROM '/projections.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS games;
CREATE TABLE IF NOT EXISTS games
(
    game_id         varchar primary key,
    game_start_time timestamp,
    home_team       varchar,
    away_team       varchar,
    sport           varchar
)
;
\copy games FROM '/games.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS projection_analytics;
CREATE TABLE IF NOT EXISTS projection_analytics as
SELECT projection_id,
       projection_value,
       projection_name,
       g.game_id             as game_id,
       game_start_time,
       date(game_start_time) as game_start_time_date,
       home_team,
       away_team,
       sport
from projections p
         join games g on g.game_id = p.game_id;
ALTER TABLE projection_analytics
    ADD PRIMARY KEY (projection_id);
CREATE INDEX IF NOT EXISTS projection_game_start_time_date ON projection_analytics (game_start_time_date);
CREATE INDEX IF NOT EXISTS projection_sport ON projection_analytics (sport);

-- Flat selection-level table
DROP TABLE IF EXISTS selection_analytics;
CREATE TABLE IF NOT EXISTS selection_analytics as
select e.entry_id       as entry_id,
       s.selection_id   as selection_id,
       s.projection_id  as projection_id,
       user_id,
       fee,
       payout,
       created_at,
       date(created_at) as created_at_date,
       choice_is_higher
from entries e
         inner join selections s on e.entry_id = s.entry_id
;
ALTER TABLE selection_analytics
    ADD PRIMARY KEY (selection_id);
CREATE INDEX IF NOT EXISTS selection_projection_id ON selection_analytics (projection_id);
CREATE INDEX IF NOT EXISTS selection_created_at_date ON selection_analytics (created_at_date);


--Entries and Payout Analysis: Write a query to analyze the total number of entries and total payouts per sport.
select sport,
       count(distinct entry_id) as total_entries,
       sum(payout)                 total_payout
from selection_analytics sa
         join projection_analytics pa on sa.projection_id = pa.projection_id
group by pa.sport;


--Popular Projections: Write a query to find the most selected projections (top 5) across all entries.
select projection_id,
       count(projection_id) as projection_count
from selection_analytics
group by projection_id
order by projection_count desc
limit 5
;


--Performance Analysis: Write a query to determine the average entry fee and average payout per user, segmented by sport.
select user_id, sport, avg(fee) as average_fee, avg(payout) as average_payout
from selection_analytics
         join projection_analytics pa on selection_analytics.projection_id = pa.projection_id
group by user_id, sport
order by user_id --can be left off for performance, but it is nice for analysis
;


--Upcoming Games Analysis: Write a query to list all upcoming games for a specific sport, along with the number of projections offered for each game and entries for each projection.
-- For summary data at the game level
-- Using analytics_full captures all of the projections, including ones that have no selections. It might be worth having a fact table just for projections, but this was an outlier in the data I generated.
select game_id, count(distinct pa.projection_id) as count_projections, count(distinct entry_id) as count_entries
from projection_analytics pa
         left join selection_analytics sa on pa.projection_id = sa.projection_id --Include projections without selections
where sport = 'Tennis'
  and game_start_time > timestamp '2024-01-01 00:00:00' -- use CURRENT_TIMESTAMP for future relative to now
group by game_id
;


-- For entry count at the projection level
select game_id,
       pa.projection_id,
       count(distinct entry_id) as count_entries
from projection_analytics pa
         left join selection_analytics sa on pa.projection_id = sa.projection_id --Include projections without selections
where sport = 'Tennis'
  and game_start_time > timestamp '2024-01-01 00:00:00' -- use CURRENT_TIMESTAMP for future relative to now
group by game_id, pa.projection_id
;
