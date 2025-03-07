---ETL View
CREATE OR REPLACE VIEW ETL_SHOTS_DATA_NEW AS

WITH STEP1 AS (
    SELECT
        NO_OF_ROW,
        match_event_id,
        location_x,
        location_y,
        COALESCE(remaining_min, remaining_min_dup) AS remaining_min,
        COALESCE(power_of_shot, power_of_shot_dup) AS power_of_shot,
        COALESCE(knockout_match, knockout_match_dup) AS knockout_match,
        COALESCE(remaining_sec, remaining_sec_dup) AS remaining_sec,
        COALESCE(distance_of_shot, distance_of_shot_dup) AS distance_of_shot,
        game_season,
        TRY_TO_NUMBER(is_goal) AS IS_GOAL,
        area_of_shot,
        shot_basics,
        range_of_shot,
        COALESCE(team_name,'Manchester United') as team_name,
        DATE_OF_GAME,
        home_away,
        shot_id_number,
        "LATITUDE_LONGITUDE" AS LATITUDE_LONGITUDE,
        COALESCE(type_of_shot,type_of_combined_shot) AS type_of_shot,
        match_id,
        team_id
    FROM RONALDO_SHOT_DATA_RAW
),
STEP2 AS (
    SELECT
        *
        /*TO_DATE(date_of_game, 'DD-MM-YYYY') AS date_of_game_std*/
    FROM STEP1
),
STEP3 AS (
    SELECT
        *,
        TRY_TO_DOUBLE(SPLIT_PART(LATITUDE_LONGITUDE, ',', 1)) AS latitude,
        TRY_TO_DOUBLE(SPLIT_PART(LATITUDE_LONGITUDE, ',', 2)) AS longitude
    FROM STEP2
),
STEP4 AS (
    SELECT
        *,
        CASE
            WHEN home_away ILIKE '%@%'  THEN 'AWAY'
            WHEN home_away ILIKE '%vs.%' THEN 'HOME'
            ELSE 'UNKNOWN'
        END AS match_location_type,
        SPLIT_PART(home_away, ' ', 3) AS opponent_team
    FROM STEP3
),
STEP5 AS (
    
    SELECT *
    FROM STEP4
    WHERE match_id IS NOT NULL
      AND shot_id_number IS NOT NULL
),
Ranked AS (
    -- Remove duplicates (keep first row per match_id + shot_id_number)
    SELECT
        STEP5.*,
        ROW_NUMBER() OVER (PARTITION BY match_id, shot_id_number ORDER BY match_event_id) AS rn
    FROM STEP5
)
SELECT
    no_of_row,
    match_event_id,
    location_x,
    location_y,
    remaining_min,
    power_of_shot,
    knockout_match,
    remaining_sec,
    distance_of_shot,
    game_season,
    is_goal,
    --SUM(IS_GOAL)/
    area_of_shot,
    shot_basics,
    range_of_shot,
    team_name,
    date_of_game,
    match_location_type,
    opponent_team,
    shot_id_number,
    latitude,
    longitude,
    type_of_shot,
    match_id,
    team_id
FROM RANKED
WHERE rn = 1 AND IS_GOAL IS NOT NULL;
-- 

SELECT * FROM YDS_DATA.RONALDO_STATS_DATA.ETL_SHOTS_DATA_NEW;

