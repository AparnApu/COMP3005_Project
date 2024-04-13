ddl_statements_events = 

    CREATE TABLE IF NOT EXISTS competitions (
            competition_id INTEGER NOT NULL,
            season_id INTEGER NOT NULL,
            country_name VARCHAR(255),
            competition_name VARCHAR(255),
            competition_gender VARCHAR(50),
            competition_youth BOOLEAN,
            competition_international BOOLEAN,
            season_name VARCHAR(255),

            PRIMARY KEY (competition_id, season_id)
        );


    CREATE TABLE IF NOT EXISTS eventType (
        id INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (id)
    );
        
    CREATE TABLE IF NOT EXISTS eventPlayPattern (
        id INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (id)
    );
        
    CREATE TABLE IF NOT EXISTS eventTeams (
        id INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS player (
        id INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS position (
        id INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS passHeight (
        id INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS bodyPart (
        id INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS technique (
        id INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS skillType (
        id INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS outcome (
        id INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS events (
        id UUID NOT NULL,
        index INTEGER NOT NULL,
        period INTEGER NOT NULL,
        timestamp TIME(3) NOT NULL,
        minute INTEGER NOT NULL,
        second INTEGER NOT NULL,
        type INTEGER NOT NULL,
        possession INTEGER NOT NULL,
        possession_team INTEGER NOT NULL,
        play_pattern INTEGER NOT NULL,
        team INTEGER NOT NULL,
        duration FLOAT,
        location FLOAT[],

        carry_end_location FLOAT[], -- REMOVE, it is has its own table
        
        counterpress BOOLEAN,
        
        recovery_failure BOOLEAN, -- FK

        PRIMARY KEY (id),

        FOREIGN KEY (type) REFERENCES eventType(id),
        FOREIGN KEY (possession_team) REFERENCES eventTeams(id),
        FOREIGN KEY (play_pattern) REFERENCES eventPlayPattern(id),
        FOREIGN KEY (team) REFERENCES eventTeams(id)
    );

    CREATE TABLE IF NOT EXISTS pass (
        recipientId INTEGER NOT NULL,
        length FLOAT NOT NULL,
        angle FLOAT NOT NULL,
        heightId INTEGER NOT NULL,
        end_location FLOAT[] NOT NULL,
        bodyPartId INTEGER NOT NULL,
        skillTypeId INTEGER,
        outcomeId INTEGER,
        aerial_won BOOLEAN,
        switch BOOLEAN,
        crossed BOOLEAN,
        assisted_shot_id INTEGER,
        goal_assist BOOLEAN,
        shot_assist BOOLEAN,
        outswinging BOOLEAN,
        techniqueId INTEGER,
        deflected BOOLEAN,
        straight BOOLEAN,
        inswinging BOOLEAN,
        cut_back BOOLEAN,
        through_ball BOOLEAN,
        miscommunication BOOLEAN,
        no_touch BOOLEAN,
        
        eventId UUID NOT NULL,

        PRIMARY KEY (recipientId, length, angle, heightId),

        FOREIGN KEY (recipientId) REFERENCES player(id),
        FOREIGN KEY (heightId) REFERENCES passHeight(id),
        FOREIGN KEY (bodyPartId) REFERENCES bodyPart(id),
        FOREIGN KEY (techniqueId) REFERENCES technique(id),
        FOREIGN KEY (skillTypeId) REFERENCES skillType(id),
        FOREIGN KEY (outcomeId) REFERENCES outcome(id),
        FOREIGN KEY (eventId) REFERENCES events(id)
    );

    CREATE TABLE IF NOT EXISTS ballRecovery (
        eventId UUID NOT NULL,
        recovery_failure BOOLEAN,
        offensive BOOLEAN,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );

    CREATE TABLE IF NOT EXISTS dispossessed (
        eventId UUID NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );

    CREATE TABLE IF NOT EXISTS duel (
        eventId UUID NOT NULL,
        outcomeId INTEGER,
        skillTypeId INTEGER NOT NULL,
        
        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (outcomeId) REFERENCES outcome(id),
        FOREIGN KEY (skillTypeId) REFERENCES skillType(id)
    );

    CREATE TABLE IF NOT EXISTS block (
        eventId UUID NOT NULL,
        deflection BOOLEAN,
        offensive BOOLEAN,
        save_block BOOLEAN,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );

    CREATE TABLE IF NOT EXISTS offside (
        eventId UUID NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );

    CREATE TABLE IF NOT EXISTS clearance (
        eventId UUID NOT NULL,
        bodyPartId INTEGER NOT NULL,
        aerialWon BOOLEAN,
        head BOOLEAN,
        leftFoot BOOLEAN,
        rightFoot BOOLEAN,
        other BOOLEAN,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (bodyPartId) REFERENCES bodyPart(id)
    );

    CREATE TABLE IF NOT EXISTS interception (
        eventId UUID NOT NULL,
        outcomeId INTEGER NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (outcomeId) REFERENCES outcome(id)
    );

    CREATE TABLE IF NOT EXISTS freeze_frame (
        location FLOAT[] NOT NULL,
        playerId INTEGER NOT NULL,
        positionId INTEGER NOT NULL,
        eventId UUID NOT NULL,
        teammate BOOLEAN NOT NULL,

        PRIMARY KEY (eventId),
        
        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (playerId) REFERENCES player(id),
        FOREIGN KEY (positionId) REFERENCES position(id)
    );

    CREATE TABLE IF NOT EXISTS shots (
        statsbomb_xg FLOAT NOT NULL,
        end_location FLOAT[] NOT NULL,
        key_pass_id UUID,
        techniqueId INTEGER,
        first_time BOOLEAN,
        bodyPartId INTEGER NOT NULL,
        skillTypeId INTEGER NOT NULL,
        outcomeId INTEGER,
        
        aerial_won BOOLEAN,
        open_goal BOOLEAN,
        deflected BOOLEAN,
        one_on_one BOOLEAN,
        redirect BOOLEAN,
        saved_to_post BOOLEAN,
        follows_dribble BOOLEAN,
        saved_off_target BOOLEAN,
        
        eventId UUID NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (techniqueId) REFERENCES technique(id),
        FOREIGN KEY (bodyPartId) REFERENCES bodyPart(id),
        FOREIGN KEY (skillTypeId) REFERENCES skillType(id),
        FOREIGN KEY (outcomeId) REFERENCES outcome(id),
        FOREIGN KEY (eventId) REFERENCES events(id)
    );

    CREATE TABLE IF NOT EXISTS eventTactics (

        event_id UUID NOT NULL,
        formation INTEGER NOT NULL,

        PRIMARY KEY (event_id),

        FOREIGN KEY (event_id) references events(id)
            ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS eventLineup (
        event_tactics_id UUID NOT NULL,
        player_id INTEGER NOT NULL,
        position_id INTEGER NOT NULL,
        jersey_number INTEGER NOT NULL,

        PRIMARY KEY (event_tactics_id, player_id, position_id),

        FOREIGN KEY (event_tactics_id) references eventTactics(event_id)
            ON DELETE CASCADE,

        FOREIGN KEY (player_id) references player(id),

        FOREIGN KEY (position_id) references position(id)
    );

--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
-- MATCHES
--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS competitionStage (
    stage_id INTEGER NOT NULL,
    stage_name TEXT NOT NULL

    PRIMARY KEY (stage_id)
);


CREATE TABLE IF NOT EXISTS country (
    country_id INTEGER NOT NULL,
    country_name TEXT NOT NULL,

    PRIMARY KEY (country_id)
);


CREATE TABLE IF NOT EXISTS stadium (
    stadium_id INTEGER NOT NULL,
    stadium_name TEXT NOT NULL,
    countryID INTEGER,

    PRIMARY KEY (stadium_id),

    FOREIGN KEY (countryID) references country(country_id)
);


CREATE TABLE IF NOT EXISTS referee (
    referee_id INTEGER NOT NULL,
    referee_name TEXT NOT NULL,
    countryID INTEGER NOT NULL

    PRIMARY KEY (referee_id),

    FOREIGN KEY (countryID) references country(country_id)
);


CREATE TABLE IF NOT EXISTS manager (
    manager_id INTEGER NOT NULL,
    manager_name TEXT NOT NULL,
    manager_nickname TEXT,
    manager_dob TEXT,
    countryID INTEGER,

    PRIMARY KEY (manager_id),

    FOREIGN KEY (countryID) references country(country_id)
);


CREATE TABLE IF NOT EXISTS teams (
    team_id INTEGER NOT NULL,
    team_name TEXT NOT NULL,
    team_gender TEXT, 
    team_group TEXT,
    countryID INTEGER,
    managersID INTEGER,

    PRIMARY KEY (team_id),

    FOREIGN KEY (countryID) references country(country_id),
    FOREIGN KEY (managersID) references manager(manager),
);


CREATE TABLE IF NOT EXISTS team_manager_link (
    team_id INTEGER NOT NULL,
    manager_id INTEGER NOT NULL,

    PRIMARY KEY (team_id, manager_id)
);


CREATE TABLE IF NOT EXISTS matches (
    match_id INTEGER NOT NULL,
    match_date TEXT,
    kick_off TIME(3),
    competitionID INTEGER, 
    seasonID INTEGER, 
    home_teamID INTEGER, 
    away_teamID INTEGER,
    home_score INTEGER,
    away_score INTEGER,
    match_week INTEGER,
    competition_stageID INTEGER, 
    stadiumID INTEGER, 
    refereeID INTEGER,

    PRIMARY KEY (match_id),

    FOREIGN KEY (competitionID) references competition(competition_id),
    FOREIGN KEY (seasonID) references competition(season_id),
    FOREIGN KEY (home_teamID) references teams(team_id),
    FOREIGN KEY (away_teamID) references teams(team_id),
    FOREIGN KEY (stadiumID) references stadium(stadium_id),
    FOREIGN KEY (refereeID) references referee(referee_id),
);










