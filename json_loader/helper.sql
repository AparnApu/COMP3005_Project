CREATE TABLE IF NOT EXISTS events 
(
    id UUID,
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
    duration INTEGER NOT NULL,

    PRIMARY KEY (id),

    FOREIGN KEY (type) REFERENCES eventType(id),
    FOREIGN KEY (possession_team) REFERENCES eventTeam(id),
    FOREIGN KEY (play_pattern) REFERENCES eventPlayPattern(id),
    FOREIGN KEY (team) REFERENCES eventTeam(id)
);
    

CREATE TABLE IF NOT EXISTS eventType 
(
    id INTEGER NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);
    

CREATE TABLE IF NOT EXISTS eventPossessionTeam 
(
    id INTEGER NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);
    

CREATE TABLE IF NOT EXISTS eventPlayPattern 
(
    id INTEGER NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);
    
    
CREATE TABLE IF NOT EXISTS eventTeam 
(
    id INTEGER NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS player 
(
    id INTEGER NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS position 
(
    id INTEGER NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS eventTactics 
(
    event_id UUID NOT NULL,
    formation INTEGER NOT NULL,

    PRIMARY KEY (event_id),

    FOREIGN KEY (event_id) references events(id)
        ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS eventLineup 
(
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