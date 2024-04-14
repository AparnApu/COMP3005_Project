##########################################################################################################################
# Aparna Apu
# 101194937

# Elie Feghali
# 101185489

# COMP 3005 Project (Version 1)
# Create the database + populate it

# Citations
# https://github.com/statsbomb/open-data/tree/0067cae166a56aa80b2ef18f61e16158d6a7359a
# https://www.psycopg.org/psycopg3/docs/basic/install.html
# https://www.psycopg.org/psycopg3/docs/basic/usage.html 
# https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-uuid/
# https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-time/

# If you are running this, paths and passwords will change!
##########################################################################################################################

import os
import json
import psycopg2
import psycopg2.extras

# Database connection parameters
dbname = "project_database"
user = "postgres"
host = "localhost"
port = "5432"

# If running this code, you may have to change this variable
db_password = "1234"

# Connect to the database via the database connection parameters
conn = psycopg2.connect(f"dbname={dbname} user={user} host={host} port={port} password={db_password}")

##########################################################################################################################
# Purpose: Helper function to get relevant "event" and "lineup" files

# Notes: 
#       Seasons/Competitions we care about are as follows - 
#           La Liga 2018/2019, 2019/2020, 2020/2021 and Premier League 2003/2004
#       Seasons in order are - 4, 42, 90, 44 (these will be the files in the matches folder)
#       Competitions are     - 11, 2 (these will be the folders in the matches folder)

#       So we loop through the matches folder and filter on competition and season to get files relevant to us
##########################################################################################################################

def getRelevantFiles():
    path_to_data = "/home/cheesemaker/Documents/COMP3005/team_project/open-data-master/data/matches"

    relevant_seasons = ["44.json", "4.json", "42.json", "90.json"]
    relevant_comp_id = ["2", "11"]
    relevant_match_ids = []

    for folder in os.listdir(path_to_data):

        if (folder in relevant_comp_id):
            folder_path = os.path.join(path_to_data, folder)

            for file in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file)
                
                if (file in relevant_seasons):
                    with open(file_path, 'r') as f:
                        matches_table_data = json.load(f)

                        for match in matches_table_data:
                            relevant_match_ids.append(str(match['match_id']) + ".json")
                        
    return relevant_match_ids

##########################################################################################################################
# Purpose: Function to create and populate the competitions table

# Notes: 
#       We will only populate with entries relevant to our purpose
#       Metadata attributes are ignored
##########################################################################################################################

def setupCompetitions():

    print("Setting up competitions ...", end = "")

    # Path to data
    path_to_data = "/home/cheesemaker/Documents/COMP3005/team_project/open-data-master/data/competitions.json"

    # To filter on season
    season_ids = [4, 42, 44, 90]
    comp_ids = [2, 11]

    with open(path_to_data, 'r') as file:
        competitions = json.load(file)

    competitions_ddl = """
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
    """

    competitions_dml = """
    INSERT INTO competitions (competition_id, season_id, country_name, competition_name, competition_gender, competition_youth, competition_international, season_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT (competition_id, season_id) DO NOTHING;
    """
    
    with conn.cursor() as cursor:

        # Create the competitions table
        cursor.execute(competitions_ddl)

        # Populate the competitions table
        # Each competition is an entity not a line
        for competition in competitions:
            if (competition['competition_id'] in comp_ids and competition['season_id'] in season_ids):
                cursor.execute(competitions_dml, (
                    competition['competition_id'],
                    competition['season_id'],
                    competition['country_name'],
                    competition['competition_name'],
                    competition['competition_gender'],
                    competition['competition_youth'],
                    competition['competition_international'],
                    competition['season_name']
                ))

        conn.commit()
    
    print(" done!")


##########################################################################################################################
# Purpose: Function to create and populate the events table (and related tables)

# Notes: 
#       We will only populate with entries relevant to our purpose using our above helper function
#       Metadata attributes are ignored
##########################################################################################################################

def setupEvents():

    print("Setting up events ...", end = "")

    # Path to data
    path_to_data = "/home/cheesemaker/Documents/COMP3005/team_project/open-data-master/data/events"

    # ELIE PUT YOUR CODE HERE
    events_ddl = """
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
        matchId INTEGER NOT NULL,
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
        counterpress BOOLEAN,
        off_camera BOOLEAN,
        under_pressure BOOLEAN,
        out BOOLEAN,
        playerId INTEGER,
        positionId INTEGER,


        PRIMARY KEY (id),

        FOREIGN KEY (type) REFERENCES eventType(id),
        FOREIGN KEY (matchId) REFERENCES matches(match_id),
        FOREIGN KEY (possession_team) REFERENCES teams(team_id),
        FOREIGN KEY (play_pattern) REFERENCES eventPlayPattern(id),
        FOREIGN KEY (team) REFERENCES teams(team_id),
        FOREIGN KEY (playerId) REFERENCES player(id),
        FOREIGN KEY (positionId) REFERENCES position(id)
    );
    

    CREATE TABLE IF NOT EXISTS pass (
        eventId UUID NOT NULL,
        matchId INTEGER NOT NULL,
        playerId INTEGER NOT NULL,
        teamId INTEGER NOT NULL,
        recipientId INTEGER,
        length FLOAT,
        angle FLOAT,
        heightId INTEGER ,
        end_location FLOAT[],
        bodyPartId INTEGER,
        skillTypeId INTEGER,
        outcomeId INTEGER,
        aerial_won BOOLEAN,
        switch BOOLEAN,
        crossed BOOLEAN,
        assisted_shot_id UUID,
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
        

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (matchId) REFERENCES matches(match_id),
        FOREIGN KEY (playerId) REFERENCES player(id),
        FOREIGN KEY (recipientId) REFERENCES player(id),
        FOREIGN KEY (heightId) REFERENCES passHeight(id),
        FOREIGN KEY (bodyPartId) REFERENCES bodyPart(id),
        FOREIGN KEY (techniqueId) REFERENCES technique(id),
        FOREIGN KEY (skillTypeId) REFERENCES skillType(id),
        FOREIGN KEY (outcomeId) REFERENCES outcome(id)
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


    CREATE TABLE IF NOT EXISTS dribble (
        eventId UUID NOT NULL,
        playerId INTEGER NOT NULL,
        matchId INTEGER NOT NULL,
        outcomeId INTEGER NOT NULL,
        overrun BOOLEAN,
        nutmeg BOOLEAN,
        no_touch BOOLEAN,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (outcomeId) REFERENCES outcome(id)
    );

    
    CREATE TABLE IF NOT EXISTS pressure (
        eventId UUID NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );


    CREATE TABLE IF NOT EXISTS halfStart (
        eventId UUID NOT NULL,
        late_video_start BOOLEAN,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );  

    
    CREATE TABLE IF NOT EXISTS halfEnd (
        eventId UUID NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );  

    
    CREATE TABLE IF NOT EXISTS substitution (
        eventId UUID NOT NULL,
        outcomeId INTEGER NOT NULL,
        replacementId INTEGER NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (outcomeId) REFERENCES outcome(id),
        FOREIGN KEY (replacementId) REFERENCES player(id)
    );  


    CREATE TABLE IF NOT EXISTS ownGoalAgainst (
        eventId UUID NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );  


    CREATE TABLE IF NOT EXISTS foulWon (
        eventId UUID NOT NULL,
        advantage BOOLEAN,
        defensive BOOLEAN,
        penalty BOOLEAN,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    ); 


    CREATE TABLE IF NOT EXISTS foulCommitted (
        eventId UUID NOT NULL,
        cardId INTEGER,
        skillTypeId INTEGER,
        advantage BOOLEAN,
        offensive BOOLEAN,
        penalty BOOLEAN,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (cardId) REFERENCES card(id),
        FOREIGN KEY (skillTypeId) REFERENCES skillType(id)
    );   


    CREATE TABLE IF NOT EXISTS goalKeeper (
        eventId UUID NOT NULL,
        outcomeId INTEGER,
        techniqueId INTEGER,
        positionId INTEGER,
        skillTypeId INTEGER NOT NULL,
        bodyPartId INTEGER,
        shot_saved_off_target BOOLEAN,
        punched_out BOOLEAN,
        success_in_play BOOLEAN,
        lost_out BOOLEAN,
        end_location FLOAT[],
        lost_in_play  BOOLEAN,
        shot_saved_to_post BOOLEAN,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (outcomeId) REFERENCES outcome(id),
        FOREIGN KEY (techniqueId) REFERENCES technique(id),
        FOREIGN KEY (positionId) REFERENCES position(id),
        FOREIGN KEY (skillTypeId) REFERENCES skillType(id),
        FOREIGN KEY (bodyPartId) REFERENCES bodyPart(id)
    );

    
    CREATE TABLE IF NOT EXISTS badBehaviour (
        eventId UUID NOT NULL,
        cardId INTEGER,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (cardId) REFERENCES card(id)
    );


    CREATE TABLE IF NOT EXISTS ownGoalFor (
        eventId UUID NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );

    
    CREATE TABLE IF NOT EXISTS playerOn (
        eventId UUID NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );


    CREATE TABLE IF NOT EXISTS playerOff (
        eventId UUID NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );

    
    CREATE TABLE IF NOT EXISTS shield (
        eventId UUID NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );

    
    CREATE TABLE IF NOT EXISTS fiftyFifty (
        eventId UUID NOT NULL,
        outcomeId INTEGER NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (outcomeId) REFERENCES outcome(id)
    );

    
    CREATE TABLE IF NOT EXISTS startingEleven (
        eventId UUID NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );

    
    CREATE TABLE IF NOT EXISTS error (
        eventId UUID NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );

    
    CREATE TABLE IF NOT EXISTS miscontrol (
        eventId UUID NOT NULL,
        aerial_won BOOLEAN,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );


    CREATE TABLE IF NOT EXISTS dribbledPast (
        eventId UUID NOT NULL,
        playerId INTEGER NOT NULL,
        matchId INTEGER NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (playerId) REFERENCES player(id)
    );

    
    CREATE TABLE IF NOT EXISTS injuryStoppage (
        eventId UUID NOT NULL,
        in_chain BOOLEAN,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );


    CREATE TABLE IF NOT EXISTS refereeBallDrop (
        eventId UUID NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );

    
    CREATE TABLE IF NOT EXISTS ballReceipt (
        eventId UUID NOT NULL,
        outcomeId INTEGER,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (outcomeId) REFERENCES outcome(id)
    );


    CREATE TABLE IF NOT EXISTS carry (
        eventId UUID NOT NULL,
        end_location FLOAT[] NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id)
    );    
    

    CREATE TABLE IF NOT EXISTS freeze_frame (
        eventId UUID NOT NULL,
        location FLOAT[] NOT NULL,
        playerId INTEGER NOT NULL,
        positionId INTEGER NOT NULL,
        teammate BOOLEAN NOT NULL,

        PRIMARY KEY (eventId),

        FOREIGN KEY (eventId) REFERENCES events(id),
        FOREIGN KEY (playerId) REFERENCES player(id),
        FOREIGN KEY (positionId) REFERENCES position(id)
    );

    
    CREATE TABLE IF NOT EXISTS shots (
        eventId UUID NOT NULL,
        matchId INTEGER NOT NULL,
        playerId INTEGER NOT NULL,
        teamId INTEGER NOT NULL,
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
        

        PRIMARY KEY (eventId),

        FOREIGN KEY  (matchId) REFERENCES matches(match_id),
        FOREIGN KEY  (playerId) REFERENCES player(id),
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

    
    CREATE TABLE IF NOT EXISTS tacticalShift (
        eventTacticsId UUID NOT NULL,

        PRIMARY KEY (eventTacticsId),

        FOREIGN KEY (eventTacticsId) REFERENCES eventTactics(event_id)
    );


    CREATE TABLE IF NOT EXISTS eventLineup (
        event_tactics_id UUID NOT NULL,
        player_id INTEGER NOT NULL,
        position_id INTEGER NOT NULL,
        jersey_number INTEGER NOT NULL,

        PRIMARY KEY (event_tactics_id),

        FOREIGN KEY (event_tactics_id) references eventTactics(event_id)
            ON DELETE CASCADE,

        FOREIGN KEY (player_id) references player(id),

        FOREIGN KEY (position_id) references position(id)
    );
    """

    eventType_table_dml = """
    INSERT INTO eventType (id, name)
    VALUES (%s, %s)
    ON CONFLICT (id) DO NOTHING;
    """

    eventPlayPattern_table_dml = """
    INSERT INTO eventPlayPattern (id, name)
    VALUES (%s, %s)
    ON CONFLICT (id) DO NOTHING;  
    """

    player_table_dml = """
    INSERT INTO player (id, name)
    VALUES (%s, %s)
    ON CONFLICT (id) DO NOTHING;  
    """

    position_table_dml = """
    INSERT INTO position (id, name)
    VALUES (%s, %s)
    ON CONFLICT (id) DO NOTHING;  
    """

    pass_height_table_dml = """
    INSERT INTO passHeight (id, name)
    VALUES (%s, %s)
    ON CONFLICT (id) DO NOTHING;  
    """

    body_part_table_dml = """
    INSERT INTO bodyPart (id, name)
    VALUES (%s, %s)
    ON CONFLICT (id) DO NOTHING;  
    """

    technique_table_dml = """
    INSERT INTO technique (id, name)
    VALUES (%s, %s)
    ON CONFLICT (id) DO NOTHING;  
    """

    skill_type_table_dml = """
    INSERT INTO skillType (id, name)
    VALUES (%s, %s)
    ON CONFLICT (id) DO NOTHING;  
    """
    
    outcome_table_dml = """
    INSERT INTO outcome (id, name)
    VALUES (%s, %s)
    ON CONFLICT (id) DO NOTHING;  
    """

    events_table_dml = """
    INSERT INTO events (id, matchId, index, period, timestamp, minute, second,
                        type, possession, possession_team, play_pattern, 
                        team, duration, location, counterpress, off_camera,
                        under_pressure, out, playerId, positionId)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """

    pass_table_dml = """
    INSERT INTO pass (eventId, matchId, playerId, teamId, recipientId, length, angle, heightId, end_location, bodyPartId,
                      skillTypeId, outcomeId, aerial_won, switch, crossed, assisted_shot_id,
                      goal_assist, shot_assist, outswinging, techniqueId, deflected, straight, inswinging,
                      cut_back, through_ball, miscommunication, no_touch)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """
    
    ball_recovery_table_dml =  """
    INSERT INTO ballRecovery (eventId, recovery_failure, offensive)
    VALUES (%s, %s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    dispossessed_table_dml =  """
    INSERT INTO dispossessed (eventId)
    VALUES (%s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    duel_table_dml =  """
    INSERT INTO duel (eventId, outcomeId, skillTypeId)
    VALUES (%s, %s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    block_table_dml =  """
    INSERT INTO block (eventId, deflection, offensive, save_block)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    offside_table_dml =  """
    INSERT INTO offside (eventId)
    VALUES (%s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    clearance_table_dml =  """
    INSERT INTO clearance (eventId, bodyPartId, aerialWon, head, leftFoot, rightFoot, other)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    interception_table_dml =  """
    INSERT INTO interception (eventId, outcomeId)
    VALUES (%s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """
    
    dribble_table_dml =  """
    INSERT INTO dribble (eventId, playerId, matchId, outcomeId, overrun, nutmeg, no_touch)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    pressure_table_dml =  """
    INSERT INTO pressure (eventId)
    VALUES (%s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    half_start_table_dml =  """
    INSERT INTO halfStart (eventId, late_video_start)
    VALUES (%s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    half_end_table_dml =  """
    INSERT INTO halfStart (eventId)
    VALUES (%s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    substitution_table_dml =  """
    INSERT INTO substitution (eventId, outcomeId, replacementId)
    VALUES (%s, %s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    own_goal_against_table_dml =  """
    INSERT INTO ownGoalAgainst (eventId)
    VALUES (%s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    foul_won_table_dml =  """
    INSERT INTO foulWon (eventId, advantage, defensive, penalty)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    foul_committed_table_dml =  """
    INSERT INTO foulCommitted (eventId, cardId, skillTypeId, advantage, offensive, penalty)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    goal_keeper_table_dml =  """
    INSERT INTO goalKeeper (eventId, outcomeId, techniqueId, positionId, 
                            skillTypeId, bodyPartId, shot_saved_off_target, 
                            punched_out, success_in_play, lost_out, end_location, 
                            lost_in_play, shot_saved_to_post)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    bad_behaviour_table_dml =  """
    INSERT INTO badBehaviour (eventId, cardId)
    VALUES (%s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    own_goal_for_table_dml =  """
    INSERT INTO ownGoalFor (eventId)
    VALUES (%s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    player_on_table_dml =  """
    INSERT INTO playerOn (eventId)
    VALUES (%s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    player_off_table_dml =  """
    INSERT INTO playerOff (eventId)
    VALUES (%s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    shield_table_dml =  """
    INSERT INTO shield (eventId)
    VALUES (%s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    fifty_fifty_table_dml =  """
    INSERT INTO fiftyFifty (eventId, outcomeId)
    VALUES (%s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    starting_eleven_table_dml =  """
    INSERT INTO startingEleven (eventId)
    VALUES (%s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    error_table_dml =  """
    INSERT INTO error (eventId)
    VALUES (%s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    miscontrol_table_dml =  """
    INSERT INTO miscontrol (eventId, aerial_won)
    VALUES (%s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    dribbled_past_table_dml =  """
    INSERT INTO dribbledPast (eventId, playerId, matchId)
    VALUES (%s, %s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    injury_stoppage_table_dml =  """
    INSERT INTO injuryStoppage (eventId, in_chain)
    VALUES (%s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    referee_ball_drop_table_dml =  """
    INSERT INTO refereeBallDrop (eventId)
    VALUES (%s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    ball_receipt_table_dml =  """
    INSERT INTO ballReceipt (eventId, outcomeId)
    VALUES (%s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    carry_table_dml =  """
    INSERT INTO carry (eventId, end_location)
    VALUES (%s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    freeze_frame_table_dml =  """
    INSERT INTO freeze_frame (eventId, location, playerId, positionId, teammate)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    shots_table_dml =  """
    INSERT INTO shots (eventId, matchId, playerId, teamId, statsbomb_xg, end_location, key_pass_id,
                       techniqueId, first_time, bodyPartId, skillTypeId,
                       outcomeId, aerial_won, open_goal, deflected,
                       one_on_one, redirect, saved_to_post, follows_dribble,
                       saved_off_target)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (eventId) DO NOTHING;
    """

    event_tactics_table_dml =  """
    INSERT INTO eventTactics (event_id, formation)
    VALUES (%s, %s)
    ON CONFLICT (event_id) DO NOTHING;
    """

    # perhaps tactical shift points to event lineup instead of event tactics because ET doesnt provide much information
    # there always a lineup in tactical shift, buut there not always a tactical shift to have a lineup
    tactical_shift_table_dml =  """
    INSERT INTO tacticalShift (eventTacticsId)
    VALUES (%s)
    ON CONFLICT (eventTacticsId) DO NOTHING;
    """

    event_lineup_table_dml =  """
    INSERT INTO eventLineup (event_tactics_id, player_id, position_id, jersey_number)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (event_tactics_id) DO NOTHING;
    """
    
    relevantEventFiles = getRelevantFiles()

    with conn.cursor() as cursor:

        # Create the events and its related tables
        cursor.execute(events_ddl)

        for file in os.listdir(path_to_data):
            if file in relevantEventFiles:
                if file.endswith(".json"):
                    file_path = os.path.join(path_to_data, file)
                    with open(file_path, 'r') as f:
                        events_table_data = json.load(f)

                        for entity in events_table_data:

                            # Populating eventType
                            psycopg2.extras.execute_batch(cursor, eventType_table_dml, [
                                (entity.get('type', {}).get('id'), entity.get('type', {}).get('name')) # for entity in events_table_data
                            ])
            
                            psycopg2.extras.execute_batch(cursor, eventPlayPattern_table_dml, [
                                (entity.get('play_pattern', {}).get('id'), entity.get('play_pattern', {}).get('name')) # for entity in events_table_data
                            ])

                            if 'pass' in entity:

                                psycopg2.extras.execute_batch(cursor, pass_height_table_dml, [
                                    (entity.get('pass', {}).get('height', {}).get('id'), entity.get('pass', {}).get('height', {}).get('name'))
                                ])
   

                                if 'body_part' in entity['pass']:
                                    psycopg2.extras.execute_batch(cursor, body_part_table_dml, [
                                        (entity.get('pass', {}).get('body_part', {}).get('id'), entity.get('pass', {}).get('body_part', {}).get('name'))
                                    ]) 
            

                                if 'technique' in entity['pass']:

                                    psycopg2.extras.execute_batch(cursor, technique_table_dml, [
                                        (entity.get('pass', {}).get('technique', {}).get('id'), entity.get('pass', {}).get('technique', {}).get('name'))
                                    ]) 
          
                                
                                if 'type' in entity['pass']:
                                    psycopg2.extras.execute_batch(cursor, skill_type_table_dml, [
                                        (entity.get('pass', {}).get('type', {}).get('id'), entity.get('pass', {}).get('type', {}).get('name'))
                                    ])
           

                                if 'outcome' in entity['pass']:
                                    psycopg2.extras.execute_batch(cursor, outcome_table_dml, [
                                        (entity.get('pass', {}).get('outcome', {}).get('id'), entity.get('pass', {}).get('outcome', {}).get('name'))
                                    ])
  
                            if 'shot' in entity:

                                if 'body_part' in entity['shot']:
                                    psycopg2.extras.execute_batch(cursor, body_part_table_dml, [
                                        (entity.get('shot', {}).get('body_part', {}).get('id'), entity.get('shot', {}).get('body_part', {}).get('name'))
                                    ])

                                if 'technique' in entity['shot']:
                                    psycopg2.extras.execute_batch(cursor, technique_table_dml, [
                                        (entity.get('shot', {}).get('technique', {}).get('id'), entity.get('shot', {}).get('technique', {}).get('name'))
                                    ])
        
                                if 'type' in entity['shot']:
                                    psycopg2.extras.execute_batch(cursor, skill_type_table_dml, [
                                        (entity.get('shot', {}).get('type', {}).get('id'), entity.get('shot', {}).get('type', {}).get('name'))
                                    ])

                                if 'outcome' in entity['shot']:
                                    psycopg2.extras.execute_batch(cursor, outcome_table_dml, [
                                        (entity.get('shot', {}).get('outcome', {}).get('id'), entity.get('shot', {}).get('outcome', {}).get('name'))
                                    ])

                            if 'goalkeeper'in entity:
                                
                                if 'body_part' in entity['goalkeeper']:
                                    psycopg2.extras.execute_batch(cursor, body_part_table_dml, [
                                        (entity.get('goalkeeper', {}).get('body_part', {}).get('id'), entity.get('goalkeeper', {}).get('body_part', {}).get('name'))
                                    ])

                                if 'technique' in entity['goalkeeper']:
                                    psycopg2.extras.execute_batch(cursor, technique_table_dml, [
                                        (entity.get('goalkeeper', {}).get('technique', {}).get('id'), entity.get('goalkeeper', {}).get('technique', {}).get('name'))
                                    ])
                                
                                if 'type' in entity['goalkeeper']:
                                    psycopg2.extras.execute_batch(cursor, skill_type_table_dml, [
                                        (entity.get('goalkeeper', {}).get('type', {}).get('id'), entity.get('goalkeeper', {}).get('type', {}).get('name'))
                                    ])
                                
                                if 'outcome' in entity['goalkeeper']:
                                    psycopg2.extras.execute_batch(cursor, outcome_table_dml, [
                                        (entity.get('goalkeeper', {}).get('outcome', {}).get('id'), entity.get('goalkeeper', {}).get('outcome', {}).get('name'))
                                    ])

                                if 'position' in entity['goalkeeper']:
                                    psycopg2.extras.execute_batch(cursor, position_table_dml, [
                                            (entity.get('goalkeeper', {}).get('position', {}).get('id'), entity.get('goalkeeper', {}).get('position', {}).get('name'))
                                    ])

                            if 'duel' in entity:

                                if 'type' in entity['duel']:
                                    psycopg2.extras.execute_batch(cursor, skill_type_table_dml, [
                                        (entity.get('duel', {}).get('type', {}).get('id'), entity.get('duel', {}).get('type', {}).get('name'))
                                    ])
                                    
                                if 'outcome' in entity['duel']:
                                    psycopg2.extras.execute_batch(cursor, outcome_table_dml, [
                                        (entity.get('duel', {}).get('outcome', {}).get('id'), entity.get('duel', {}).get('outcome', {}).get('name'))
                                    ])                              

                            if 'foul_committed' in entity:
                                
                                if 'type' in entity['foul_committed']:
                                    psycopg2.extras.execute_batch(cursor, skill_type_table_dml, [
                                        (entity.get('foul_committed', {}).get('type', {}).get('id'), entity.get('foul_committed', {}).get('type', {}).get('name'))
                                    ])                           

                            if 'clearance'in entity:
                                psycopg2.extras.execute_batch(cursor, body_part_table_dml, [
                                    (entity.get('clearance', {}).get('body_part', {}).get('id'), entity.get('clearance', {}).get('body_part', {}).get('name'))
                                ])

                            if 'ball_receipt' in entity:

                                if 'outcome' in entity['ball_receipt']:
                                    psycopg2.extras.execute_batch(cursor, outcome_table_dml, [
                                        (entity.get('ball_receipt', {}).get('outcome', {}).get('id'), entity.get('ball_receipt', {}).get('outcome', {}).get('name'))
                                    ])                            

                            if 'dribble' in entity:

                                if 'outcome' in entity['dribble']:
                                    psycopg2.extras.execute_batch(cursor, outcome_table_dml, [
                                        (entity.get('dribble', {}).get('outcome', {}).get('id'), entity.get('dribble', {}).get('outcome', {}).get('name'))
                                    ])

                            if 'interception' in entity:

                                if 'outcome' in entity['interception']:
                                    psycopg2.extras.execute_batch(cursor, outcome_table_dml, [
                                        (entity.get('interception', {}).get('outcome', {}).get('id'), entity.get('interception', {}).get('outcome', {}).get('name'))
                                    ])

                            if 'substitution' in entity:

                                if 'outcome' in entity['substitution']:
                                    psycopg2.extras.execute_batch(cursor, outcome_table_dml, [
                                        (entity.get('substitution', {}).get('outcome', {}).get('id'), entity.get('substitution', {}).get('outcome', {}).get('name'))
                                    ])

                                # psycopg2.extras.execute_batch(cursor, player_table_dml, [
                                #         (entity.get('substitution', {}).get('replacement', {}).get('id'), entity.get('replacement', {}).get('replacement', {}).get('name'))
                                # ])
                            
                            if '50_50' in entity:

                                if 'outcome' in entity['50_50']:
                                    psycopg2.extras.execute_batch(cursor, outcome_table_dml, [
                                        (entity.get('50_50', {}).get('outcome', {}).get('id'), entity.get('50_50', {}).get('outcome', {}).get('name'))
                                    ])                          

                            # Populating player and position table via tactics -> lineups
                            # if 'tactics' in entity:
                            #     for lineup_entity in (entity['tactics']['lineup']):

                            #         psycopg2.extras.execute_batch(cursor, player_table_dml, [
                            #             (lineup_entity.get('player', {}).get('id'), lineup_entity.get('player', {}).get('name'))
                            #         ]) 
                            #         # cursor.execute(player_table_dml, (
                            #         # lineup_entity['player']['id'],
                            #         # lineup_entity['player']['name']))

                            #         psycopg2.extras.execute_batch(cursor, position_table_dml, [
                            #             (lineup_entity.get('position', {}).get('id'), lineup_entity.get('position', {}).get('name'))
                            #         ])
                            #         # cursor.execute(position_table_dml, (
                            #         # lineup_entity['position']['id'],
                            #         # lineup_entity['position']['name']))

                            if 'player' in entity:
                                    
                                psycopg2.extras.execute_batch(cursor, player_table_dml, [
                                    (entity.get('player', {}).get('id'), entity.get('player', {}).get('name'))
                                ])


                                psycopg2.extras.execute_batch(cursor, position_table_dml, [
                                    (entity.get('position', {}).get('id'), entity.get('position', {}).get('name'))
                                ])
                            
                            
                            psycopg2.extras.execute_batch(cursor, events_table_dml, [(
                            entity['id'],
                            int(file.split(".")[0]),
                            entity['index'],
                            entity['period'],
                            entity['timestamp'],
                            entity['minute'],
                            entity['second'],
                            entity['type']['id'],
                            entity['possession'],
                            entity['possession_team']['id'],
                            entity['play_pattern']['id'],
                            entity['team']['id'],
                            entity.get('duration', None),
                            entity.get('location', None),
                            entity.get('counterpress', None),
                            entity.get('off_camera', None),
                            entity.get('under_pressure', None),
                            entity.get('out', None),
                            entity.get('player', {}).get('id', None),
                            entity.get('position', {}).get('id', None)
                            )])
        
                  
                            if 'Ball Recovery' == entity['type']['name']:

                                psycopg2.extras.execute_batch(cursor, ball_recovery_table_dml, [
                                    (entity.get('id'),
                                     entity.get('ball_recovery', {}).get('recovery_failure', None), 
                                     entity.get('ball_recovery', {}).get('offensive', None)) 
                                ])

                            elif 'Dispossessed' == entity['type']['name']:

                                cursor.execute(dispossessed_table_dml, (entity.get('id'),))
                            
                            elif 'Duel' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, duel_table_dml, [
                                    (entity.get('id'),
                                     entity.get('duel', {}).get('outcome', {}).get('id', None), 
                                     entity.get('duel', {}).get('type', {}).get('id', None))
                                ])
                                
                            elif 'Block' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, block_table_dml, [
                                    (entity.get('id'),
                                     entity.get('block', {}).get('deflection', None), 
                                     entity.get('block', {}).get('offensive', None),
                                     entity.get('block', {}).get('save_block', None))
                                ])

                            elif 'Offside' == entity['type']['name']:
                                cursor.execute(offside_table_dml, (entity.get('id'),))
                            
                            elif 'Clearance' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, clearance_table_dml, [
                                    (entity.get('id'),
                                     entity.get('clearance', {}).get('body_part').get('id'), 
                                     entity.get('clearance', {}).get('aerial_won', None),
                                     entity.get('clearance', {}).get('head', None),
                                     entity.get('clearance', {}).get('leftFoot', None),
                                     entity.get('clearance', {}).get('rightFoot', None),
                                     entity.get('clearance', {}).get('other', None))
                                ])

                            elif 'Interception' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, interception_table_dml, [
                                    (entity.get('id'),
                                     entity.get('interception', {}).get('outcome', {}).get('id'))
                                ])
                        
                            elif 'Dribble' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, dribble_table_dml, [
                                    (entity.get('id'),
                                     entity.get('player', {}).get('id'),
                                     int(file.split(".")[0]),
                                     entity.get('dribble', {}).get('outcome', {}).get('id'),
                                     entity.get('dribble', {}).get('overrun', None),
                                     entity.get('dribble', {}).get('nutmeg', None),
                                     entity.get('dribble', {}).get('no_touch', None))
                                ])

                            elif 'Pressure' == entity['type']['name']:
                                cursor.execute(pressure_table_dml, (entity.get('id'),))
                            
                            elif 'Half Start' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, half_start_table_dml, [
                                    (entity.get('id'),
                                     entity.get('half_start', {}).get('late_video_start', None))
                                ])
                            
                            elif 'Half End' == entity['type']['name']:
                                cursor.execute(half_end_table_dml, (entity.get('id'),))
                            
                            elif 'Substitution' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, substitution_table_dml, [
                                    (entity.get('id'),
                                     entity.get('substitution', {}).get('outcome', {}).get('id'),
                                     entity.get('substitution', {}).get('replacement', {}).get('id'))
                                ])
                            
                            elif 'Own Goal Against' == entity['type']['name']:
                                cursor.execute(own_goal_against_table_dml, (entity.get('id'),))

                            elif 'Foul Won' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, foul_won_table_dml, [
                                    (entity.get('id'),
                                     entity.get('foul_won', {}).get('advantage', None),
                                     entity.get('foul_won', {}).get('defensive', None),
                                     entity.get('foul_won', {}).get('penalty', None))
                                ])

                            elif 'Foul Committed' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, foul_committed_table_dml, [
                                    (entity.get('id'),
                                     entity.get('foul_committed', {}).get('card', {}).get('id', None),
                                     entity.get('foul_committed', {}).get('type', {}).get('id', None),
                                     entity.get('foul_committed', {}).get('advantage', None),
                                     entity.get('foul_committed', {}).get('offensive', None),
                                     entity.get('foul_committed', {}).get('penalty', None))
                                ])
                            
                            elif 'Goal Keeper' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, goal_keeper_table_dml, [
                                    (entity.get('id'),
                                     entity.get('goalkeeper', {}).get('outcome', {}).get('id', None),
                                     entity.get('goalkeeper', {}).get('technique', {}).get('id', None),
                                     entity.get('goalkeeper', {}).get('position', {}).get('id', None),
                                     entity.get('goalkeeper', {}).get('type', {}).get('id'),
                                     entity.get('goalkeeper', {}).get('body_part', {}).get('id', None),
                                     entity.get('goalkeeper', {}).get('shot_saved_off_target', None),
                                     entity.get('goalkeeper', {}).get('punched_out', None),
                                     entity.get('goalkeeper', {}).get('success_in_player', None),
                                     entity.get('goalkeeper', {}).get('lost_out', None),
                                     entity.get('goalkeeper', {}).get('end_location', None),
                                     entity.get('goalkeeper', {}).get('lost_in_play', None),
                                     entity.get('goalkeeper', {}).get('shot_saved_to_post', None))
                                ])
                            
                            elif 'Bad Behaviour' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, bad_behaviour_table_dml, [
                                    (entity.get('id'),
                                     entity.get('bad_behaviour', {}).get('card', {}).get('id'))
                                ])
                            
                            elif 'Own Goal For' == entity['type']['name']:
                                cursor.execute(own_goal_for_table_dml, (entity.get('id'),))
                            
                            elif 'Player On' == entity['type']['name']:
                                cursor.execute(player_on_table_dml, (entity.get('id'),))
                            
                            elif 'Player Off' == entity['type']['name']:
                                cursor.execute(player_off_table_dml, (entity.get('id'),))
                            
                            elif 'Shield' == entity['type']['name']:
                                cursor.execute(shield_table_dml, (entity.get('id'),))
                            
                            elif '50/50' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, fifty_fifty_table_dml, [
                                    (entity.get('id'),
                                     entity.get('50_50', {}).get('outcome', {}).get('id'))
                                ])
                            
                            elif 'Starting XI' == entity['type']['name']:
                                cursor.execute(starting_eleven_table_dml, (entity.get('id'),))

                                psycopg2.extras.execute_batch(cursor, event_tactics_table_dml, [
                                    (entity.get('id'),
                                     entity.get('tactics', {}).get('formation', None))
                                ])

                                for data in entity['tactics']['lineup']:
                                    psycopg2.extras.execute_batch(cursor, event_lineup_table_dml, [
                                        (entity.get('id'),
                                        data.get('player', {}).get('id'),
                                        data.get('position', {}).get('id'),
                                        data.get('jersey_number', {}))
                                    ])

                            elif 'Error' == entity['type']['name']:
                                cursor.execute(error_table_dml, (entity.get('id'),))

                            elif 'Miscontrol' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, miscontrol_table_dml, [
                                    (entity.get('id'),
                                     entity.get('aerial_won', None))
                                ])
                            
                            elif 'Dribbled Past' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, dribbled_past_table_dml, [
                                    (entity.get('id'), 
                                     entity.get('player').get('id'),
                                     int(file.split(".")[0]))
                                    ])
                            
                            elif 'Injury Stoppage' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, injury_stoppage_table_dml, [
                                    (entity.get('id'),
                                     entity.get('in_chain', None))
                                ])
                            
                            elif 'Referee Ball-Drop' == entity['type']['name']:
                                cursor.execute(referee_ball_drop_table_dml, (entity.get('id'),))
                            
                            elif 'Ball Receipt' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, ball_receipt_table_dml, [
                                    (entity.get('id'),
                                     entity.get('ball_receipt', {}).get('outcome', {}).get('id'))
                                ])
                            
                            elif 'Carry' == entity['type']['name']:
                                psycopg2.extras.execute_batch(cursor, carry_table_dml, [
                                    (entity.get('id'),
                                     entity.get('carry', {}).get('end_location'))
                                ])
                            
                            elif 'Shot' == entity['type']['name']:


                                if 'freeze_frame' in entity['shot']:
                            
                                    for data in entity['shot']['freeze_frame']:
                                        psycopg2.extras.execute_batch(cursor, freeze_frame_table_dml, [
                                            (entity.get('id'),
                                            data.get('location'),
                                            data.get('player').get('id'),
                                            data.get('position').get('id'),
                                            data.get('teammate'))
                                        ])
                                
                                psycopg2.extras.execute_batch(cursor, shots_table_dml, [
                                    (entity.get('id'),
                                     int(file.split(".")[0]),
                                     entity.get('player').get('id'),
                                     entity.get('team').get('id'),
                                     entity.get('shot').get('statsbomb_xg'),
                                     entity.get('shot').get('end_location'),
                                     entity.get('shot').get('key_pass_id', None),
                                     entity.get('shot').get('technique', {}).get('id', None),
                                     entity.get('shot').get('first_time', None),
                                     entity.get('shot').get('body_part').get('id'),
                                     entity.get('shot').get('type', {}).get('id'),
                                     entity.get('shot').get('outcome', {}).get('id', None),
                                     entity.get('shot').get('aerial_won', None),
                                     entity.get('shot').get('open_goal', None),
                                     entity.get('shot').get('deflected', None),
                                     entity.get('shot').get('one_on_one', None),
                                     entity.get('shot').get('redirect', None),
                                     entity.get('shot').get('saved_to_post', None),
                                     entity.get('shot').get('follows_dribble', None),
                                     entity.get('shot').get('saved_off_target', None))
                                ])

                            elif 'Pass' == entity['type']['name']:
                                
                                psycopg2.extras.execute_batch(cursor, pass_table_dml, [
                                    (entity.get('id'),
                                     int(file.split(".")[0]),
                                     entity.get('player').get('id'),
                                     entity.get('team').get('id'),
                                     entity.get('pass').get('recipient', {}).get('id', None),
                                     entity.get('pass').get('length', None),
                                     entity.get('pass').get('angle', None),
                                     entity.get('pass').get('height', {}).get('id', None),
                                     entity.get('pass').get('end_location', None),
                                     entity.get('pass').get('body_part', {}).get('id', None),
                                     entity.get('pass').get('type', {}).get('id', None),
                                     entity.get('pass').get('outcome', {}).get('id', None),
                                     entity.get('pass').get('aerial_won', None),
                                     entity.get('pass').get('switch', None),
                                     entity.get('pass').get('cross', None),
                                     entity.get('pass').get('assisted_shot_id', None),
                                     entity.get('pass').get('goal_assist', None),
                                     entity.get('pass').get('shot_assist', None),
                                     entity.get('pass').get('outswinging', None),
                                     entity.get('pass').get('technique', {}).get('id', None),
                                     entity.get('pass').get('deflected', None),
                                     entity.get('pass').get('straight', None),
                                     entity.get('pass').get('inswinging', None),
                                     entity.get('pass').get('cut_back', None),
                                     entity.get('pass').get('through_ball', None),
                                     entity.get('pass').get('miscommunication', None),
                                     entity.get('pass').get('no_touch', None))
                                ])

                            elif 'Tactical Shift' == entity['type']['name']:

                                psycopg2.extras.execute_batch(cursor, event_tactics_table_dml, [
                                    (entity.get('id'),
                                     entity.get('tactics', {}).get('formation', None))
                                ])

                                for data in entity['tactics']['lineup']:
                                    psycopg2.extras.execute_batch(cursor, event_lineup_table_dml, [
                                        (entity.get('id'),
                                        data.get('player', {}).get('id'),
                                        data.get('position', {}).get('id'),
                                        data.get('jersey_number', {}))
                                    ])
                                    
                                cursor.execute(tactical_shift_table_dml, (entity.get('id'),))
                                


        conn.commit()
    print(" done!")

##########################################################################################################################
# Purpose: Function to create and populate the matches table (and related tables)

# Notes: 
#       We will only populate with entries relevant to our purpose
#       Metadata attributes are ignored
##########################################################################################################################

def setupMatches():

    print("Setting up matches ...", end = "")

    # Path to data
    path_to_data = "/home/cheesemaker/Documents/COMP3005/team_project/open-data-master/data/matches"

    # To filter on season (file)
    relevant_seasons = ["4.json", "42.json", "44.json", "90.json"]

    # To filter on competition (folder) 
    relevant_comps = ["2", "11"]

    # Create the tables (DDL)

    matches_ddl = """
    CREATE TABLE IF NOT EXISTS competitionStage (
        stage_id INTEGER NOT NULL,
        stage_name TEXT NOT NULL,

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
        countryID INTEGER NOT NULL,

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

        PRIMARY KEY (team_id),

        FOREIGN KEY (countryID) references country(country_id)
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

        FOREIGN KEY (competitionID, seasonID) references competitions(competition_id, season_id),
        FOREIGN KEY (home_teamID) references teams(team_id),
        FOREIGN KEY (away_teamID) references teams(team_id),
        FOREIGN KEY (stadiumID) references stadium(stadium_id),
        FOREIGN KEY (refereeID) references referee(referee_id)
    );
    """

    competition_stage_dml = """
    INSERT INTO competitionStage (stage_id, stage_name)
    VALUES (%s, %s) 
    ON CONFLICT (stage_id) DO NOTHING;
    """

    country_dml = """
    INSERT INTO country (country_id, country_name)
    VALUES (%s, %s) 
    ON CONFLICT (country_id) DO NOTHING;
    """

    stadium_dml = """
    INSERT INTO stadium (stadium_id, stadium_name, countryID)
    VALUES (%s, %s, %s) 
    ON CONFLICT (stadium_id) DO NOTHING;
    """

    referee_dml = """
    INSERT INTO referee (referee_id, referee_name, countryID)
    VALUES (%s, %s, %s) 
    ON CONFLICT (referee_id) DO NOTHING;
    """

    manager_dml = """
    INSERT INTO manager (manager_id, manager_name, manager_nickname, manager_dob, countryID)
    VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT (manager_id) DO NOTHING;
    """

    teams_dml = """
    INSERT INTO teams (team_id, team_name, team_gender, team_group, countryID)
    VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT (team_id) DO NOTHING;
    """

    team_manager_link_dml = """
    INSERT INTO team_manager_link (team_id, manager_id)
    VALUES (%s, %s) 
    ON CONFLICT (team_id, manager_id) DO NOTHING;
    """

    matches_dml = """
    INSERT INTO matches (match_id, match_date, kick_off, competitionID, seasonID, home_teamID, away_teamID, home_score, away_score, match_week, competition_stageID, stadiumID, refereeID)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT (match_id) DO NOTHING;
    """
    
    with conn.cursor() as cursor:

        # Create the matches and its related tables
        cursor.execute(matches_ddl)
        
        for folder in os.listdir(path_to_data):
            # Filter on folder (PL, LL)
            if (folder in relevant_comps):
                folder_path = os.path.join(path_to_data, folder)
                for file in os.listdir(folder_path):
                    file_path = os.path.join(folder_path, file)
                    # Filter on file 
                    if (file in relevant_seasons):
                        with open(file_path, 'r') as f:
                            matches_table_data = json.load(f)

                            for match in matches_table_data:

                                # competitionStage
                                cursor.execute(competition_stage_dml, (
                                match['competition_stage']['id'],
                                match['competition_stage']['name']))

                                # country
                                both_teams = ['home_team', 'away_team']

                                for team_type in both_teams:
                                    team = match[team_type]
                                    cursor.execute(country_dml, (
                                    team['country']['id'],
                                    team['country']['name']))

                                    cursor.execute(teams_dml, (
                                    team[team_type + '_id'],
                                    team[team_type + '_name'],
                                    team[team_type + '_gender'],
                                    team[team_type + '_group'],
                                    team['country']['id']))

                                    for manager in team.get('managers', []):
                                        cursor.execute(country_dml, (
                                        manager['country']['id'],
                                        manager['country']['name']))

                                        cursor.execute(manager_dml, (
                                        manager['id'],
                                        manager['name'],
                                        manager['nickname'],
                                        manager['dob'],
                                        manager['country']['id']))

                                        cursor.execute(team_manager_link_dml, (
                                        team[team_type + '_id'],
                                        manager['id']))

                                if 'stadium' in match:
                                    cursor.execute(country_dml, (
                                    match['stadium']['country']['id'],
                                    match['stadium']['country']['name']))

                                    cursor.execute(stadium_dml, (
                                    match['stadium']['id'],
                                    match['stadium']['name'],
                                    match['stadium']['country']['id']))

                                if 'referee' in match:
                                    cursor.execute(country_dml, (
                                    match['referee']['country']['id'],
                                    match['referee']['country']['name']))

                                    cursor.execute(referee_dml, (
                                    match['referee']['id'],
                                    match['referee']['name'],
                                    match['referee']['country']['id']))
                                
                                cursor.execute(matches_dml, (
                                match['match_id'],
                                match['match_date'],
                                match['kick_off'],
                                match['competition']['competition_id'],
                                match['season']['season_id'],
                                match['home_team']['home_team_id'],
                                match['away_team']['away_team_id'],
                                match['home_score'],
                                match['away_score'],
                                match['match_week'],
                                match['competition_stage']['id'],
                                match.get('stadium', {}).get('id', None),
                                match.get('referee', {}).get('id', None)))
        
        conn.commit()

    print (" done!")

##########################################################################################################################
# Setup function
##########################################################################################################################

def setupLineups():
    print("Setting up lineups ...", end = "")

    # Path to data
    path_to_data = "/home/cheesemaker/Documents/COMP3005/team_project/open-data-master/data/lineups"

    # Create the tables (DDL)

    linueup_ddl = """
    CREATE TABLE IF NOT EXISTS player (
        id INTEGER NOT NULL,
        name TEXT NOT NULL,
        nickname TEXT,

        PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS position (
        id INTEGER NOT NULL,
        name TEXT NOT NULL,
        
        PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS card (
        id INTEGER NOT NULL,
        type TEXT NOT NULL,
        
        PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS player_card_link (
        match_id INTEGER NOT NULL,
        player_id INTEGER NOT NULL,
        card_id INTEGER NOT NULL,
        card_issued_time TEXT,
        card_reason TEXT,
        period INTEGER,
                
        PRIMARY KEY (match_id, player_id, card_id),

        FOREIGN KEY (match_id) REFERENCES matches(match_id),
        FOREIGN KEY (player_id) REFERENCES player(id),
        FOREIGN KEY (card_id) REFERENCES card(id)
    );

    CREATE TABLE IF NOT EXISTS player_position_link (
        match_id INTEGER NOT NULL,
        player_id INTEGER NOT NULL,
        position_id INTEGER NOT NULL,
        position_from TEXT,
        position_to TEXT,
        from_period INTEGER,
        to_period INTEGER,
        start_reason TEXT,
        end_reason TEXT,
        
        PRIMARY KEY (match_id, player_id, position_id),

        FOREIGN KEY (match_id) REFERENCES matches(match_id),
        FOREIGN KEY (player_id) REFERENCES player(id),
        FOREIGN KEY (position_id) REFERENCES position(id)
    );

    CREATE TABLE IF NOT EXISTS lineup (
        match_id INTEGER NOT NULL,
        team_id INTEGER NOT NULL,
        player_id INTEGER NOT NULL,
        country_id INTEGER, 
        jersey_number INTEGER,
        
        PRIMARY KEY (match_id, team_id, player_id),

        FOREIGN KEY (match_id) REFERENCES matches(match_id),
        FOREIGN KEY (team_id) REFERENCES teams(team_id),
        FOREIGN KEY (player_id) REFERENCES player(id),
        FOREIGN KEY (country_id) REFERENCES country(country_id)
    );
    """

    player_table_dml = """
    INSERT INTO player (id, name, nickname)
    VALUES (%s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """

    position_table_dml = """
    INSERT INTO position (id, name)
    VALUES (%s, %s)
    ON CONFLICT (id) DO NOTHING;
    """

    card_table_dml = """
    INSERT INTO card (id, type)
    VALUES (%s, %s)
    ON CONFLICT (id) DO NOTHING;
    """

    country_table_dml = """
    INSERT INTO country (country_id, country_name)
    VALUES (%s, %s) 
    ON CONFLICT (country_id) DO NOTHING;
    """

    player_card_link_table_dml = """
    INSERT INTO player_card_link (match_id, player_id, card_id, card_issued_time, card_reason, period)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (match_id, player_id, card_id) DO NOTHING;
    """

    player_position_link_table_dml = """
    INSERT INTO player_position_link (match_id, player_id, position_id, position_from, position_to,
                                 from_period, to_period, start_reason, end_reason)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (match_id, player_id, position_id) DO NOTHING;
    """

    lineup_table_dml = """
    INSERT INTO lineup (match_id, team_id, player_id, country_id, jersey_number)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (match_id, team_id, player_id) DO NOTHING;
    """

    relevantEventFiles = getRelevantFiles()

    with conn.cursor() as cursor:

        # Create the events and its related tables
        cursor.execute(linueup_ddl)


        for file in os.listdir(path_to_data):
            if file in relevantEventFiles:
                if file.endswith(".json"):
                    file_path = os.path.join(path_to_data, file)
                    with open(file_path, 'r') as f:
                        lineup_table_data = json.load(f)

                        # print(file_path)
                        for entity in lineup_table_data:

                            
                            for lineup_entity in entity['lineup']:
                                psycopg2.extras.execute_batch(cursor, player_table_dml, [
                                    (lineup_entity.get('player_id'), lineup_entity.get('player_name'), lineup_entity.get('player_nickname', None)) # for entity in events_table_data
                                ])

                                # print(lineup_entity.get('country'))
                                psycopg2.extras.execute_batch(cursor, country_table_dml, [
                                        (lineup_entity.get('country').get('id'), lineup_entity.get('country').get('name') ) # for entity in events_table_data
                                ])

                                for position in lineup_entity['positions']:

                                    psycopg2.extras.execute_batch(cursor, position_table_dml, [
                                        (position.get('position_id'), position.get('position')) # for entity in events_table_data
                                    ])

                                    psycopg2.extras.execute_batch(cursor, player_position_link_table_dml, [
                                        (int(file.split(".")[0]), lineup_entity.get('player_id'), position.get('position_id'),
                                          position.get('from'), position.get('to'), position.get('from_period'),
                                          position.get('to_period'), position.get('start_reason'), position.get('end_reason'))  # for entity in events_table_data
                                    ])

                                for card in lineup_entity['cards']:

                                    id = None
                                    if card['card_type'] == 'Yellow Card':
                                        psycopg2.extras.execute_batch(cursor, card_table_dml, [
                                            (7, card.get('card_type')) # for entity in events_table_data
                                        ])
                                        id = 7

                                    elif card['card_type'] == 'Second Yellow':
                                        psycopg2.extras.execute_batch(cursor, card_table_dml, [
                                            (6, card.get('card_type')) # for entity in events_table_data
                                        ])
                                        id = 6

                                    elif card['card_type'] == 'Red Card':
                                        psycopg2.extras.execute_batch(cursor, card_table_dml, [
                                            (5, card.get('card_type')) # for entity in events_table_data
                                        ])
                                        id = 5

                                    psycopg2.extras.execute_batch(cursor, player_card_link_table_dml, [
                                        (int(file.split(".")[0]), lineup_entity.get('player_id'), id, card.get('time'), 
                                        card.get('reason'), card.get('period'))  # for entity in events_table_data
                                    ])
                                
                                psycopg2.extras.execute_batch(cursor, lineup_table_dml, [
                                        (int(file.split(".")[0]), entity.get('team_id'), lineup_entity.get('player_id', None),
                                         lineup_entity.get('country', {}).get('id'), lineup_entity.get('jersey_number', None)) # for entity in events_table_data
                                ])
                                    
        conn.commit()
        print (" done!")
               



##########################################################################################################################
# Main function
##########################################################################################################################

def main():

    setupCompetitions()
    setupMatches()
    setupLineups()
    setupEvents()
    conn.close()

if __name__ == "__main__":
    main()