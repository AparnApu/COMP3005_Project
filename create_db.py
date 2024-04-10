###############################################################################################
# Aparna Apu
# 101194937

# ELie Feghali
# 101185489

# COMP 3005 Project (Version 1)

# Citations
# https://github.com/statsbomb/open-data/tree/0067cae166a56aa80b2ef18f61e16158d6a7359a
# https://www.psycopg.org/psycopg3/docs/basic/install.html
# https://www.psycopg.org/psycopg3/docs/basic/usage.html 
# https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-uuid/
# https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-time/
# 
# 
# 
# 
# 
###############################################################################################

import os
import json
import psycopg

# Database connection parameters
dbname = "Soccer"
user = "postgres"
host = "localhost"
port = "5432"

db_password = "postgres"

# Connect to the database
conn = psycopg.connect(f"dbname={dbname} user={user} host={host} port={port} password={db_password}")


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


    # print(len(relevant_match_ids))
                        
    return relevant_match_ids


###############################################################################################
# Create the tables
# TO DO - filter season
###############################################################################################

def setupCompetitions():

    # Path to data
    path_to_data = "/home/cheesemaker/Documents/COMP3005/team_project/open-data-master/data/competitions.json"

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


###############################################################################################
# Create the tables
###############################################################################################

def setupEvents():

    # Path to data
    path_to_data = "/home/cheesemaker/Documents/COMP3005/team_project/open-data-master/data/events"


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


    CREATE TABLE IF NOT EXISTS pass (
        id INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS events (
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
        duration FLOAT,
        location FLOAT[],
        carry_end_location FLOAT[],
        pass

        PRIMARY KEY (id),

        FOREIGN KEY (type) REFERENCES eventType(id),
        FOREIGN KEY (possession_team) REFERENCES eventTeams(id),
        FOREIGN KEY (play_pattern) REFERENCES eventPlayPattern(id),
        FOREIGN KEY (team) REFERENCES eventTeams(id)
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

    eventTeams_table_dml = """
    INSERT INTO eventTeams (id, name)
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

    events_table_dml = """
    INSERT INTO events (id, index, period, timestamp, minute, second, type, possession, possession_team, play_pattern, team, duration)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """

    relevantEventFiles = getRelevantFiles()

    with conn.cursor() as cursor:

        # Create the events and its related tables
        cursor.execute(events_ddl)

        print ("Iterating through files now")

        for file in os.listdir(path_to_data):
            if file in relevantEventFiles:
                if file.endswith(".json"):
                    file_path = os.path.join(path_to_data, file)
                    with open(file_path, 'r') as file:
                        events_table_data = json.load(file)

                        for entity in events_table_data:
                            cursor.execute(eventType_table_dml, (
                            entity['type']['id'],
                            entity['type']['name']))

                            cursor.execute(eventPlayPattern_table_dml, (
                            entity['play_pattern']['id'],
                            entity['play_pattern']['name']))

                            cursor.execute(eventTeams_table_dml, (
                            entity['team']['id'],
                            entity['team']['name']))

                            if 'tactics' in entity:
                                for lineup_entity in (entity['tactics']['lineup']):
                                
                                    cursor.execute(player_table_dml, (
                                    lineup_entity['player']['id'],
                                    lineup_entity['player']['name']))

                                    cursor.execute(position_table_dml, (
                                    lineup_entity['position']['id'],
                                    lineup_entity['position']['name']))
                            
                            cursor.execute(events_table_dml, (
                            entity['id'],
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
                            entity['duration']))
                            


        conn.commit()

def main():

    setupCompetitions()
    setupEvents()
    conn.close()

if __name__ == "__main__":
    main()