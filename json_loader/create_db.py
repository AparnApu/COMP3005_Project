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
dbname = "Soccer"
user = "postgres"
host = "localhost"
port = "5432"

# If running this code, you may have to change this variable
db_password = "postgres"

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
    path_to_data = "/home/student/Documents/comp3005/project_v1/open-data/data/matches"

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
    path_to_data = "/home/student/Documents/comp3005/project_v1/open-data/data/competitions.json"

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
    path_to_data = "/home/student/Documents/comp3005/project_v1/open-data/data/events"

    # ELIE PUT YOUR CODE HERE
    
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
    path_to_data = "/home/student/Documents/comp3005/project_v1/open-data/data/matches"

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
# Main function
##########################################################################################################################

def main():

    setupCompetitions()
    #setupEvents()
    setupMatches()
    conn.close()

if __name__ == "__main__":
    main()