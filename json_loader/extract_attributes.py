###############################################################################################
# Aparna Apu
# 101194937

# Elie Feghali
# 101185489

# COMP 3005 Project (Version 1)
# Helper Functions

# Citations
# https://stackoverflow.com/questions/25231989/how-to-check-if-a-variable-is-a-dictionary-in-python
###############################################################################################

import os
import json

###############################################################################################
# Get files relevant to our seasons
    # La Liga 2018/2019, 2019/2020, 2020/2021 and Premier League 2003/2004

# Match ID's correspond to event folder JSON ID's
###############################################################################################

def getRelevantFiles():

    path_to_data = "/home/student/Documents/comp3005/project_v1/open-data/data/matches"

    # Seasons
    # 2018/2019, 2019/2020, 2020/2021, 2003/2004
    relevant_seasons = ["44.json", "4.json", "42.json", "90.json"]
    
    # Leagues
    # La Liga and Premier League
    relevant_comp_id = ["2", "11"]

    # Relevant files 
    relevant_match_ids = []

    for folder in os.listdir(path_to_data):
        # Filter on folder (PL, LL)
        if (folder in relevant_comp_id):
            folder_path = os.path.join(path_to_data, folder)

            for file in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file)
                # Filter on file 
                if (file in relevant_seasons):
                    with open(file_path, 'r') as f:
                        matches_table_data = json.load(f)

                        for match in matches_table_data:
                            relevant_match_ids.append(str(match['match_id']) + ".json")
                        
    return relevant_match_ids

###############################################################################################
# Get all attributes for a type of file
###############################################################################################

def extract_attributes():

    path_to_data = "/home/student/Documents/comp3005/project_v1/open-data/data/events"
    
    relevant_event_files = getRelevantFiles()

    print("Iterating through the files ...")

    attributes = []

    # Recursive helper
    def add_keys_from_dict(data_dict, prefix = ''):
        for key, value in data_dict.items():
            if (prefix != ''):
                path = prefix + '.' + key
            else:
                path = key

            # Prevent dupliactes from being added
            if path not in attributes:
                attributes.append(path)

            # Check if it is nested/dictionary
            # If it is, then there is more than just one simple attribute
            # So recursively call the function to add the innards too
            if isinstance(value, dict):
                add_keys_from_dict(value, path)
            # Check is dictionary
            # Make sure there is a value
            # And then also make sure that the first element is a dictionary so that its not []
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                add_keys_from_dict(value[0], path)

    # Loop through each file in the folder we are in (events, matches, lineups)
    # But only loop through the ones we actually care about
    for file_name in relevant_event_files:
        file_path = os.path.join(path_to_data, file_name)
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                # Load the data from the file
                events_table_data = json.load(file) 
                # Now for each entity, get the key and add it to the list
                # We use the above helper for this bit
                for event in events_table_data:
                    add_keys_from_dict(event)

    # Print the attributes
    # Use some indentation if they are nested
    # Not perfect, can be refined
    for attribute in attributes:
        if ("." in attribute):
            print("   " + attribute)
        else:
            print(attribute)

# Call the functions
extract_attributes()