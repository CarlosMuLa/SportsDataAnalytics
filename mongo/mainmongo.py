import requests
import os
import datetime


MONGO_BASE_URL = "http://localhost:8000"


functions = {"1": "Match History","2":"Player Injuries","3":"Teams",
             "4": "Upcoming Matches","5":"Match Result","6":"Recent Matches for specific Team",
             "7":"Past Matches for specific Team","8":"Player Tranfers", "9":"Awards by: ",
             "10":"Player Value","11":"Match Officials", "12":"Exit"}


def main():
    for key, value in functions.items():
        print(f"{key} -- {value}")
    
    choice = input("Enter the number of the query you want to execute: ")
    while True:
        if choice == '1':
            match_history()
        elif choice == '2':
            player = str(input("Enter the player name: "))
            player_injuries(player)
        elif choice == '3':
         getTeams()
        elif choice == '4':
            upcoming_matches()
        elif choice == '5':
            match_result()
        elif choice == '6':
            team = input("Enter the team name: ")
            recent_matches(team)
        elif choice == '7':
            team = input("Enter the team name: ")
            past_matches(team)
        elif choice == '8':
            player = input("Enter the player name: ")
            player_transfers(player)
        elif choice == '9':
            awarded = input("Enter the team/player name: ")
            awards(awarded)
        elif choice == '10':
            player = input("Enter the player name: ")
            player_value(player)
        elif choice == '11':
            match_officials()
        elif choice == '12':
            exit()



def match_history():
    suffix = "/matches"
    endpoint = MONGO_BASE_URL + suffix
    response = requests.get(endpoint)
    print(response.json())
    if response.ok:
        json_data = response.json()
        for data in json_data:
            print(data)
            print("="*50)
    else:
        print(f"Error: {response.status_code}")


def player_injuries(player):
    suffix = "/player_injuries"
    endpoint = MONGO_BASE_URL + suffix
    params = {"player_name": player}
    print(params)
    response = requests.get(endpoint, params=params)
    if response.ok:
        json_data = response.json()
        print(json_data)
    else:
        print(f"Error: {response.status_code}")


def get_past_matches(limit: int = 5):
    suffix = "/matches"
    endpoint = MONGO_BASE_URL + suffix
    today = datetime.now().isoformat()
    
    params = {
        "query": {
            "date": {"$lt": today},
            "status": "Finished"
        },
        "sort": {"date": -1},
        "limit": limit
    }
    try:
        response = requests.get(endpoint, params=params)
        if response.ok:
            return response.json()
        else:
            print(f"Error fetching matches: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None

def getTeams():
    suffix = "/teams"
    endpoint = MONGO_BASE_URL + suffix
    try:
        response = requests.get(endpoint)
        if response.ok:
             print(response.json())
        else:
            print(f"Error fetching matches: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None
    


def upcoming_matches():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    suffix = "/upcoming_matches"
    endpoint = MONGO_BASE_URL + suffix
    params = {
        "status": "Scheduled"
    }
    try:
        response = requests.get(endpoint, params=params)
        if response.ok:
            print( response.json())
        else:
            print(f"Error fetching matches: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None



def match_result():
    suffix = "/matches_score"
    endpoint = MONGO_BASE_URL + suffix
    params = {
        "status": "Finished"
    }
    try:
        response = requests.get(endpoint, params=params)
        if response.ok:
            print("="*50)
            for data in response.json():
                print(data.get("home_team_name"),data.get("away_team_name"),data.get("score"))
                print("="*50)
        else:
            print(f"Error fetching matches: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None

def recent_matches(team):
    suffix ="/matches_team"
    endpoint = MONGO_BASE_URL + suffix
    params = {
        "team_name": team
    }
    try:
        response = requests.get(endpoint, params=params)
        if response.ok:
            print(response.json())
        else:
            print(f"Error fetching matches: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None



def past_matches(team):
    suffix ="/matches_team_all"
    endpoint = MONGO_BASE_URL + suffix
    params = {
        "team_name": team
    }
    try:
        response = requests.get(endpoint, params=params)
        if response.ok:
            print(response.json())
        else:
            print(f"Error fetching matches: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None

def player_transfers(player):
    suffix = "/player_transfers"
    endpoint = MONGO_BASE_URL + suffix
    params = {
        "player_name": player
    }
    try:
        response = requests.get(endpoint, params=params)
        if response.ok:
            print(response.json())
        else:
            print(f"Error fetching matches: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None
def awards(awarded):
    suffix = "/awards"
    endpoint = MONGO_BASE_URL + suffix
    params = {
        "awarded": awarded
    }
    try:
        response = requests.get(endpoint, params=params)
        if response.ok:
            print(response.json())
        else:
            print(f"Error fetching matches: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None

def player_value(player):
    suffix = "/player_values"
    endpoint = MONGO_BASE_URL + suffix
    params = {
        "player_name": player
    }
    try:
        response = requests.get(endpoint, params=params)
        if response.ok:
            print(response.json())
        else:
            print(f"Error fetching matches: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None

def match_officials():
    pass

if __name__ == "__main__":
    main()