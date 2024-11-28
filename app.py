import logging
import os
import cassandra1.cmodel
from cassandra.cluster import Cluster
from dgraph.model import analyze_player_performance, get_player_stats_by_league, get_player_stats_by_country, get_player_stats_by_age, get_basic_player_stats, search_players, compare_players, get_top_scorers, create_client, set_schema
from mongo.mainmongo import match_history, player_injuries, getTeams,upcoming_matches,match_result,recent_matches,past_matches,player_transfers,awards,player_value



#------------ Functions ----------------
functions = {"1": "Show Teams",
             "2": "Real time matches",
             "3": "Show player history",
             "4": "Show teams history",
             "5": "Show stadiums capacity",
             "6": "Show players list of a team",
             "7": "Show teams rankings",
             "8": "Show teams budgets",
             "9": "Teams comparisons",
             "10": "Show Leagues standings",
             "11": "Show stadium attendence Trends",
             "12": "Show Players Jerseys",
             "13":"Match history",
            "14":"Player injuries",
            "15":"Upcoming matches",
            "16":"Match result",
            "17":"Recent matches for specific team",
            "18":"Past matches for specific team",
            "19":"Player transfers",
            "20":"Awards by: ",
            "21":"Player value",
            "22":"Analyze a player's performance",
            "23":"Get player stats by league",
            "24":"Get player stats by country",
            "25":"Get player stats by age",
            "26":"Get basic player stats",
            "27":"Search for players by name",
            "28":"Compare players",
            "29":"Get top scorers",
            "30":"Exit"}



#------------ Cassandra set up ---------

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CASSANDRA_CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

log.info("Connecting to Cluster")
cluster = Cluster(CASSANDRA_CLUSTER_IPS.split(','))
session = cluster.connect()

cassandra1.cmodel.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
session.set_keyspace(KEYSPACE)

cassandra1.cmodel.delete_schema(session)
cassandra1.cmodel.create_schema(session)
cassandra1.cmodel.bulk_insert(session)

#------------ Mongo set up ------------
MONGO_BASE_URL = "http://localhost:8000"

#------------ Dgraph set up ------------
dgraph_client = create_client()
set_schema(dgraph_client)
load_data(dgraph_client, 'dgraph\players.csv', 'Player')
load_data(dgraph_client, 'dgraph\league.csv', 'League')
load_data(dgraph_client, 'dgraph\countries.csv', 'Country')
load_data(dgraph_client, 'dgraph\playerstats.csv', 'PlayerStats')




#------------ main function ------------
def main():
    while (1):
        for key, value in functions.items():
            print(f"{key} -- {value}")

        choice = input("Enter the number of the query you want to execute: ")
        if choice == '1':
            cassandra1.cmodel.storedTeamData(session)
            getTeams()
        elif choice == '2':
            cassandra1.cmodel.displayRealTimeVisualization(session)
        elif choice == '3':
            player = str(input("Enter the player name: "))
            cassandra1.cmodel.getPlayerHistory(session, player)
        elif choice == '4':
            pname = input("Please insert the Team name: ")
            cassandra1.cmodel.getTeamHistory(session,pname)
        elif choice == '5':
            cn = input("Please insert the Country name: ")
            ca = int(input("Please insert the minimum capacity: "))
            cassandra1.cmodel.affitionStatus(session,cn,ca)
        elif choice == '6':
            team = input("Enter the team name: ")
            cassandra1.cmodel.getPlayersByTeam(session, team)
        elif choice == '7':
            cn = input("Please insert the Country name: ")
            r = int(input("Please insert the minimum rank: "))
            cassandra1.cmodel.getTeamRanking(session,cn,r)
        elif choice == '8':
            c = input("Please insert the Country name: ")
            ba = int(input("Please insert the minimum budget allocated: "))
            cassandra1.cmodel.manageTeamBudgets(session,c,ba)
        elif choice == '9':
            pname1 = input("Please insert the Teams1 name: ")
            pname2 = input("Please insert the Teams2 name: ")
            cassandra1.cmodel.compareTeams(session,pname1,pname2)
        elif choice == '10':
            cassandra1.cmodel.getLeagueStandings(session)
        elif choice == '11':
            c = input("Please insert the Country name: ")
            av = int(input("Please insert the minimum of the avergae assistance: "))
            cassandra1.cmodel.analyzeAttendanceTrends(session, c, av)
        elif choice == '12':
            pname = input("Please insert the player name: ")
            cassandra1.cmodel.get_players_jersey_history(session,pname)
        elif choice == '13':
            match_history()
        elif choice == '14':
            player = str(input("Enter the player name: "))
            player_injuries(player)
        elif choice == '15':
            upcoming_matches()
        elif choice == '16':
            match_result()
        elif choice == '17':
            team = input("Enter the team name: ")
            recent_matches(team)
        elif choice == '18':
            team = input("Enter the team name: ")
            past_matches(team)
        elif choice == '19':
            player = input("Enter the player name: ")
            player_transfers(player)
        elif choice == '20':
            awarded = input("Enter the team/player name: ")
            awards(awarded)
        elif choice == '21':
            player = input("Enter the player name: ")
            player_value(player)
        elif choice == '22':
            player_id = input("Enter the player: ")
            analyze_player_performance(dgraph_client, player_id)
        elif choice == '23':
            league_id = input("Enter the league: ")
            get_player_stats_by_league(dgraph_client, league_id)
        elif choice == '24':
            country_id = input("Enter the country: ")
            get_player_stats_by_country(dgraph_client, country_id)
        elif choice == '25':
            age = int(input("Enter the minimum player age: "))
            get_player_stats_by_age(dgraph_client, age)
        elif choice == '26':
            stats_id = input("Enter the player name for his stats: ")
            get_basic_player_stats(dgraph_client, stats_id)
        elif choice == '27':
            search_term = input("Enter the player's name to search: ")
            search_players(dgraph_client, search_term)
        elif choice == '28':
            player_id_1 = input("Enter the ID of the first player: ")
            player_id_2 = input("Enter the ID of the second player: ")
            compare_players(dgraph_client, player_id_1, player_id_2)
        elif choice == '29':
            get_top_scorers(dgraph_client)
        elif choice == '30':
            print("Exiting...")
            exit()

if __name__ == "__main__":
    main()