#!/usr/bin/env python3
import logging
import os
import random

from cassandra.cluster import Cluster

import model

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('leaguedata.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'leaguedata')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_menu():
    mm_options = {
        0: "Populate data",
        1: "Show Teams",
        2: "Real time matches",
        3: "Show player history",
        4: "Show teams history",
        5: "Show stadiums capacity",
        6: "Show players list of a team",
        7: "Show teams rankings",
        8: "Show teams budgets",
        9: "Teams comparisons",
        10: "Show Leagues standings",
        11: "Show stadium attendence Trends",
        12: "Show Players Jerseys",
        13: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])







def main():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.delete_schema(session)
    model.create_schema(session)


    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 0:
            model.bulk_insert(session)
        if option == 1:
            model.storedTeamData(session)
        if option == 2:
            model.displayRealTimeVisualization(session) 
        if option == 3:
            pname = input("Please insert the Player name: ")
            model.getPlayerHistory(session, pname)
        if option == 4:
            pname = input("Please insert the Team name: ")
            model.getTeamHistory(session,pname)
        if option == 5:
            cn = input("Please insert the Country name: ")
            ca = int(input("Please insert the minimum capacity: "))
            model.affitionStatus(session, cn, ca)
        if option == 6:
            pname = input("Please insert the Team name: ")
            model.getPlayersByTeam(session,pname)
        if option == 7:
            cn = input("Please insert the Country name: ")
            r = int(input("Please insert the minimum rank: "))
            model.getTeamRanking(session, cn, r)
        if option == 8:
            c = input("Please insert the Country name: ")
            ba = int(input("Please insert the minimum budget allocated: "))
            model.manageTeamBudgets(session, c, ba)
        if option == 9:
            pname1 = input("Please insert the Teams1 name: ")
            pname2 = input("Please insert the Teams2 name: ")
            model.compareTeams(session, pname1, pname2)
        if option == 10:
            model.getLeagueStandings(session)
        if option == 11:
            c = input("Please insert the Country name: ")
            av = int(input("Please insert the minimum of the avergae assistance: "))
            model.analyzeAttendanceTrends(session, c, av)
        if option == 12:
            pname = input("Please insert the player name: ")
            model.get_players_jersey_history(session, pname)
        if option == 13:
            exit(0)


if __name__ == '__main__':
    main()
