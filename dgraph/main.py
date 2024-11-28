#!/usr/bin/env python3
import os
import pydgraph
from model import analyze_player_performance, create_data, get_player_stats_by_league, get_player_stats_by_country, get_player_stats_by_age, get_basic_player_stats, search_players, compare_players, get_top_scorers, create_client, set_schema

def load_data(client):
    create_data(client)  # Llama a la función `create_data` que ya tienes definida
    print("Data loaded into the database.")

def display_menu():
    """ Displays the options menu for the user to select a query. """
    print("\nSelect a query:")
    print("1. Analyze a player's performance")
    print("2. Get player stats by league")
    print("3. Get player stats by country")
    print("4. Get player stats by age")
    print("5. Get basic player stats")
    print("6. Search for players by name")
    print("7. Compare players")
    print("8. Get top scorers")
    print("9. Exit")

def main():
    # Crea el cliente de Dgraph
    stub = pydgraph.DgraphClientStub('localhost:9080')
    client = pydgraph.DgraphClient(stub)
    
    try:
        # Aplica el esquema y carga los datos (ya definidos directamente en model.py)
        set_schema(client)
        load_data(client) 

        while True:
            display_menu()

            choice = input("\nEnter the number of the query you want to execute: ")

            if choice == '1':
                player_name = input("Enter the player name: ")
                analyze_player_performance(client, player_name)

            elif choice == '2':
                league_name = input("Enter the league name: ")
                get_player_stats_by_league(client, league_name)

            elif choice == '3':
                country_name = input("Enter the country name: ")
                get_player_stats_by_country(client, country_name)

            elif choice == '4':
                age = int(input("Enter the minimum player age: "))
                get_player_stats_by_age(client, age)

            elif choice == '5':
                player_name = input("Enter the player's name for stats: ")
                get_basic_player_stats(client, player_name)

            elif choice == '6':
                search_term = input("Enter the name or part of the name to search for players: ")
                search_players(client, search_term)

            elif choice == '7':
                player_id_1 = input("Enter the first player ID: ")
                player_id_2 = input("Enter the second player ID: ")
                compare_players(client, player_id_1, player_id_2)

            elif choice == '8':
                get_top_scorers(client)

            elif choice == '9':
                print("Exiting...")
                break

            else:
                print("Invalid choice. Please select a valid option.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        stub.close()

if __name__ == '__main__':
    main()
