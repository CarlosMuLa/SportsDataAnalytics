from model import analyze_player_performance, get_player_stats_by_league, get_player_stats_by_country, get_player_stats_by_age, get_basic_player_stats, search_players, compare_players, get_top_scorers, create_client, load_data, set_schema

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
    client = create_client()
    try:
        # Set the schema and load data
        set_schema(client)
        load_data(client, 'players.csv', 'Player')
        load_data(client, 'leagues.csv', 'League')
        load_data(client, 'countries.csv', 'Country')
        load_data(client, 'player_stats.csv', 'PlayerStats')

        while True:
            display_menu()

            choice = input("\nEnter the number of the query you want to execute: ")

            if choice == '1':
                player_id = input("Enter the player ID: ")
                analyze_player_performance(client, player_id)

            elif choice == '2':
                league_id = input("Enter the league ID: ")
                get_player_stats_by_league(client, league_id)

            elif choice == '3':
                country_id = input("Enter the country ID: ")
                get_player_stats_by_country(client, country_id)

            elif choice == '4':
                age = int(input("Enter the minimum player age: "))
                get_player_stats_by_age(client, age)

            elif choice == '5':
                stats_id = input("Enter the player stats ID: ")
                get_basic_player_stats(client, stats_id)

            elif choice == '6':
                search_term = input("Enter the player's name to search: ")
                search_players(client, search_term)

            elif choice == '7':
                player_id_1 = input("Enter the ID of the first player: ")
                player_id_2 = input("Enter the ID of the second player: ")
                compare_players(client, player_id_1, player_id_2)

            elif choice == '8':
                get_top_scorers(client)

            elif choice == '9':
                print("Exiting...")
                break

            else:
                print("Invalid option. Please choose an option from 1 to 9.")

    finally:
        client.close()

if __name__ == "__main__":
    main()
