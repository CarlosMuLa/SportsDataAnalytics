import os
import json
import pydgraph
from pydgraph import DgraphClient, DgraphClientStub

def create_client():
    stub = DgraphClientStub('localhost:9080')
    return DgraphClient(stub)

def set_schema(client):
#drop the data if it exists
    client.alter(pydgraph.Operation(drop_all=True))
    schema = """
        type Country {
            country_id: string
            name: string
            originates: [uid]
        }

        type League {
            league_id: string
            name: string
            includes: [uid]
        }

        type Player {
            name: string
            age: int
            country: uid
            plays_in: uid
            has_stats: uid
        }

        type PlayerStats {
            stats_id: string
            matches: int
            assists: int
            goals: int
            belongs_to: uid
        }

        country_id: string @index(hash) .
        stats_id: string @index(exact) .
        league_id: string @index(hash) .
        name: string @index(exact) .
        age: int @index(int) .
        matches: int .
        assists: int .
        goals: int .
        country: uid @reverse .
        plays_in: uid @reverse .
        has_stats: uid @reverse .
        originates: uid @reverse .
        includes: uid @reverse .
        belongs_to: uid @reverse .
    """
    client.alter(pydgraph.Operation(schema=schema))
    print("Schema applied.")

def create_data(client):
    txn = client.txn()
    try:
        data = [
            # Countries
            # Actualiza los países
{"uid": "_:Uruguay", "dgraph.type": "Country", "name": "Uruguay", "originates": [{"uid": "_:Fernando_Gorriaran"}]},
{"uid": "_:France", "dgraph.type": "Country", "name": "France", "originates": [{"uid": "_:Andre_Gignac"}]},


            # Leagues
            # Actualiza las ligas
{"uid": "_:LigaMX", "dgraph.type": "League", "name": "Liga MX", "includes": [{"uid": "_:Andre_Gignac"}, {"uid": "_:Fernando_Gorriaran"}]},


            # Players
            {
                "uid": "_:Andre_Gignac",
                "dgraph.type": "Player",
                "name": "Andre Gignac",
                "age": 37,
                "country": {"uid": "_:France"},
                "plays_in": {"uid": "_:Liga_MX"},
                "has_stats": {"uid": "_:Stats_Gignac"}
            },
            {
                "uid": "_:Fernando_Gorriaran",
                "dgraph.type": "Player",
                "name": "Fernando Gorriaran",
                "age": 29,
                "country": {"uid": "_:Uruguay"},
                "plays_in": {"uid": "_:Liga_MX"},
                "has_stats": {"uid": "_:Stats_Gorriaran"}
            },

            # Player Stats
            {
                "uid": "_:Stats_Gignac",
                "dgraph.type": "PlayerStats",
                "matches": 100,
                "assists": 50,
                "goals": 80,
                "belongs_to": {"uid": "_:Andre_Gignac"}
            },
            {
                "uid": "_:Stats_Gorriaran",
                "dgraph.type": "PlayerStats",
                "matches": 150,
                "assists": 40,
                "goals": 120,
                "belongs_to": {"uid": "_:Fernando_Gorriaran"}
            }
        ]
        txn.mutate(set_obj=data)
        txn.commit()
        print("Data loaded successfully.")
    finally:
        txn.discard()

def analyze_player_performance(client, player_name):
    query = f"""
    {{
        player(func: eq(name, "{player_name}")) {{
            name
            has_stats {{
                matches
                assists
                goals
            }}
        }}
    }}
    """
    print(f"Query: {query}")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)  # Decodificar directamente a diccionario
        
        if "player" in data and data["player"]:
            player_data = data["player"][0]
            stats = player_data.get("has_stats", {})
            
            print(f"Player: {player_data['name']}")
            print(f"Matches: {stats.get('matches', 'N/A')}")
            print(f"Assists: {stats.get('assists', 'N/A')}")
            print(f"Goals: {stats.get('goals', 'N/A')}")
        else:
            print("Player not found.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        txn.discard()


def get_player_stats_by_league(client, league_name):
    query = f"""
    {{
        league(func: eq(name, "{league_name}")) {{
            includes {{
                name
                has_stats {{
                    matches
                    assists
                    goals
                }}
            }}
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)

        # Imprime los datos para depuración
        print("Response Data:", data)

        if "league" in data and data["league"]:
            league_data = data["league"][0]
            players = league_data.get("includes", [])
            
            # Si `includes` es un diccionario y no una lista, conviértelo en lista
            if isinstance(players, dict):
                players = [players]

            if players:
                print(f"\nPlayers in league {league_name}:")
                for player in players:
                    name = player.get("name", "N/A")
                    stats = player.get("has_stats", {})
                    matches = stats.get("matches", "N/A")
                    assists = stats.get("assists", "N/A")
                    goals = stats.get("goals", "N/A")
                    print(f"Name: {name}, Matches: {matches}, Assists: {assists}, Goals: {goals}")
            else:
                print(f"No players found in league {league_name}.")
        else:
            print(f"No league found with name {league_name}.")
    except Exception as e:
        print(f"Error while querying player stats by league: {e}")
    finally:
        txn.discard()



def get_player_stats_by_country(client, country_name):
    query = f"""
    {{
        country(func: eq(name, "{country_name}")) {{
            originates {{
                name
                has_stats {{
                    matches
                    assists
                    goals
                }}
            }}
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)

        # Imprime los datos para depuración
        print("Response Data:", data)

        if "country" in data and data["country"]:
            country_data = data["country"][0]
            players = country_data.get("originates", [])
            
            # Si `originates` es un diccionario y no una lista, conviértelo en lista
            if isinstance(players, dict):
                players = [players]

            if players:
                print(f"\nPlayers from {country_name}:")
                for player in players:
                    name = player.get("name", "N/A")
                    stats = player.get("has_stats", {})
                    matches = stats.get("matches", "N/A")
                    assists = stats.get("assists", "N/A")
                    goals = stats.get("goals", "N/A")
                    print(f"Name: {name}, Matches: {matches}, Assists: {assists}, Goals: {goals}")
            else:
                print(f"No players found for {country_name}.")
        else:
            print(f"No data found for country {country_name}.")
    except Exception as e:
        print(f"Error while querying player stats by country: {e}")
    finally:
        txn.discard()




def get_player_stats_by_age(client, min_age):
    query = f"""
    {{
        players(func: ge(age, {min_age})) {{
            name
            age
            has_stats {{
                matches
                assists
                goals
            }}
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        if "players" in data and data["players"]:
            print(f"\nPlayers older than {min_age} years:")
            for player in data["players"]:
                print(f"Name: {player['name']}, Age: {player['age']}")
                stats = player.get("has_stats", {})
                print(f"Matches: {stats.get('matches', 'N/A')}")
                print(f"Assists: {stats.get('assists', 'N/A')}")
                print(f"Goals: {stats.get('goals', 'N/A')}")
        else:
            print(f"No players found older than {min_age}.")
    except Exception as e:
        print(f"Error while querying player stats by age: {e}")
    finally:
        txn.discard()


def get_basic_player_stats(client, player_name):
    query = f"""
    {{
        player(func: eq(name, "{player_name}")) {{
            name
            has_stats {{
                matches
                assists
                goals
            }}
        }}
    }}
    """
    print(f"Query: {query}")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        if "player" in data and data["player"]:
            player_data = data["player"][0]
            has_stats = player_data.get("has_stats", {})
            print(f"Matches: {has_stats.get('matches', 'N/A')}")
            print(f"Assists: {has_stats.get('assists', 'N/A')}")
            print(f"Goals: {has_stats.get('goals', 'N/A')}")
        else:
            print("Player not found.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        txn.discard()

def search_players(client, player_name):
    query = f"""
    {{
        player(func: eq(name, "{player_name}")) {{
            name
            has_stats {{
                matches
                assists
                goals
            }}
        }}
    }}
    """
    print(f"Query: {query}")
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)

        # Imprimir datos de depuración
        print("Response Data:", data)

        if "player" in data and data["player"]:
            for player in data["player"]:
                name = player.get("name", "N/A")
                stats = player.get("has_stats", {})
                matches = stats.get("matches", "N/A")
                assists = stats.get("assists", "N/A")
                goals = stats.get("goals", "N/A")
                print(f"Name: {name}")
                print(f"Matches: {matches}")
                print(f"Assists: {assists}")
                print(f"Goals: {goals}")
        else:
            print("Player not found.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        txn.discard()

def compare_players(client, player_id_1, player_id_2):
    query = f"""
    {{
        player(func: uid({player_id_1}, {player_id_2})) {{
            name
            has_stats {{
                matches
                assists
                goals
            }}
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)

        # Imprimir los datos de depuración
        print("Response Data:", json.dumps(data, indent=2))

        if "player" in data and data["player"]:
            print("\nComparison Results:")
            for player in data["player"]:
                name = player.get("name", "N/A")
                stats = player.get("has_stats", {})
                matches = stats.get("matches", "N/A")
                assists = stats.get("assists", "N/A")
                goals = stats.get("goals", "N/A")
                print(f"Name: {name}")
                print(f"Matches: {matches}")
                print(f"Assists: {assists}")
                print(f"Goals: {goals}")
        else:
            print("No players found for comparison.")
    except Exception as e:
        print(f"Error while comparing players: {e}")
    finally:
        txn.discard()


def get_top_scorers(client):
    query = """
   {
    topScorers(func: type(PlayerStats), orderdesc: goals, first: 2) {
        goals
        belongs_to {
            name
        }
    }
}
    """
    print(f"Query: {query}")  # Imprime la consulta que se está ejecutando
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        
        # Cargar la respuesta en JSON
        data = json.loads(res.json)  # `res.json` para acceder directamente al JSON
        print(f"Decoded JSON data: {data}")  # Muestra el JSON decodificado para verificar la estructura
        
        if "topScorers" in data and data["topScorers"]:
            for scorer in data["topScorers"]:
                team = scorer.get("belongs_to", [{}])[0]  # Maneja listas vacías
                player_name = team.get("name", "N/A")
                player_goals = scorer.get("goals", "N/A")

                print(f"Name: {player_name}, Goals: {player_goals}")
        else:
            print("No top scorers found.")

    except Exception as e:
        print(f" {e}")
    finally:
        txn.discard()
