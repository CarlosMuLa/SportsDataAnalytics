import os
import json
import pydgraph
from pydgraph import DgraphClient, DgraphClientStub

def create_client():
    stub = DgraphClientStub('localhost:9080')
    return DgraphClient(stub)

def set_schema(client):
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
            {"uid": "_:France", "dgraph.type": "Country", "name": "France"},
            {"uid": "_:Uruguay", "dgraph.type": "Country", "name": "Uruguay"},
            {"uid": "_:Mexico", "dgraph.type": "Country", "name": "Mexico"},
            {"uid": "_:Spain", "dgraph.type": "Country", "name": "Spain"},

            # Leagues
            {"uid": "_:LigaMX", "dgraph.type": "League", "name": "Liga MX"},

            # Players
            {
                "uid": "_:Andre_Gignac",
                "dgraph.type": "Player",
                "name": "Andre Gignac",
                "age": 37,
                "country": {"uid": "_:France"},
                "plays_in": {"uid": "_:LigaMX"},
                "has_stats": {"uid": "_:Stats_Gignac"}
            },
            {
                "uid": "_:Fernando_Gorriaran",
                "dgraph.type": "Player",
                "name": "Fernando Gorriaran",
                "age": 29,
                "country": {"uid": "_:Uruguay"},
                "plays_in": {"uid": "_:LigaMX"},
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
        data = json.loads(res.json)
        if data["player"]:
            #print(data)
            #print("hola"+str(data["player"]))
            player_data = data["player"][0]
            player_stats = data["player"][1]
            #print type of player_stats
            has_stats = player_stats.get("has_stats", [])
           # print(player_data)
            if player_stats:
                print(f"Matches: {has_stats.get('matches', 'N/A')}")
                print(f"Assists: {has_stats.get('assists', 'N/A')}")
                print(f"Goals: {has_stats.get('goals', 'N/A')}")
            else:
                print("No stats available.")
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
            name
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
        if data["league"]:
            league = data["league"][0]
            print(f"League: {league['name']}")
            for player in league.get("includes", []):
                print(f"Player: {player['name']}")
                stats = player.get("has_stats", {})
                print(f"Matches: {stats.get('matches', 'N/A')}")
                print(f"Assists: {stats.get('assists', 'N/A')}")
                print(f"Goals: {stats.get('goals', 'N/A')}")
        else:
            print("League not found.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        txn.discard()

def get_player_stats_by_country(client, country_name):
    query = f"""
    {{
        players(func: has(name)) @filter(eq(country.name, "{country_name}")) {{
            name
            stats {{
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
        if "players" in data and len(data["players"]) > 0:
            print(f"\nPlayers from {country_name}:")
            for player in data["players"]:
                print(f"Name: {player['name']}")
                stats = player.get("stats", [])
                if stats:
                    print(f"Matches: {stats[0].get('matches', 'N/A')}")
                    print(f"Assists: {stats[0].get('assists', 'N/A')}")
                    print(f"Goals: {stats[0].get('goals', 'N/A')}")
        else:
            print(f"\nNo players found from {country_name}.")
    except Exception as e:
        print(f"\nError while querying player stats by country: {e}")
    finally:
        txn.discard()


def get_player_stats_by_age(client, min_age):
    query = f"""
    {{
        players(func: has(name)) @filter(ge(age, {min_age})) {{
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
        data = json.loads(res.json())
        if data['players']:
            print(f"\nPlayers older than {min_age} years:")
            for player in data['players']:
                print(f"Name: {player['name']}, Age: {player['age']}")
                stats = player.get("has_stats", [])
                if stats:
                    print(f"Matches: {stats[0].get('matches', 'N/A')}")
                    print(f"Assists: {stats[0].get('assists', 'N/A')}")
                    print(f"Goals: {stats[0].get('goals', 'N/A')}")
                else:
                    print("No stats found.")
        else:
            print(f"\nNo players found older than {min_age} years.")
    except Exception as e:
        print(f"\nError while querying player stats by age: {e}")
    finally:
        txn.discard()

def get_basic_player_stats(client, player_name):
    query = f"""
    {{
        player(func: eq(name, "{player_name}")) {{
            name
            has_stats {{
                stats_id
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
        if "player" in data and len(data["player"]) > 0:
            player_data = data["player"][0]
            print(f"Player: {player_data['name']}")
            stats = player_data.get("has_stats", [])
            if stats:
                for stat in stats:
                    print(f"Stats ID: {stat['stats_id']}")
                    print(f"  Matches: {stat.get('matches', 'N/A')}")
                    print(f"  Assists: {stat.get('assists', 'N/A')}")
                    print(f"  Goals: {stat.get('goals', 'N/A')}")
        else:
            print(f"No data found for player: {player_name}")
    except Exception as e:
        print(f"Error while querying player stats: {e}")
    finally:
        txn.discard()

 
def search_players(client, search_term):
    query = f"""
    {{
        players(func: has(name), first: 10) @filter(regex(name, /{search_term}/i)) {{
            name
            player_id  # Esta es la relación con el player_id
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        if data and "players" in data:
            for player in data["players"]:
                print(f"Name: {player['name']}, ID: {player['player_id']}")
        else:
            print("No players found.")
    except Exception as e:
        print(f"Error while searching players: {e}")
    finally:
        txn.discard()

def compare_players(client, player_id_1, player_id_2):
    query = f"""
    {{
        player1 as var(func: uid({player_id_1})) {{
            has_stats {{
                matches
                assists
                goals
            }}
        }}
        player2 as var(func: uid({player_id_2})) {{
            has_stats {{
                matches
                assists
                goals
            }}
        }}
        comparison(func: uid(player1, player2)) {{
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
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"Error while comparing players: {e}")
    finally:
        txn.discard()

def get_top_scorers(client):
    query = """
    {
        topScorers(func: type(PlayerStats), orderdesc: goals, first: 10) {
            name
            goals
            belongs_to {
                name
            }
        }
    }
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        if data and "topScorers" in data:
            for scorer in data["topScorers"]:
                print(f"Name: {scorer['name']}, Goals: {scorer['goals']}, Team: {scorer['belongs_to'][0]['name']}")
        else:
            print("No top scorers found.")
    except Exception as e:
        print(f"Error while querying top scorers: {e}")
    finally:
        txn.discard()

