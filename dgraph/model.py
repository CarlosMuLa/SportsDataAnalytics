# model.py
import os
import csv
import json
import pydgraph
from pydgraph import DgraphClient, DgraphClientStub, Operation

def create_client():
    stub = DgraphClientStub('localhost:9080')
    return DgraphClient(stub)

def set_schema(client):
    client.alter(pydgraph.Operation(drop_all=True))  # Limpia todo el esquema y los datos existentes.
    
    schema = """
# Definición de tipos
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
    plays_in: [uid] 
    has_stats: [uid] 
}

type PlayerStats {
    stats_id: string
    matches: int
    assists: int
    goals: int
    belongs_to: uid
}

# Definición de los índices fuera de los tipos
country_id: string @index(hash) .
stats_id: string @index(hash) .
age: int @index(int) .
league_id: string @index(hash) .

# Relaciones y directivas @reverse
country: uid @reverse .
plays_in: uid @reverse .
has_stats: uid @reverse .
originates: uid @reverse .
includes: uid @reverse .
belongs_to: uid @reverse .

matches: int  .
assists: int  .
goals: int .

# Índice exacto en el campo `name` de Player
name: string @index(exact) .
"""
    
    # Aplica el esquema en el cliente de Dgraph
    client.alter(pydgraph.Operation(schema=schema))
    print("Schema applied.")

def create_data(client):
    txn = client.txn()
    try:
        data = [
            # Países
            {
                'uid': '_:France',
                'dgraph.type': 'Country',
                'country_id': '1',
                'name': 'France'
            },
            {
                'uid': '_:Uruguay',
                'dgraph.type': 'Country',
                'country_id': '2',
                'name': 'Uruguay'
            },
            {
                'uid': '_:Mexico',
                'dgraph.type': 'Country',
                'country_id': '3',
                'name': 'Mexico'
            },
            {
                'uid': '_:España',
                'dgraph.type': 'Country',
                'country_id': '4',
                'name': 'España'
            },
            {
                'uid': '_:Paises_Bajos',
                'dgraph.type': 'Country',
                'country_id': '5',
                'name': 'Paises Bajos'
            },
            {
                'uid': '_:Chile',
                'dgraph.type': 'Country',
                'country_id': '6',
                'name': 'Chile'
            },

            # Ligas
            {
                'uid': '_:ligaMX',
                'dgraph.type': 'League',
                'league_id': '1',
                'name': 'ligaMX'
            },

            # Jugadores
            {
                'uid': '_:Andre_Gignac',
                'dgraph.type': 'Player',
                'name': 'Andre Gignac',
                'age': 37,
                'country': {'uid': '_:France'},
                'plays_in': [{'uid': '_:ligaMX'}]
            },
            {
                'uid': '_:Fernando_Gorriaran',
                'dgraph.type': 'Player',
                'name': 'Fernando Gorriaran',
                'age': 29,
                'country': {'uid': '_:Uruguay'},
                'plays_in': [{'uid': '_:ligaMX'}]
            },
            {
                'uid': '_:Alan_Mozo',
                'dgraph.type': 'Player',
                'name': 'Alan Mozo',
                'age': 28,
                'country': {'uid': '_:Mexico'},
                'plays_in': [{'uid': '_:ligaMX'}]
            },
            {
                'uid': '_:Alvaro_Fidalgo',
                'dgraph.type': 'Player',
                'name': 'Alvaro Fidalgo',
                'age': 30,
                'country': {'uid': '_:España'},
                'plays_in': [{'uid': '_:ligaMX'}]
            },
            {
                'uid': '_:Jaivaro_Dilrosun',
                'dgraph.type': 'Player',
                'name': 'Jaivaro Dilrosun',
                'age': 18,
                'country': {'uid': '_:Paises_Bajos'},
                'plays_in': [{'uid': '_:ligaMX'}]
            },
            {
                'uid': '_:Diego_Valdez',
                'dgraph.type': 'Player',
                'name': 'Diego Valdez',
                'age': 28,
                'country': {'uid': '_:Chile'},
                'plays_in': [{'uid': '_:ligaMX'}]
            },

            # Estadísticas de Jugadores
            {
                'uid': '_:stats1',
                'dgraph.type': 'PlayerStats',
                'stats_id': '1',
                'matches': 100,
                'assists': 50,
                'goals': 80,
                'has_stats': {'uid': '_:Andre_Gignac'}
            },
            {
                'uid': '_:stats2',
                'dgraph.type': 'PlayerStats',
                'stats_id': '2',
                'matches': 150,
                'assists': 40,
                'goals': 120,
                'has_stats': {'uid': '_:Fernando_Gorriaran'}
            },
            {
                'uid': '_:stats3',
                'dgraph.type': 'PlayerStats',
                'stats_id': '3',
                'matches': 120,
                'assists': 30,
                'goals': 14,
                'has_stats': {'uid': '_:Alan_Mozo'}
            },
            {
                'uid': '_:stats4',
                'dgraph.type': 'PlayerStats',
                'stats_id': '4',
                'matches': 200,
                'assists': 40,
                'goals': 50,
                'has_stats': {'uid': '_:Alvaro_Fidalgo'}
            },
            {
                'uid': '_:stats5',
                'dgraph.type': 'PlayerStats',
                'stats_id': '5',
                'matches': 50,
                'assists': 11,
                'goals': 9,
                'has_stats': {'uid': '_:Jaivaro_Dilrosun'}
            },
            {
                'uid': '_:stats6',
                'dgraph.type': 'PlayerStats',
                'stats_id': '6',
                'matches': 111,
                'assists': 88,
                'goals': 10,
                'has_stats': {'uid': '_:Diego_Valdez'}
            }
        ]
        
        txn.mutate(set_obj=data)
        txn.commit()
    finally:
        txn.discard()

def verify_data(client):
    query = """
    {
        allPlayers(func: has(player_id)) {
            player_id
            name
        }
    }
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        print("Players in the database:")
        print(json.loads(res.json))
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
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        if "player" in data and len(data["player"]) > 0:
            player_data = data["player"][0]
            print(f"\nPlayer Performance Data for {player_name}:")
            print(f"Name: {player_data['name']}")
            stats = player_data.get("has_stats", [])
            if stats:
                print(f"Matches: {stats[0].get('matches', 'N/A')}")
                print(f"Assists: {stats[0].get('assists', 'N/A')}")
                print(f"Goals: {stats[0].get('goals', 'N/A')}")
            else:
                print("No stats found.")
        else:
            print(f"\nNo data found for player: {player_name}")
    except Exception as e:
        print(f"\nError while querying player performance: {e}")
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
        print(f"\nPlayers in the {league_name} League:")
        data = json.loads(res.json)
        if "league" in data and len(data["league"]) > 0:
            players = data["league"][0].get("includes", [])
            for player in players:
                print(f"Name: {player['name']}")
                stats = player.get("has_stats", [])
                if stats:
                    print(f"  Matches: {stats[0].get('matches', 'N/A')}")
                    print(f"  Assists: {stats[0].get('assists', 'N/A')}")
                    print(f"  Goals: {stats[0].get('goals', 'N/A')}")
        else:
            print(f"No players found for league: {league_name}")
    except Exception as e:
        print(f"Error while querying league: {e}")
    finally:
        txn.discard()

def get_player_stats_by_country(client, country_name):
    query = f"""
    {{
        players(func: has(name)) @filter(eq(country.name, "{country_name}")) {{
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
        data = json.loads(res.json())
        if data['players']:
            print(f"\nPlayers from {country_name}:")
            for player in data['players']:
                print(f"Name: {player['name']}")
                stats = player.get("has_stats", [])
                if stats:
                    print(f"Matches: {stats[0].get('matches', 'N/A')}")
                    print(f"Assists: {stats[0].get('assists', 'N/A')}")
                    print(f"Goals: {stats[0].get('goals', 'N/A')}")
                else:
                    print("No stats found.")
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

