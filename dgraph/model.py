from pydgraph import DgraphClient, DgraphClientStub
import csv
import json


# Definir el esquema de Dgraph
def set_schema(client):
    schema = """
    player_id: string @index(hash) .
    name: string @index(term) .
    age: int @index(int) .
    country: string @index(term) .
    league_id: string @index(hash) .
    stats_id: string @index(hash) .
    matches: int @index(int) .
    assists: int @index(int) .
    goals: int @index(int) .
    plays_in: uid @reverse .
    has_stats: uid @reverse .
    originates: uid @reverse .
    includes: uid @reverse .
    belongs_to: uid @reverse .
    country_stats: uid @reverse .
    """
    op = client.txn().alter(pydgraph.Operation(schema=schema))
    print("Schema applied.")

# Conectar al servidor Dgraph
def create_client():
    stub = DgraphClientStub('localhost:9080')  # Cambiar si el puerto es diferente
    return DgraphClient(stub)

def load_data(client, file_path, node_type):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        mutations = []
        for row in reader:
            try:
                if node_type == 'Player':
                    mutations.append({
                        'uid': f'_:{row["player_id"]}',
                        'dgraph.type': 'Player',
                        'player_id': row['player_id'],
                        'name': row['name'],
                        'age': int(row['age']),
                        'country': row['country']
                    })
                elif node_type == 'League':
                    mutations.append({
                        'uid': f'_:{row["league_id"]}',
                        'dgraph.type': 'League',
                        'league_id': row['league_id'],
                        'name': row['name']
                    })
                elif node_type == 'Country':
                    mutations.append({
                        'uid': f'_:{row["country_id"]}',
                        'dgraph.type': 'Country',
                        'country_id': row['country_id'],
                        'name': row['name']
                    })
                elif node_type == 'PlayerStats':
                    mutations.append({
                        'uid': f'_:{row["stats_id"]}',
                        'dgraph.type': 'PlayerStats',
                        'stats_id': row['stats_id'],
                        'matches': int(row['matches']),
                        'assists': int(row['assists']),
                        'goals': int(row['goals'])
                    })
            except Exception as e:
                print(f"Error processing row {row}: {e}")
        
        txn = client.txn()
        try:
            txn.mutate(set_obj=mutations, commit_now=True)
            print(f"Data loaded for {node_type}.")
        except Exception as e:
            print(f"Error loading data for {node_type}: {e}")
        finally:
            txn.discard()


def analyze_player_performance(client, player_id):
    query = f"""
    {{
        player(func: uid({player_id})) {{
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
        print(json.loads(res.json))
    finally:
        txn.discard()

def get_player_stats_by_league(client, league_id):
    query = f"""
    {{
        players(func: uid({league_id})) {{
            name
            plays_in {{
                name
            }}
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
        print(json.loads(res.json))
    finally:
        txn.discard()

def get_player_stats_by_country(client, country_id):
    query = f"""
    {{
        players(func: uid({country_id})) {{
            name
            originates {{
                name
            }}
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
        print(json.loads(res.json))
    finally:
        txn.discard()

def get_player_stats_by_age(client, age):
    query = f"""
    {{
        players(func: has(age), orderasc: age) @filter(gt(age, {age})) {{
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
        print(json.loads(res.json))
    finally:
        txn.discard()

def get_basic_player_stats(client, stats_id):
    query = f"""
    {{
        playerStats(func: uid({stats_id})) {{
            stats_id
            matches
            assists
            goals
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        print(json.loads(res.json))
    finally:
        txn.discard()

def search_players(client, search_term):
    query = f"""
    {{
        players(func: has(name), first: 10) @filter(regex(name, /{search_term}/i)) {{
            name
            player_id
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        print(json.loads(res.json))
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
        print(json.loads(res.json))
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
        print(json.loads(res.json))
    finally:
        txn.discard()