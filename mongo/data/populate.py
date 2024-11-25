import csv
import json
import requests
from datetime import datetime

BASE_URL = "http://localhost:8000"


def main():
    #drop all data
    response = requests.delete(f"{BASE_URL}/all")
    # Populate teams with encoder for accented characters
    with open("team.csv", "r",encoding="utf-8") as fd:
        teams = csv.DictReader(fd)
        for team in teams:
            response = requests.post(f"{BASE_URL}/team", json=team)
            if not response.ok:
                print(f"Failed to post team: {response.json()}")
    with open("playerInjuries.csv", "r", encoding="utf-8") as fd:
        teams = csv.DictReader(fd)
        for team in teams:
            # Limpiar los espacios extra de los campos
            team = {key.strip(): value.strip() for key, value in team.items()}
            
            # Convertir las fechas a objetos datetime
            try:
                team['start_date'] = datetime.strptime(team['start_date'], '%Y-%m-%d').isoformat() #iso format is to make it compatible with the datetime format
                team['end_date'] = datetime.strptime(team['end_date'], '%Y-%m-%d').isoformat()
            except KeyError:
                # Si no existe la clave, ignora el error
                pass
            except ValueError:
                # Si hay un error de formato, maneja el caso
                print(f"Error al convertir las fechas para el equipo: {team['team_name']}")
                continue
            
            # Realizar la solicitud POST
            response = requests.post(f"{BASE_URL}/player_injuries", json=team)
            if not response.ok:
                print(f"Failed to post player injury: {response.json()}")
    with open("awards.csv", "r", encoding="utf-8") as fd:
        awards = csv.DictReader(fd)
        for award in awards:
            try:
                award['date_awarded'] = datetime.strptime(award['date_awarded'], '%Y-%m-%d').isoformat()
            except KeyError:
                pass
            except ValueError:
                print(f"Error al convertir la fecha para el premio: {award['award_name']}")
                continue
            response = requests.post(f"{BASE_URL}/awards", json=award)
            if not response.ok:
                print(f"Failed to post award: {response.json()}")
    with open("matches.csv", "r", encoding="utf-8") as fd:
        matches = csv.DictReader(fd)
        print(str(matches)+"entro  mathces")
        for match1 in matches:
            if not match1['score']:
                match1['score'] = None
            if not match1['statistics']:
                match1['statistics'] = None
            try:
                match1['date'] = datetime.strptime(match1['date'], '%Y-%m-%d').isoformat()
            except KeyError:#si no existe la clave imprime el error
                print("Campo 'date' faltante o inválido.")
                continue
                
            except ValueError:
                print(f"Error al convertir la fecha para el partido: {match1['home_team_name']} vs {match1['away_team_name']}")
                continue
            try:
                match1['officials'] = [
                official.strip() for official in match1['officials'][1:-1].split(', ')
            ]
            except KeyError:
                print("Campo 'officials' faltante o inválido.")
                continue

            try:
                if match1['statistics']:
                    # Remove brackets and split by comma
                    stats_str = match1['statistics'].strip('[]')
                    match1['statistics'] = [stat.strip() for stat in stats_str.split(',')]
            except KeyError:
                print("Missing or invalid 'statistics' field")
                continue
            response = requests.post(f"{BASE_URL}/matches", json=match1)
    with open("transfers.csv", "r", encoding="utf-8") as fd:
        transfers = csv.DictReader(fd)
        print(transfers)
        for transfer in transfers:
            try:
                transfer['transfer_date'] = datetime.strptime(transfer['transfer_date'], '%Y-%m-%d').isoformat()
            except KeyError:
                pass
            except ValueError:
                print(f"Error al convertir la fecha para la transferencia: {transfer['player_name']}")
                continue
            response = requests.post(f"{BASE_URL}/player_transfers", json=transfer)
            if not response.ok:
                print(f"Failed to post transfer: {response.json()}")
    with open("playerValues.csv", "r", encoding="utf-8") as fd:
        player_values = csv.DictReader(fd)
        for player_value in player_values:
            # Reparar value_history
            try:
                player_value['value_history'] = json.loads(player_value['value_history'])
            except json.JSONDecodeError:
               print(f"Formato inválido en value_history: {player_value['value_history']}")

            response = requests.post(f"{BASE_URL}/player_values", json=player_value)
            if not response.ok:
                print(f"Failed to post player value: {response.json()}")

if __name__ == "__main__":
    main()



