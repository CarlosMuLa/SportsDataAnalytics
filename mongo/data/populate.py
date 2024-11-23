import csv
import json
import requests
from datetime import datetime

BASE_URL = "http://localhost:8000"


def main():
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
                team['start_date'] = datetime.strptime(team['start_date'], '%Y-%m-%d')
                team['end_date'] = datetime.strptime(team['end_date'], '%Y-%m-%d')
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

if __name__ == "__main__":
    main()