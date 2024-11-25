import csv
import json
import requests
from datetime import datetime

BASE_URL = "http://localhost:8000"



def main():
    response = requests.delete(f"{BASE_URL}/all")
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
            print(response)
            if not response.ok:
                print(f"Failed to post transfer: {response.json()}")




if __name__ == "__main__":
    main()