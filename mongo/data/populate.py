import csv
import json
import requests

BASE_URL = "http://localhost:8000"


def main():
    with open("team.csv") as fd:
        teams = csv.DictReader(fd)
        for team in teams:
            response = requests.post(f"{BASE_URL}/team", json=team)
            if not response.ok:
                print(f"Failed to post team: {response.json()}")


if __name__ == "__main__":
    main()