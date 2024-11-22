import os

from fastapi import FastAPI
from pymongo import MongoClient,   ASCENDING, TEXT

from mongo.routes import router as mongo_router

app = FastAPI()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "football_db")

def create_indexes():
    app.database.teams.create_index([("team_name", TEXT)])
    app.database.matches.create_index([("date", ASCENDING)])
    app.database.matches.create_index([("home_team_name", TEXT)])
    app.database.matches.create_index([("away_team_name", TEXT)])
    app.database.matches.create_index([("status", TEXT)])
    app.database.player_injuries.create_index([("player_name", TEXT)])
    app.database.player_injuries.create_index([("team_name", TEXT)])
    app.database.player_injuries.create_index([("status", TEXT)])
    app.database.transfers.create_index([("player_name", TEXT)])
    app.datanase.transfers.create_index([("from_team_name", TEXT)])
    app.database.transfers.create_index([("team_name", TEXT)])
    app.database.transfers.create_index([("transfer_date",-1)])
    app.database.awards.create_index([("recipient_name", TEXT)])
    app.database.awards.create_index([("award_name", TEXT)])
    app.database.awards.create_index([("season", ASCENDING)])
    app.database.player_values.create_index([("player_name", TEXT)])

def create_collections():
    app.database.create_collection("teams")
    app.database.create_collection("matches")
    app.database.create_collection("player_injuries")
    app.database.create_collection("transfers")
    app.database.create_collection("awards")
    app.database.create_collection("player_values")

def aggregations(player_name):
    pipeline = [
        {"$match": {"player_name": player_name}},
        {"$project": {
            "values": {
                "$map": {
                    "input": "$value_history",
                    "as": "value",
                    "in": {"$toDouble": "$$value"}  # Convert string values to numbers
                }
            }
        }},
        {"$project": {
            "avgValue": {"$avg": "$values"},
            "maxValue": {"$max": "$values"},
            "minValue": {"$min": "$values"}
        }}
    ]
    return list(app.database.player_values.aggregate(pipeline))


@app.on_event("startup")
def startup_db_client():
    app.mongodb_client = MongoClient(MONGO_URI)
    app.database = app.mongodb_client[MONGO_DB_NAME]
    print(f"Connected to MongoDB at: {MONGO_URI} \n\t Database: {MONGO_DB_NAME}")
    create_collections()
    print("Collections created")
    create_indexes()
    print("Indexes created")

    
@app.on_event("shutdown")
def shutdown_db_client():
    app.mongodb_client.close()
    print("Connection to MongoDB closed")

app.include_router(mongo_router)