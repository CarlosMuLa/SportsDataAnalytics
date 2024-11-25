#!/usr/bin/env python3
#comand to run the app: python -m uvicorn main:app --reload
import os

from fastapi import FastAPI
from pymongo import MongoClient,   ASCENDING, TEXT

from mongo.routes import router as mongo_router

app = FastAPI()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "football_db")

def create_indexes():
    # Teams collection
    try:
        app.database.teams.drop_indexes()
        app.database.matches.drop_indexes()
        app.database.player_injuries.drop_indexes()
        app.database.player_transfers.drop_indexes()
        app.database.awards.drop_indexes()
        app.database.player_values.drop_indexes()
    except Exception as e:
        print(f"Error dropping indexes: {e}")
    finally:
        print("Finished attempting to drop indexes")
    app.database.teams.create_index([("team_name", TEXT)])

    # Matches collection
    app.database.matches.create_index([("date", ASCENDING)])
    # Combine all text fields into a single text index
    app.database.matches.create_index([
        ("home_team_name", TEXT),
        ("away_team_name", TEXT),
        ("status", TEXT)
    ], name="matches_text_search")

    # Player injuries collection
    # Combine all text fields into a single text index
    app.database.player_injuries.create_index([
        ("player_name", TEXT),
        ("team_name", TEXT),
        ("status", TEXT)
    ], name="injuries_text_search")

    # Transfers collection
    # Combine all text fields into a single text index
    app.database.player_transfers.create_index([
        ("player_name", TEXT),
        ("from_team_name", TEXT),
        ("team_name", TEXT)
    ], name="transfers_text_search")
    app.database.player_transfers.create_index([("transfer_date", -1)])

    # Awards collection
    # Combine text fields into a single text index
    app.database.awards.create_index([
        ("recipient_name", TEXT),
        ("award_name", TEXT)
    ], name="awards_text_search")
    app.database.awards.create_index([("season", ASCENDING)])

    # Player values collection
    app.database.player_values.create_index([("player_name", TEXT)])



@app.on_event("startup")
def startup_db_client():
    app.mongodb_client = MongoClient(MONGO_URI)
    app.database = app.mongodb_client[MONGO_DB_NAME]
    print(f"Connected to MongoDB at: {MONGO_URI} \n\t Database: {MONGO_DB_NAME}")
    create_indexes()
    print("Indexes created")

    
@app.on_event("shutdown")
def shutdown_db_client():
    app.mongodb_client.close()
    print("Connection to MongoDB closed")

app.include_router(mongo_router)
