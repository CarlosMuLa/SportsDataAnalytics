#!/usr/bin/env python3
from fastapi import APIRouter, Body, Request, Response, HTTPException, status,Query
from fastapi.encoders import jsonable_encoder
from typing import List, Optional,Union

from .model import Team, PlayerInjuries, Awards, Matches,PlayerTransfers,PlayerValues

router = APIRouter()

@router.post("/team", response_description="Add new team", status_code=status.HTTP_201_CREATED,response_model=Team)
def create_team(request:Request, team:Team=Body(...)):
    team = jsonable_encoder(team)
    print(team)
    new_team = request.app.database["teams"].insert_one(team)
    print(new_team)
    created_team = request.app.database["teams"].find_one({"_id": new_team.inserted_id})
    return created_team

@router.get("/team",response_description="get Team",status_code = status.HTTP_200_OK,response_model=Team)
def get_team(team:str,request: Request):
    if(team := request.app.database["teams"].find_one({"team":team}))is not None:
        return team
    raise HTTPException(status_code=404,detail=f"Team {team} not found")

@router.get("/teams", response_description="get Teams", status_code=status.HTTP_200_OK, response_model=List[Team])
def get_teams(request: Request):
    teams = list(request.app.database["teams"].find())
    return teams

@router.post("/player_injuries", response_description="Add new player injury", status_code=status.HTTP_201_CREATED,response_model=PlayerInjuries)
def create_player_injury(request:Request, player_injury:PlayerInjuries=Body(...)):
    player_injury = jsonable_encoder(player_injury)
    new_player_injury = request.app.database["player_injuries"].insert_one(player_injury)
    created_player_injury = request.app.database["player_injuries"].find_one({"_id": new_player_injury.inserted_id})
    return created_player_injury


@router.get("/player_injuries", response_description="get Player Injuries", status_code=status.HTTP_200_OK, response_model=PlayerInjuries)
def get_player_injuries(request: Request, player_name: Optional[str] = Query(None)):
    # Si el jugador es nulo, devolver todas las lesiones de jugadores
    player_injury = request.app.database["player_injuries"].find_one({"player_name": player_name})
    print(player_injury)
    if player_injury:
        return player_injury
    raise HTTPException(status_code=404, detail=f"Player {player_name} not found")
@router.post("/awards", response_description="Add new award", status_code=status.HTTP_201_CREATED,response_model=Awards)
def create_award(request:Request, award:Awards=Body(...)):
    award = jsonable_encoder(award)
    new_award = request.app.database["awards"].insert_one(award)
    created_award = request.app.database["awards"].find_one({"_id": new_award.inserted_id})
    return created_award

@router.get("/awards", response_description="get Awards", status_code=status.HTTP_200_OK, response_model=List[Awards])
def get_awards(request: Request, awarded:str):
    return list(request.app.database["awards"].find({"recipient_name": awarded}))

@router.post("/matches", response_description="Add new match", status_code=status.HTTP_201_CREATED,response_model=Matches)
def create_match(request:Request, match:Matches=Body(...)):
    match = jsonable_encoder(match)
    new_match = request.app.database["matches"].insert_one(match)
    print(new_match)
    created_match = request.app.database["matches"].find_one({"_id": new_match.inserted_id})
    return created_match

@router.get("/matches", response_description="get Matches", status_code=status.HTTP_200_OK, response_model=List[Matches])
def get_matches(request: Request):
    # Verificar si los partidos están completados
    return list(request.app.database["matches"].find({"status": "Finished"}))

@router.get("/upcoming_matches", response_description="get Upcoming Matches", status_code=status.HTTP_200_OK, response_model=List[Matches])
def get_upcoming_matches(request: Request):
    # Verificar si los partidos están completados
    return list(request.app.database["matches"].find({"status": "Scheduled"}))

@router.get("/matches_score", response_description="get Matches with score", status_code=status.HTTP_200_OK, response_model=List[Matches])
def get_matches_score(request: Request):
    # Verificar si los partidos están completados
    return list(request.app.database["matches"].find({"score": {"$ne": None}}))

@router.get("/matches_team", response_description="get Matches by team", status_code=status.HTTP_200_OK, response_model=List[Matches]) #param is team_name
def get_matches_team(request: Request, team_name:str):
    # Verificar si los partidos están completados
    # solo primeros 5 partidos
    return list(request.app.database["matches"].find({"$or":[{"home_team_name":team_name},{"away_team_name":team_name}]}).limit(5))

@router.get("/matches_team_all", response_description="get Matches by team", status_code=status.HTTP_200_OK, response_model=List[Matches]) #param is team_name
def get_matches_team(request: Request, team_name:str):
    # Verificar si los partidos están completados
    return list(request.app.database["matches"].find({"$or":[{"home_team_name":team_name},{"away_team_name":team_name}]}))
    

@router.post("/player_transfers", response_description="Add new player transfer", status_code=status.HTTP_201_CREATED,response_model=PlayerTransfers)
def create_player_transfer(request:Request, player_transfer:PlayerTransfers=Body(...)):
    player_transfer = jsonable_encoder(player_transfer)
    new_player_transfer = request.app.database["player_transfers"].insert_one(player_transfer)
    created_player_transfer = request.app.database["player_transfers"].find_one({"_id": new_player_transfer.inserted_id})
    if not created_player_transfer:
        raise HTTPException(status_code=500, detail="Error fetching the created player transfer")
    
    return created_player_transfer

@router.get("/player_transfers", response_description="get Player Transfers", status_code=status.HTTP_200_OK, response_model=List[PlayerTransfers])
def get_player_transfers(request: Request,player_name:str):
    return list(request.app.database["player_transfers"].find({"player_name":player_name}))

@router.post("/player_values", response_description="Add new player value", status_code=status.HTTP_201_CREATED,response_model=PlayerValues)
def create_player_value(request:Request, player_value:PlayerValues=Body(...)):
    player_value = jsonable_encoder(player_value)
    new_player_value = request.app.database["player_values"].insert_one(player_value)
    created_player_value = request.app.database["player_values"].find_one({"_id": new_player_value.inserted_id})
    return created_player_value

@router.get("/player_values", response_description="get Player Values", status_code=status.HTTP_200_OK, response_model=List[PlayerValues])
def get_player_values(request: Request, player_name:str):
    try:
        results = aggregations(player_name,request)
        print(results)
        if results:
            return results
        raise HTTPException(status_code=404, detail=f"Player {player_name} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/all", response_description="Delete all data", status_code=status.HTTP_200_OK)
def delete_all(request:Request):
    request.app.database["teams"].delete_many({})
    request.app.database["player_injuries"].delete_many({})
    request.app.database["awards"].delete_many({})
    request.app.database["matches"].delete_many({})
    request.app.database["player_transfers"].delete_many({})
    request.app.database["player_values"].delete_many({})
    print("All data deleted")
    return Response(status_code=status.HTTP_200_OK)


def aggregations(player_name:str,request:Request):
    pipeline = [
        {"$match": {"player_name": player_name}},
        {"$project": {
            "player_name": 1,
            "value_history": 1,
            "values": {
                "$map": {
                    "input": "$value_history",
                    "as": "value",
                    "in": {"$toDouble": "$$value"}  # Convert string values to numbers
                }
            }
        }},
        {"$project": {
            "player_name": 1,
            "value_history": 1,
            "avgValue": {"$avg": "$values"},
            "maxValue": {"$max": "$values"},
            "minValue": {"$min": "$values"}
        }}
    ]
    return list(request.app.database["player_values"].aggregate(pipeline))  
