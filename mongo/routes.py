from fastapi import APIRouter, Body, Request, Response, HTTPException, status
from fastapi.encoders import jsonable_encoder
from typing import List, Optional

from model import Team, PlayerInjuries, Awards, Matches,PlayerTransfers,PlayerValues

router = APIRouter()

@router.post("/team", response_description="Add new team", status_code=status.HTTO_201_CREATED,response_model=Team)
def create_team(request:Request, team:Team=Body(...)):
    team = jsonable_encoder(team)
    new_team = request.app.database["teams"].insert_one(team)
    created_team = request.app.database["teams"].find_one({"_id": new_team.inserted_id})
    return created_team

@router.post("/player_injuries", response_description="Add new player injury", status_code=status.HTTO_201_CREATED,response_model=PlayerInjuries)
def create_player_injury(request:Request, player_injury:PlayerInjuries=Body(...)):
    player_injury = jsonable_encoder(player_injury)
    new_player_injury = request.app.database["player_injuries"].insert_one(player_injury)
    created_player_injury = request.app.database["player_injuries"].find_one({"_id": new_player_injury.inserted_id})
    return created_player_injury

@router.post("/awards", response_description="Add new award", status_code=status.HTTO_201_CREATED,response_model=Awards)
def create_award(request:Request, award:Awards=Body(...)):
    award = jsonable_encoder(award)
    new_award = request.app.database["awards"].insert_one(award)
    created_award = request.app.database["awards"].find_one({"_id": new_award.inserted_id})
    return created_award

@router.post("/matches", response_description="Add new match", status_code=status.HTTO_201_CREATED,response_model=Matches)
def create_match(request:Request, match:Matches=Body(...)):
    match = jsonable_encoder(match)
    new_match = request.app.database["matches"].insert_one(match)
    created_match = request.app.database["matches"].find_one({"_id": new_match.inserted_id})
    return created_match

@router.post("/player_transfers", response_description="Add new player transfer", status_code=status.HTTO_201_CREATED,response_model=PlayerTransfers)
def create_player_transfer(request:Request, player_transfer:PlayerTransfers=Body(...)):
    player_transfer = jsonable_encoder(player_transfer)
    new_player_transfer = request.app.database["player_transfers"].insert_one(player_transfer)
    created_player_transfer = request.app.database["player_transfers"].find_one({"_id": new_player_transfer.inserted_id})
    return created_player_transfer

@router.post("/player_values", response_description="Add new player value", status_code=status.HTTO_201_CREATED,response_model=PlayerValues)
def create_player_value(request:Request, player_value:PlayerValues=Body(...)):
    player_value = jsonable_encoder(player_value)
    new_player_value = request.app.database["player_values"].insert_one(player_value)
    created_player_value = request.app.database["player_values"].find_one({"_id": new_player_value.inserted_id})
    return created_player_value