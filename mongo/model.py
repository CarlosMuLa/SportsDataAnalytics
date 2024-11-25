#!/usr/bin/env python3
import uuid
from typing import List, Optional,Any
from pydantic import BaseModel, Field
from datetime import datetime


class Team(BaseModel):
    id: str = Field(default_factory=uuid.uuid4, alias='_id')
    team_name: str = Field(...) #the (...) means that the field is required
    email: str = Field(...)
    password: str = Field(...)
    owner: str = Field(...)
    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "team_name": "Team 1",
                "email": "chivas@gmail.com",
                "password": "password",
                "owner": "Chivas"
            }
        }

class PlayerInjuries(BaseModel):
    injury_id: str = Field(default_factory=uuid.uuid4, alias='_id')
    player_name: Optional[str] = Field(...)
    team_name: str = Field(...)
    injury_type: str = Field(...)
    start_date: datetime = Field(...)
    end_date: datetime = Field(...)
    medical_notes:str #optional field
    status: str = Field(...)
    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "player_id": "1",
                "team_id": "1",
                "injury_type": "ACL",
                "start_date": "2022-01-01",
                "end_date": "2022-02-01",
                "medical_notes": "Surgery required",
                "status": "Active"
            }
        }

class Awards(BaseModel):
    award_id: str = Field(default_factory=uuid.uuid4, alias='_id')
    recipient_type: str = Field(...)
    recipient_name: str = Field(...)
    award_name: str = Field(...)
    season: str = Field(...)
    category: str = Field(...)
    date_awarded: datetime = Field(...)
    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "recipient_type": "Player",
                "recipient_id": "1",
                "award_name": "MVP",
                "season": "2022",
                "category": "Offense",
                "date_awarded": "2022-01-01"
            }
        }
class Matches(BaseModel):
    match_id: str = Field(default_factory=uuid.uuid4, alias='_id')
    home_team_name: str = Field(...)
    away_team_name: str = Field(...)
    date: datetime = Field(...)
    status: str = Field(...) #score is a string
    #score str not required but not a list
    score: Optional[Any]
    officials: List[str]
    statistics: Optional[List[str]]
    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "home_team_id": "1",
                "away_team_id": "2",
                "date": "2022-01-01",
                "status": "Final",
                "score": "2-1",
                "officials": ["Ref1", "Ref2"],
                "statistics": ["Stat1", "Stat2"]
            }
        }   
class PlayerTransfers(BaseModel):
    team_name: str = Field(...)
    player_name: str = Field(...)
    from_team_name: str = Field(...)
    transfer_date: datetime = Field(...)
    fee: float = Field(...)
    contract_length: int = Field(...)
    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "team_id": "1",
                "player_id": "1",
                "from_team_id": "2",
                "transfer_date": "2022-01-01",
                "fee": 1000000,
                "contract_length": 3
            }
        }
class PlayerValues(BaseModel):
    player_name: str = Field(...)
    value_history: List[int]
    avgValue:Optional[float]
    maxValue:Optional[float]
    minValue:Optional[float]
    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "player_id": "1",
                "value_history": ["1000000", "2000000", "3000000"]
            }
        }

