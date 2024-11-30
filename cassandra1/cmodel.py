#!/usr/bin/env python3
import datetime
import logging
import random
import uuid

import time_uuid
from cassandra.query import BatchStatement

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""


# In this seccion since we need to create each table for each query I create each one of the tables I will use
#1
CREATE_TEAM_DATA_TABLE = """
    CREATE TABLE IF NOT EXISTS team_data (
        team_id DECIMAL,
        name TEXT,
        manager TEXT,
        country TEXT,
        players LIST<TEXT>,
        total_wins DECIMAL,
        total_losses DECIMAL,
        total_goals DECIMAL,
        last_match TIMESTAMP,
        next_match TIMESTAMP,
        gender TEXT,
        ranking DECIMAL,
        budget_allocated DECIMAL,
        budget_spent DECIMAL,
        PRIMARY KEY (team_id),
    ) 
""" 
#2
CREATE_DISPLAY_REAL_TIME_VISUALIZATION = """
    CREATE TABLE IF NOT EXISTS display_real_time_visualization (
        match_id DECIMAL,
        away_team TEXT,
        home_team TEXT,
        away_score DECIMAL,
        home_score DECIMAL,
        type TEXT,
        date TIMESTAMP,
        PRIMARY KEY (match_id)
    )
"""

#3
CREATE_PLAYER_HISTORY = """
    CREATE TABLE IF NOT EXISTS player_history (
        player_id DECIMAL,
        name TEXT,
        goals DECIMAL,
        assists DECIMAL,
        position TEXT,
        nationality TEXT,
        minutes_played DECIMAL,
        birthday TIMESTAMP,
        jersey_num LIST<DECIMAL>,
        PRIMARY KEY (name)
    )
"""
#4
CREATE_TEAM_HISTORY = """
    CREATE TABLE IF NOT EXISTS team_history (
        team_id DECIMAL,
        name TEXT,
        manager TEXT,
        country TEXT,
        players LIST<TEXT>,
        total_wins DECIMAL,
        total_losses DECIMAL,
        total_goals DECIMAL,
        last_match TIMESTAMP,
        next_match TIMESTAMP,
        gender TEXT,
        ranking DECIMAL,
        budget_allocated DECIMAL,
        budget_spent DECIMAL,
        PRIMARY KEY (name)
    )
""" 

#5
CREATE_AFITION_STATUS = """
    CREATE TABLE IF NOT EXISTS afition_status (
        stadium_id DECIMAL,
        name TEXT,  
        country TEXT,  
        capacity DECIMAL,
        average_assistance DECIMAL,
        areas LIST<TEXT>,
        next_match TIMESTAMP,
        PRIMARY KEY ((country), capacity)
    ) WITH CLUSTERING ORDER BY (capacity DESC)
""" 

#6
CREATE_PLAYERS_BY_TEAM = """
    CREATE TABLE IF NOT EXISTS players_by_team (
        team_id DECIMAL,
        name TEXT,
        manager TEXT,
        country TEXT,
        players LIST<TEXT>,
        gender TEXT,
        PRIMARY KEY (name)
    )
""" 

#7
CREATE_TEAM_RANKING = """
    CREATE TABLE IF NOT EXISTS team_ranking (
        team_id DECIMAL,
        name TEXT,
        country TEXT,
        total_wins DECIMAL,
        total_losses DECIMAL,
        total_goals DECIMAL,
        gender TEXT,
        ranking DECIMAL,
        PRIMARY KEY ((country), ranking)
    ) WITH CLUSTERING ORDER BY (ranking DESC)
""" 

    

#8
CREATE_TEAM_BUDGETS = """
    CREATE TABLE IF NOT EXISTS team_budgets (
        team_id DECIMAL,
        name TEXT,
        country TEXT,
        manager TEXT,
        budget_allocated DECIMAL,
        budget_spent DECIMAL,
        PRIMARY KEY ((country), budget_allocated)
    ) WITH CLUSTERING ORDER BY (budget_allocated DESC)

""" 

#9
CREATE_COMPARE_TEAMS = """
    CREATE TABLE IF NOT EXISTS compare_teams (
        team_id DECIMAL,
        name TEXT,
        total_wins DECIMAL,
        total_losses DECIMAL,
        total_goals DECIMAL,
        gender TEXT,
        ranking DECIMAL,
        PRIMARY KEY ((name), ranking)
    ) WITH CLUSTERING ORDER BY (ranking ASC)
""" 
#10
CREATE_LEAGUE_STANDING = """
    CREATE TABLE IF NOT EXISTS league_standing (
        league_id DECIMAL,
        name TEXT,
        country TEXT,
        seasons DECIMAL,
        teams LIST<TEXT>,
        date TIMESTAMP,
        level TEXT,
        points_system TEXT,
        format TEXT,
        PRIMARY KEY ((league_id), level)
    ) WITH CLUSTERING ORDER BY (level ASC)
""" 

#11
CREATE_STADIUM_ATTENDANCE_TRENDS = """
    CREATE TABLE IF NOT EXISTS analyze_attendance_trends (
        stadium_id DECIMAL,
        name TEXT,  
        country TEXT,  
        capacity DECIMAL,
        average_assistance DECIMAL,
        areas LIST<TEXT>,
        next_match TIMESTAMP,
        PRIMARY KEY ((country), average_assistance)
    ) WITH CLUSTERING ORDER BY (average_assistance DESC)
"""

#12
CREATE_PLAYER_JERSEY_HISTORY = """
    CREATE TABLE IF NOT EXISTS player_jersey_history (
        player_id DECIMAL,
        name TEXT,
        jersey_num LIST<DECIMAL>,
        PRIMARY KEY (name)
    )
"""

#Deleting tables for fresh restart
#1
DELETE_TEAM_DATA_TABLE = """
    DROP TABLE IF EXISTS team_data 
""" 
#2
DELETE_DISPLAY_REAL_TIME_VISUALIZATION = """
    DROP TABLE IF EXISTS display_real_time_visualization 
"""

#3
DELETE_PLAYER_HISTORY = """
    DROP TABLE IF EXISTS player_history 
"""
#4
DELETE_TEAM_HISTORY = """
    DROP TABLE IF EXISTS team_history 
""" 

#5
DELETE_AFITION_STATUS = """
    DROP TABLE IF EXISTS afition_status 
""" 

#6
DELETE_PLAYERS_BY_TEAM = """
    DROP TABLE IF EXISTS players_by_team 
""" 

#7
DELETE_TEAM_RANKING = """
    DROP TABLE IF EXISTS team_ranking 
""" 

#8
DELETE_TEAM_BUDGETS = """
    DROP TABLE IF EXISTS team_budgets 

""" 

#9
DELETE_COMPARE_TEAMS = """
    DROP TABLE IF EXISTS compare_teams 
""" 
#10
DELETE_LEAGUE_STANDING = """
    DROP TABLE IF EXISTS league_standing 
""" 

#11
DELETE_STADIUM_ATTENDANCE_TRENDS = """
    DROP TABLE IF EXISTS analyze_attendance_trends 
"""

#12
DELETE_PLAYER_JERSEY_HISTORY = """
    DROP TABLE IF EXISTS player_jersey_history 
"""

#Querys
#1
SELECT_TEAM_DATA_TABLE = """
     SELECT name, manager, country, players, total_wins, total_losses, total_goals, last_match, next_match, gender, ranking, budget_allocated, budget_spent
    FROM team_data
"""

#2
SELECT_DISPLAY_REAL_TIME_VISUALIZATION = """
    SELECT *
    FROM display_real_time_visualization 
"""
#3
SELECT_PLAYER_HISTORY = """
    SELECT  name, goals, assists, position, nationality, minutes_played, birthday, jersey_num 
    FROM player_history
    WHERE name = ?
"""


#4
SELECT_TEAM_HISTORY = """
    SELECT name, manager, country, players, total_wins, total_losses, total_goals, last_match, next_match, gender, ranking, budget_allocated, budget_spent
    FROM team_history
    WHERE name = ? 
"""
#5
SELECT_AFITION_STATUS = """
    SELECT name, country, capacity, average_assistance, areas, next_match
    FROM afition_status
    WHERE country = ? AND capacity >= ?
"""

#6
SELECT_PLAYERS_BY_TEAM = """
    SELECT name, manager, country, players, gender
    FROM players_by_team
    WHERE name = ? 
"""
#7
SELECT_TEAM_RANKING = """
    SELECT name, country,total_wins, total_losses, total_goals, gender, ranking
    FROM team_ranking
    WHERE country = ? AND ranking >= ? 
"""


#8
SELECT_TEAM_BUDGETS = """
    SELECT name, country, manager, budget_allocated, budget_spent
    FROM team_budgets
    WHERE country = ? AND budget_allocated >= ? 
"""
#9
SELECT_COMPARE_TEAMS = """
    SELECT name, total_wins, total_losses, total_goals, gender, ranking
    FROM compare_teams
    WHERE name IN (?,?)
"""
#10
SELECT_LEAGUE_STANDING = """
    SELECT name, country, seasons, teams, date, level, points_system, format
    FROM league_standing
"""
#11
SELECT_STADIUM_ATTENDANCE_TRENDS = """
    SELECT name, country, capacity, average_assistance, areas, next_match
    FROM analyze_attendance_trends
    WHERE country = ? AND average_assistance >= ?
"""
#12
SELECT_PLAYER_JERSEY_HISTORY = """
    SELECT name, jersey_num
    FROM player_jersey_history
    WHERE name = ?  
"""


def bulk_insert(session):
    #1
    session.execute("INSERT INTO team_data (team_id,name,manager,country,players,total_wins,total_losses,total_goals,last_match,next_match,gender,ranking,budget_allocated,budget_spent) VALUES (1, 'Cruz Azul', 'Martin Anselmi', 'Mexico', ['Andres Gudiño', 'Luis Jimenez', 'Kevin Mier', 'Jorge Sanchez', 'Willer Ditta', 'Camilo Candido', 'Raymundo Rubio', 'Carlos Vargas', 'Gonzalo Piovi', 'Jose Suarez', 'Jorge Garcia'], 15, 8, 45, '2024-11-20', '2024-11-27', 'Male', 5, 2000000, 1500000)")
    session.execute("INSERT INTO team_data (team_id,name,manager,country,players,total_wins,total_losses,total_goals,last_match,next_match,gender,ranking,budget_allocated,budget_spent) VALUES (2, 'Manchester United', 'Ruben Amorim', 'England', ['Tom Heaton', 'Victor Lindelof', 'Harry Maguire', 'Lisandro Martinez', 'Tyrell Malacia', 'Diogo Dalot', 'Luke Shaw', 'Bruno Fernandez', 'Christian Eriksen', 'Amad Diallo', 'Casemiro'], 25, 5, 65, '2024-11-18', '2024-11-30', 'Male', 2, 5000000, 4500000)")
    session.execute("INSERT INTO team_data (team_id,name,manager,country,players,total_wins,total_losses,total_goals,last_match,next_match,gender,ranking,budget_allocated,budget_spent) VALUES (3, 'Barcelona', 'Pere Romeu', 'Spain', ['Peña', 'Pau Cubarsi', 'Alejandro Balde', 'Ronald Araujo', 'Gavi', 'Pedri', 'Pablo Torre', 'Ferran Torres', 'Ansu Fati', 'Pau Victor', 'Raphinha'], 30, 2, 80, '2024-11-15', '2024-11-29', 'Male', 1, 8000000, 7500000)")
    session.execute("INSERT INTO team_data (team_id,name,manager,country,players,total_wins,total_losses,total_goals,last_match,next_match,gender,ranking,budget_allocated,budget_spent) VALUES (4, 'Feyenoord Vrouwen', 'Jessica Torny', 'Netherlands', ['Jacintha Weimar', 'Roos Van Ejik', 'Oliwia Szymczak', 'Maruschka Waldus', 'Danique Ypema', 'Amber Verspaget', 'Celainy Obispo', 'Justine Brandau', 'Emma Pijnenburg', 'Esmee De Graaf', 'Toko Koga'], 18, 10, 50, '2024-11-19', '2024-12-01', 'Female', 4, 3000000, 2500000)")
    session.execute("INSERT INTO team_data (team_id,name,manager,country,players,total_wins,total_losses,total_goals,last_match,next_match,gender,ranking,budget_allocated,budget_spent) VALUES (5, 'Atlas', 'Beñat San Jose', 'Mexico', ['Jose Hernandez', 'Camilo Vargas', 'Antonio Sanchez', 'Martin Nervo', 'Idekel Dominguez', 'Adrian Mora', 'Gaddi Aguirre', 'Luis Reyes', 'Jose Rivaldo Lozano', 'Carlos Robles', 'Matheus Doria'], 12, 12, 36, '2024-11-22', '2024-11-28', 'Male', 6, 1800000, 1400000)")
    
    #2
    session.execute("INSERT INTO display_real_time_visualization(match_id, away_team, home_team, away_score, home_score, type, date) VALUES (1, 'Manchester United', 'Barcelona', 2, 1, 'League', '2024-11-22')")
    session.execute("INSERT INTO display_real_time_visualization(match_id, away_team, home_team, away_score, home_score, type, date) VALUES (2, 'Cruz Azul', 'Atlas', 1, 1, 'Cup', '2024-11-23')")
    session.execute("INSERT INTO display_real_time_visualization(match_id, away_team, home_team, away_score, home_score, type, date) VALUES (3, 'Feyenoord Vrouwen', 'Barcelona', 0, 3, 'Friendly', '2024-11-24')")
    session.execute("INSERT INTO display_real_time_visualization(match_id, away_team, home_team, away_score, home_score, type, date) VALUES (4, 'Atlas', 'Manchester United', 1, 4, 'League', '2024-11-25')")
    session.execute("INSERT INTO display_real_time_visualization(match_id, away_team, home_team, away_score, home_score, type, date) VALUES (5, 'Barcelona', 'Cruz Azul', 2, 0, 'Cup', '2024-11-26')")

    #3
    session.execute("INSERT INTO player_history (player_id, name, goals, assists, position, nationality, minutes_played, birthday, jersey_num) VALUES (1, 'Lionel Messi', 804, 350, 'Forward', 'Argentina', 73200, '1987-06-24', [10, 30])")
    session.execute("INSERT INTO player_history (player_id, name, goals, assists, position, nationality, minutes_played, birthday, jersey_num) VALUES (2, 'Cristiano Ronaldo', 850, 230, 'Forward', 'Portugal', 75000, '1985-02-05', [7, 28, 11])")
    session.execute("INSERT INTO player_history (player_id, name, goals, assists, position, nationality, minutes_played, birthday, jersey_num) VALUES (3, 'Kevin De Bruyne', 140, 220, 'Midfielder', 'Belgium', 54000, '1991-06-28', [17])")
    session.execute("INSERT INTO player_history (player_id, name, goals, assists, position, nationality, minutes_played, birthday, jersey_num) VALUES (4, 'Megan Rapinoe', 63, 73, 'Forward', 'USA', 31000, '1985-07-05', [15, 16])")
    session.execute("INSERT INTO player_history (player_id, name, goals, assists, position, nationality, minutes_played, birthday, jersey_num) VALUES (5, 'Virgil van Dijk', 40, 15, 'Defender', 'Netherlands', 45000, '1991-07-08', [4, 7])")

    #4
    session.execute("INSERT INTO team_history (team_id,name,manager,country,players,total_wins,total_losses,total_goals,last_match,next_match,gender,ranking,budget_allocated,budget_spent) VALUES (1, 'Cruz Azul', 'Martin Anselmi', 'Mexico', ['Andres Gudiño', 'Luis Jimenez', 'Kevin Mier', 'Jorge Sanchez', 'Willer Ditta', 'Camilo Candido', 'Raymundo Rubio', 'Carlos Vargas', 'Gonzalo Piovi', 'Jose Suarez', 'Jorge Garcia'], 15, 8, 45, '2024-11-20', '2024-11-27', 'Male', 5, 2000000, 1500000)")
    session.execute("INSERT INTO team_history (team_id,name,manager,country,players,total_wins,total_losses,total_goals,last_match,next_match,gender,ranking,budget_allocated,budget_spent) VALUES (2, 'Manchester United', 'Ruben Amorim', 'England', ['Tom Heaton', 'Victor Lindelof', 'Harry Maguire', 'Lisandro Martinez', 'Tyrell Malacia', 'Diogo Dalot', 'Luke Shaw', 'Bruno Fernandez', 'Christian Eriksen', 'Amad Diallo', 'Casemiro'], 25, 5, 65, '2024-11-18', '2024-11-30', 'Male', 2, 5000000, 4500000)")
    session.execute("INSERT INTO team_history (team_id,name,manager,country,players,total_wins,total_losses,total_goals,last_match,next_match,gender,ranking,budget_allocated,budget_spent) VALUES (3, 'Barcelona', 'Pere Romeu', 'Spain', ['Peña', 'Pau Cubarsi', 'Alejandro Balde', 'Ronald Araujo', 'Gavi', 'Pedri', 'Pablo Torre', 'Ferran Torres', 'Ansu Fati', 'Pau Victor', 'Raphinha'], 30, 2, 80, '2024-11-15', '2024-11-29', 'Male', 1, 8000000, 7500000)")
    session.execute("INSERT INTO team_history (team_id,name,manager,country,players,total_wins,total_losses,total_goals,last_match,next_match,gender,ranking,budget_allocated,budget_spent) VALUES (4, 'Feyenoord Vrouwen', 'Jessica Torny', 'Netherlands', ['Jacintha Weimar', 'Roos Van Ejik', 'Oliwia Szymczak', 'Maruschka Waldus', 'Danique Ypema', 'Amber Verspaget', 'Celainy Obispo', 'Justine Brandau', 'Emma Pijnenburg', 'Esmee De Graaf', 'Toko Koga'], 18, 10, 50, '2024-11-19', '2024-12-01', 'Female', 4, 3000000, 2500000)")
    session.execute("INSERT INTO team_history (team_id,name,manager,country,players,total_wins,total_losses,total_goals,last_match,next_match,gender,ranking,budget_allocated,budget_spent) VALUES (5, 'Atlas', 'Beñat San Jose', 'Mexico', ['Jose Hernandez', 'Camilo Vargas', 'Antonio Sanchez', 'Martin Nervo', 'Idekel Dominguez', 'Adrian Mora', 'Gaddi Aguirre', 'Luis Reyes', 'Jose Rivaldo Lozano', 'Carlos Robles', 'Matheus Doria'], 12, 12, 36, '2024-11-22', '2024-11-28', 'Male', 6, 1800000, 1400000)")

    #5
    session.execute("INSERT INTO afition_status (stadium_id, name, country, capacity, average_assistance, areas, next_match) VALUES (1, 'Estadio Azteca', 'Mexico', 87000, 65000, ['VIP', 'General', 'Palcos'], '2024-11-26 20:00')")
    session.execute("INSERT INTO afition_status (stadium_id, name, country, capacity, average_assistance, areas, next_match) VALUES (2, 'Old Trafford', 'England', 75000, 72000, ['VIP', 'Family', 'East Stand'], '2024-11-28 18:30')")
    session.execute("INSERT INTO afition_status (stadium_id, name, country, capacity, average_assistance, areas, next_match) VALUES (3, 'Camp Nou', 'Spain', 99354, 85000, ['General', 'VIP', 'Corporate Boxes'], '2024-11-25 21:00')")
    session.execute("INSERT INTO afition_status (stadium_id, name, country, capacity, average_assistance, areas, next_match) VALUES (4, 'De Kuip', 'Netherlands', 51000, 45000, ['VIP', 'General'], '2024-11-29 19:00')")
    session.execute("INSERT INTO afition_status (stadium_id, name, country, capacity, average_assistance, areas, next_match) VALUES (5, 'Estadio Jalisco', 'Mexico', 55000, 40000, ['VIP', 'General'], '2024-11-27 20:30')")

    #6
    session.execute("INSERT INTO players_by_team (team_id, name, manager, country, players, gender) VALUES (1, 'Cruz Azul', 'Martin Anselmi', 'Mexico', ['Andres Gudiño', 'Luis Jimenez', 'Kevin Mier', 'Jorge Sanchez', 'Willer Ditta', 'Camilo Candido', 'Raymundo Rubio', 'Carlos Vargas', 'Gonzalo Piovi', 'Jose Suarez', 'Jorge Garcia'], 'Male')")
    session.execute("INSERT INTO players_by_team (team_id, name, manager, country, players, gender) VALUES (2, 'Manchester United', 'Ruben Amorim', 'England', ['Tom Heaton', 'Victor Lindelof', 'Harry Maguire', 'Lisandro Martinez', 'Tyrell Malacia', 'Diogo Dalot', 'Luke Shaw', 'Bruno Fernandez', 'Christian Eriksen', 'Amad Diallo', 'Casemiro'], 'Male')")
    session.execute("INSERT INTO players_by_team (team_id, name, manager, country, players, gender) VALUES (3, 'Barcelona', 'Pere Romeu', 'Spain', ['Peña', 'Pau Cubarsi', 'Alejandro Balde', 'Ronald Araujo', 'Gavi', 'Pedri', 'Pablo Torre', 'Ferran Torres', 'Ansu Fati', 'Pau Victor', 'Raphinha'], 'Male')")
    session.execute("INSERT INTO players_by_team (team_id, name, manager, country, players, gender) VALUES (4, 'Feyenoord Vrouwen', 'Jessica Torny', 'Netherlands', ['Jacintha Weimar', 'Roos Van Ejik', 'Oliwia Szymczak', 'Maruschka Waldus', 'Danique Ypema', 'Amber Verspaget', 'Celainy Obispo', 'Justine Brandau', 'Emma Pijnenburg', 'Esmee De Graaf', 'Toko Koga'], 'Female')")
    session.execute("INSERT INTO players_by_team (team_id, name, manager, country, players, gender) VALUES (5, 'Atlas', 'Beñat San Jose', 'Mexico', ['Jose Hernandez', 'Camilo Vargas', 'Antonio Sanchez', 'Martin Nervo', 'Idekel Dominguez', 'Adrian Mora', 'Gaddi Aguirre', 'Luis Reyes', 'Jose Rivaldo Lozano', 'Carlos Robles', 'Matheus Doria'], 'Male')")

    #7
    session.execute("INSERT INTO team_ranking (team_id, name, country, total_wins, total_losses, total_goals, gender, ranking) VALUES (1, 'Cruz Azul', 'Mexico', 15, 8, 45, 'Male', 5)")
    session.execute("INSERT INTO team_ranking (team_id, name, country, total_wins, total_losses, total_goals, gender, ranking) VALUES (2, 'Manchester United', 'England', 25, 5, 65, 'Male', 2)")
    session.execute("INSERT INTO team_ranking (team_id, name, country, total_wins, total_losses, total_goals, gender, ranking) VALUES (3, 'Barcelona', 'Spain', 30, 2, 80, 'Male', 1)")
    session.execute("INSERT INTO team_ranking (team_id, name, country, total_wins, total_losses, total_goals, gender, ranking) VALUES (4, 'Feyenoord Vrouwen', 'Netherlands', 18, 10, 50, 'Female', 4)")
    session.execute("INSERT INTO team_ranking (team_id, name, country, total_wins, total_losses, total_goals, gender, ranking) VALUES (5, 'Atlas', 'Mexico', 12, 12, 36, 'Male', 6)")

    #8
    session.execute("INSERT INTO team_budgets (team_id, name, country, manager, budget_allocated, budget_spent) VALUES (1, 'Cruz Azul', 'Mexico','Martin Anselmi', 2000000, 1500000)")
    session.execute("INSERT INTO team_budgets (team_id, name, country, manager, budget_allocated, budget_spent) VALUES (2, 'Manchester United', 'England','Ruben Amorim', 5000000, 4500000)")
    session.execute("INSERT INTO team_budgets (team_id, name, country, manager, budget_allocated, budget_spent) VALUES (3, 'Barcelona', 'Spain','Pere Romeu', 8000000, 7500000)")
    session.execute("INSERT INTO team_budgets (team_id, name, country, manager, budget_allocated, budget_spent) VALUES (4, 'Feyenoord Vrouwen', 'Netherlands','Jessica Torny', 3000000, 2500000)")
    session.execute("INSERT INTO team_budgets (team_id, name, country, manager, budget_allocated, budget_spent) VALUES (5, 'Atlas', 'Mexico', 'Beñat San Jose', 1800000, 1400000)")


    #9
    session.execute("INSERT INTO compare_teams (team_id, name, total_wins, total_losses, total_goals, gender, ranking) VALUES (1, 'Cruz Azul', 15, 8, 45, 'Male', 5)")
    session.execute("INSERT INTO compare_teams (team_id, name, total_wins, total_losses, total_goals, gender, ranking) VALUES (2, 'Manchester United', 25, 5, 65, 'Male', 2)")
    session.execute("INSERT INTO compare_teams (team_id, name, total_wins, total_losses, total_goals, gender, ranking) VALUES (3, 'Barcelona', 30, 2, 80, 'Male', 1)")
    session.execute("INSERT INTO compare_teams (team_id, name, total_wins, total_losses, total_goals, gender, ranking) VALUES (4, 'Feyenoord Vrouwen', 18, 10, 50, 'Female', 4)")
    session.execute("INSERT INTO compare_teams (team_id, name, total_wins, total_losses, total_goals, gender, ranking) VALUES (5, 'Atlas', 12, 12, 36, 'Male', 6)")


    #10
    session.execute("INSERT INTO league_standing (league_id, name, country, seasons, teams, date, level, points_system, format) VALUES (1, 'La Liga', 'Spain', 2024, ['Barcelona', 'Real Madrid', 'Atletico Madrid'], '2024-11-01', 'Top Division', '3 points for win, 1 for draw, 0 for loss', 'Round robin')")
    session.execute("INSERT INTO league_standing (league_id, name, country, seasons, teams, date, level, points_system, format) VALUES (2, 'Premier League', 'England', 2024, ['Manchester United', 'Manchester City', 'Chelsea'], '2024-11-01', 'Top Division', '3 points for win, 1 for draw, 0 for loss', 'Round robin')")
    session.execute("INSERT INTO league_standing (league_id, name, country, seasons, teams, date, level, points_system, format) VALUES (3, 'Eredivisie', 'Netherlands', 2024, ['Ajax', 'PSV Eindhoven', 'Feyenoord'], '2024-11-01', 'Top Division', '3 points for win, 1 for draw, 0 for loss', 'Round robin')")
    session.execute("INSERT INTO league_standing (league_id, name, country, seasons, teams, date, level, points_system, format) VALUES (4, 'Ligue 1', 'France', 2024, ['Paris Saint-Germain', 'Lyon', 'Marseille'], '2024-11-01', 'Top Division', '3 points for win, 1 for draw, 0 for loss', 'Round robin')")
    session.execute("INSERT INTO league_standing (league_id, name, country, seasons, teams, date, level, points_system, format) VALUES (5, 'MLS', 'USA', 2024, ['Los Angeles FC', 'New York City FC', 'Seattle Sounders'], '2024-11-01', 'Top Division', '3 points for win, 1 for draw, 0 for loss', 'Round robin')")


    #11
    session.execute("INSERT INTO analyze_attendance_trends (stadium_id, name, country, capacity, average_assistance, areas, next_match) VALUES (1, 'Estadio Azteca', 'Mexico', 87000, 65000, ['VIP', 'General', 'Palcos'], '2024-11-26 20:00')")
    session.execute("INSERT INTO analyze_attendance_trends (stadium_id, name, country, capacity, average_assistance, areas, next_match) VALUES (2, 'Old Trafford', 'England',75000, 72000, ['VIP', 'Family', 'East Stand'], '2024-11-28 18:30')")
    session.execute("INSERT INTO analyze_attendance_trends (stadium_id, name, country, capacity, average_assistance, areas, next_match) VALUES (3, 'Camp Nou', 'Spain', 99354, 85000, ['General', 'VIP', 'Corporate Boxes'], '2024-11-25 21:00')")
    session.execute("INSERT INTO analyze_attendance_trends (stadium_id, name, country, capacity, average_assistance, areas, next_match) VALUES (4, 'De Kuip', 'Netherlands', 51000, 45000, ['VIP', 'General'], '2024-11-29 19:00')")
    session.execute("INSERT INTO analyze_attendance_trends (stadium_id, name, country, capacity, average_assistance, areas, next_match) VALUES (5, 'Estadio Jalisco', 'Mexico', 55000, 40000, ['VIP', 'General'], '2024-11-27 20:30')")

    #12
    session.execute("INSERT INTO player_jersey_history (player_id, name, jersey_num) VALUES (1, 'Lionel Messi', [10, 30])")  
    session.execute("INSERT INTO player_jersey_history (player_id, name, jersey_num) VALUES (2, 'Cristiano Ronaldo', [7, 28, 11])")  
    session.execute("INSERT INTO player_jersey_history (player_id, name, jersey_num) VALUES (3, 'Kevin De Bruyne', [17])")  
    session.execute("INSERT INTO player_jersey_history (player_id, name, jersey_num) VALUES (4, 'Megan Rapinoe', [15, 16])") 
    session.execute("INSERT INTO player_jersey_history (player_id, name, jersey_num) VALUES (5, 'Virgil van Dijk', [4, 7])")  



def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_TEAM_DATA_TABLE)
    session.execute(CREATE_DISPLAY_REAL_TIME_VISUALIZATION)
    session.execute(CREATE_PLAYER_HISTORY)
    session.execute(CREATE_TEAM_HISTORY)
    session.execute(CREATE_AFITION_STATUS)
    session.execute(CREATE_PLAYERS_BY_TEAM)
    session.execute(CREATE_TEAM_RANKING)
    session.execute(CREATE_TEAM_BUDGETS)
    session.execute(CREATE_COMPARE_TEAMS)
    session.execute(CREATE_LEAGUE_STANDING)
    session.execute(CREATE_STADIUM_ATTENDANCE_TRENDS)
    session.execute(CREATE_PLAYER_JERSEY_HISTORY)

def delete_schema(session):
    log.info("Deleting model schema")
    session.execute(DELETE_TEAM_DATA_TABLE)
    session.execute(DELETE_DISPLAY_REAL_TIME_VISUALIZATION)
    session.execute(DELETE_PLAYER_HISTORY)
    session.execute(DELETE_TEAM_HISTORY)
    session.execute(DELETE_AFITION_STATUS)
    session.execute(DELETE_PLAYERS_BY_TEAM)
    session.execute(DELETE_TEAM_RANKING)
    session.execute(DELETE_TEAM_BUDGETS)
    session.execute(DELETE_COMPARE_TEAMS)
    session.execute(DELETE_LEAGUE_STANDING)
    session.execute(DELETE_STADIUM_ATTENDANCE_TRENDS)
    session.execute(DELETE_PLAYER_JERSEY_HISTORY)
#1
def storedTeamData(session):
    log.info(f"Retrieving team")
    
    query = session.prepare(SELECT_TEAM_DATA_TABLE)
    rows = session.execute(query)
    for row in rows:
        print(f"=== Team: {row.name} ===")
        print(f"- Manager: {row.manager}")
        print(f"- Country: {row.country}")
        print(f"- Players: {row.players}")
        print(f"- Total Wins: {row.total_wins}")
        print(f"- Total Losses: {row.total_losses}")
        print(f"- Total Goal: {row.total_goals}")
        print(f"- Last match: {row.last_match}")
        print(f"- Next Match: {row.next_match}")
        print(f"- Gender: {row.gender}")
        print(f"- Ranking: {row.ranking}")
        print(f"- Budget Allocated: {row.budget_allocated}")
        print(f"- Budget Spent: {row.budget_spent}")

#2
def displayRealTimeVisualization(session):
    log.info(f"Retrieving Player")
    query = session.prepare(SELECT_DISPLAY_REAL_TIME_VISUALIZATION)
    rows = session.execute(query)
    for row in rows:
        print(f"=== Match: {row.match_id} ===")
        print(f"- Away Team: {row.away_team}")
        print(f"- Home team: {row.home_team}")
        print(f"- Away Score: {row.away_score}")
        print(f"- Home Score: {row.home_score}")
        print(f"- Type of match: {row.type}")
        print(f"- Date: {row.date}")

#3
def getPlayerHistory(session, n):
    log.info(f"Retrieving Player")
    query = session.prepare(SELECT_PLAYER_HISTORY)
    rows = session.execute(query, [n])
    for row in rows:
        print(f"=== Player: {row.name} ===")
        print(f"- Goals: {row.goals}")
        print(f"- Assists: {row.assists}")
        print(f"- Position: {row.position}")
        print(f"- Nationality: {row.nationality}")
        print(f"- Minutes played: {row.minutes_played}")
        print(f"- Birthday: {row.birthday}")
        print(f"- Jersey Number: {row.jersey_num}")
      

#4
def getTeamHistory(session, n):
    log.info(f"Retrieving team")
    
    query = session.prepare(SELECT_TEAM_HISTORY)
    rows = session.execute(query, [n])
    for row in rows:
        print(f"=== Team: {row.name} ===")
        print(f"- Manager: {row.manager}")
        print(f"- Country: {row.country}")
        print(f"- Players: {row.players}")
        print(f"- Total Wins: {row.total_wins}")
        print(f"- Total Losses: {row.total_losses}")
        print(f"- Total Goal: {row.total_goals}")
        print(f"- Last match: {row.last_match}")
        print(f"- Next Match: {row.next_match}")
        print(f"- Gender: {row.gender}")
        print(f"- Ranking: {row.ranking}")
        print(f"- Budget Allocated: {row.budget_allocated}")
        print(f"- Budget Spent: {row.budget_spent}")

#5
def affitionStatus(session, c,ca):
    log.info(f"Retrieving stadiums capacity")
    
    query = session.prepare(SELECT_AFITION_STATUS)
    rows = session.execute(query, [c, ca])
    for row in rows:
        print(f"=== Stadium: {row.name} ===")
        print(f"- Country: {row.country}")
        print(f"- Capacity: {row.capacity}")
        print(f"- Average Assistance: {row.average_assistance}")
        print(f"- Areas: {row.areas}")
        print(f"- Next Match: {row.next_match}")

#6
def getPlayersByTeam(session, n):
    log.info(f"Retrieving players")
    
    query = session.prepare(SELECT_PLAYERS_BY_TEAM)
    rows = session.execute(query, [n])
    for row in rows:
        print(f"=== Team: {row.name} ===")
        print(f"- Manager: {row.manager}")
        print(f"- Country: {row.country}")
        print(f"- Players: {row.players}")
        print(f"- Gender: {row.gender}")
     

#7
def getTeamRanking(session, cn,r):
    log.info(f"Retrieving teams")
    
    query = session.prepare(SELECT_TEAM_RANKING)
    rows = session.execute(query, [cn, r])
    for row in rows:
        print(f"=== Team: {row.name} ===")
        print(f"- Country: {row.country}")
        print(f"- Total Wins: {row.total_wins}")
        print(f"- Total Losses: {row.total_losses}")
        print(f"- Total goals: {row.total_goals}")
        print(f"- Gender: {row.gender}")
        print(f"- Ranking: {row.ranking}")
    

#8
def manageTeamBudgets(session, c,ba):
    log.info(f"Retrieving budgets")
    
    query = session.prepare(SELECT_TEAM_BUDGETS)
    rows = session.execute(query, [c,ba])
    for row in rows:
        print(f"=== Team: {row.name} ===")
        print(f"- Country: {row.country}")
        print(f"- Manager: {row.manager}")
        print(f"- Budget Allocated: {row.budget_allocated}")
        print(f"- Budget Spent: {row.budget_spent}")

#9
def compareTeams(session, n1, n2):
    log.info(f"Retrieving teams")
    
    query = session.prepare(SELECT_COMPARE_TEAMS)
    rows = session.execute(query, [n1, n2])
    for row in rows:
        print(f"=== Team: {row.name} ===")
        print(f"- Total Wins: {row.total_wins}")
        print(f"- Total Losses: {row.total_losses}")
        print(f"- Total goals: {row.total_goals}")
        print(f"- Gender: {row.gender}")
        print(f"- Ranking: {row.ranking}")
    

#10
def getLeagueStandings(session):
    log.info(f"Retrieving leagues")
    
    query = session.prepare(SELECT_LEAGUE_STANDING)
    rows = session.execute(query)
    for row in rows:
        print(f"=== League: {row.name} ===")
        print(f"- Country: {row.country}")
        print(f"- Season: {row.seasons}")
        print(f"- Teams: {row.teams}")
        print(f"- Date: {row.date}")
        print(f"- Level: {row.level}")
        print(f"- Point systems: {row.points_system}")
        print(f"- Format: {row.format}")

#Querys function
#11
def analyzeAttendanceTrends(session, c,ac):
    log.info(f"Retrieving stadiums capacity")
    
    query = session.prepare(SELECT_STADIUM_ATTENDANCE_TRENDS)
    rows = session.execute(query, [c,ac])
    for row in rows:
        print(f"=== Stadium: {row.name} ===")
        print(f"- Country: {row.country}")
        print(f"- Capacity: {row.capacity}")
        print(f"- Average Assistance: {row.average_assistance}")
        print(f"- Areas: {row.areas}")
        print(f"- Next Match: {row.next_match}")

#12
def get_players_jersey_history(session, n):
    log.info(f"Retrieving {n} player jersey history")
    
    query = session.prepare(SELECT_PLAYER_JERSEY_HISTORY)
    rows = session.execute(query, [n])
    for row in rows:
        print(f"=== Player: {row.name} ===")
        print(f"- Jerseys: {row.jersey_num}")
        
        
