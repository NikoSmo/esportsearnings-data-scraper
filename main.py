from datetime import datetime, date
import requests
import time
import pandas as pd
from tqdm import tqdm

import config

requests.packages.urllib3.disable_warnings()
# Change to your API token here. Register for a free API token at https://esportsearnings.com/api
API_TOKEN = config.API_TOKEN
# This is the minimum date for the tournaments to be queried. Longer history equals more data but also longer runtime
query_min_date = date(2023, 1, 1)

wait_time_in_seconds = 1  # This is the minimum wait time required to respect the API rate limit between calls
verbose = True

base_url = "http://api.esportsearnings.com/v0"
# Endpoints
recent_tournaments = "/LookupRecentTournaments"
highest_earning_players_by_game = "/LookupHighestEarningPlayersByGame"
highest_earning_teams_by_game = "/LookupHighestEarningTeamsByGame"
player_tournaments = "/LookupPlayerTournaments"
game_by_id = "/LookupGameById"
tournament_results_by_tournament_id = "/LookupTournamentResultsByTournamentId"
tournament_team_results_by_tournament_id = "/LookupTournamentTeamResultsByTournamentId"


def get_data(url: str, params: dict, verbose: str = False) -> pd.DataFrame:
    response = requests.get(url, params=params, verify=False)
    if response.status_code == 200:
        if not response.text:
            return pd.DataFrame()
        data = response.json()
        df = pd.json_normalize(data)
        return df
    else:
        print(f"Error: {response.status_code}")
        return pd.DataFrame()


def process_tournaments(df: pd.DataFrame) -> pd.DataFrame:
    date_columns = ["StartDate", "EndDate"]
    for date_column in date_columns:
        df[date_column] = pd.to_datetime(df[date_column])
        df[date_column] = df[date_column].dt.date
    df["TotalUSDPrize"] = df["TotalUSDPrize"].astype(float)
    return df


# Get all tournament data from recent_tournaments endpoint using offset for pagination
def get_tournament_data(
    query_until_date: date, limit: str = 100, verbose: str = False
) -> pd.DataFrame:
    print("Getting tournament data")
    url = base_url + recent_tournaments
    offset = 0
    first_run = True
    df_all = pd.DataFrame()
    df = pd.DataFrame()
    while first_run or df.shape[0] < limit or df_all.StartDate.min() > query_until_date:
        params = {"apikey": API_TOKEN, "offset": offset}
        df = get_data(url, params)
        df = process_tournaments(df)
        df_all = df_all.append(df)
        offset += limit
        first_run = False
        if verbose:
            print(f"{df.shape}", offset)
        time.sleep(wait_time_in_seconds)
    return df_all


def get_games_data(game_ids: list, verbose: bool = False) -> pd.DataFrame:
    print(f"Getting games data for {len(game_ids)} Games")
    url_games = base_url + game_by_id
    df_games = pd.DataFrame()
    for game_id in tqdm(game_ids):
        df_game = pd.DataFrame()
        params = {"apikey": API_TOKEN, "gameid": game_id}
        df_game = get_data(url_games, params)
        df_game["GameId"] = game_id
        df_games = df_games.append(df_game)
        time.sleep(wait_time_in_seconds)
        if verbose:
            print(f"Game Name: {df_game.GameName.values[0]}")
    return df_games


def get_game_earnings(
    game_ids: list, teamplay: bool, limit: int = 100, verbose=False,
) -> pd.DataFrame:
    if teamplay:
        print("Getting Games Earnings Team Data")
        url_earnings = base_url + highest_earning_teams_by_game
    else:
        print("Getting Games Earnings Solo Data")
        url_earnings = base_url + highest_earning_players_by_game
    df_game_earnings_all = pd.DataFrame()
    for game_id in tqdm(game_ids):
        offset = 0
        last_page = False
        while not last_page:
            params = {"apikey": API_TOKEN, "gameid": game_id, "offset": offset}
            df_game_earnings = get_data(url_earnings, params)
            df_game_earnings["GameId"] = game_id
            df_game_earnings_all = df_game_earnings_all.append(df_game_earnings)
            first_run = False
            offset += limit
            last_page = (
                df_game_earnings.shape[0] < limit
            )  # We assume that the last page is reached when the number of results is less than the limit
            time.sleep(wait_time_in_seconds)
            if verbose:
                print(
                    f"Game ID: {game_id} | offset : {offset} | Num Results: {df_game_earnings.shape[0]}"
                )
    return df_game_earnings_all


def get_tournament_earnings(
    tournament_ids: list, teamplay: bool, verbose: bool = False,
) -> pd.DataFrame:
    if teamplay:
        url_earnings = base_url + tournament_team_results_by_tournament_id
    else:
        url_earnings = base_url + tournament_results_by_tournament_id
    df_tournament_earnings_all = pd.DataFrame()
    for tournament_id in tqdm(tournament_ids):
        params = {"apikey": API_TOKEN, "tournamentid": tournament_id}
        df_tournament_earnings = get_data(url_earnings, params)
        df_tournament_earnings["TournamentId"] = tournament_id
        df_tournament_earnings_all = df_tournament_earnings_all.append(
            df_tournament_earnings
        )
        time.sleep(wait_time_in_seconds)
        if verbose:
            print(df_tournament_earnings.TournamentId.values)
    return df_tournament_earnings_all


def main():
    df_tournaments = get_tournament_data(
        query_until_date=query_min_date, verbose=verbose
    )

    print("Getting Games Data")
    game_ids = df_tournaments.GameId.unique()
    df_games = get_games_data(game_ids, verbose=verbose)

    solo_tournaments = df_tournaments[df_tournaments["Teamplay"] == 0]
    solo_game_ids = solo_tournaments.GameId.unique()
    df_solo_earnings = get_game_earnings(solo_game_ids, teamplay=False, verbose=True)

    team_tournaments = df_tournaments[df_tournaments["Teamplay"] == 1]
    team_game_ids = team_tournaments.GameId.unique()
    df_team_earnings = get_game_earnings(team_game_ids, teamplay=True, verbose=True)

    solo_tournament_ids = solo_tournaments.TournamentId.unique()
    df_solo_tournament_earnings = get_tournament_earnings(
        solo_tournament_ids, teamplay=False
    )
    team_tournament_ids = team_tournaments.TournamentId.unique()
    df_team_tournament_earnings = get_tournament_earnings(
        team_tournament_ids, teamplay=True
    )

    # Final Postprocessing
    df_solo_earnings = df_solo_earnings.drop(["NameFirst", "NameLast"], axis=1)
    df_solo_tournament_earnings = df_solo_tournament_earnings.drop(
        ["NameFirst", "NameLast"], axis=1
    )

    # Save Data
    df_tournaments.to_csv("data/tournaments.csv", index=False)
    df_games.to_csv("data/games.csv", index=False)
    df_solo_earnings.to_csv("data/solo_earnings.csv", index=False)
    df_team_earnings.to_csv("data/team_earnings.csv", index=False)
    df_solo_tournament_earnings.to_csv("data/solo_tournament_earnings.csv", index=False)
    df_team_tournament_earnings.to_csv("data/team_tournament_earnings.csv", index=False)


if __name__ == "__main__":
    main()
