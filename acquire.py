import os
import math
import pandas as pd
import requests
from env import user, password, host

def get_swapi_api(endpoint):
    # If endpoint csv exists, use it
    if os.path.isfile(f"{endpoint}.csv"):
        return pd.read_csv(f"{endpoint}.csv")
    else:
        # Find how many pages the api has
        response = requests.get(f"https://swapi.dev/api/{endpoint}/")
        data = response.json()
        # Assume data is a dictionary containing the results and count
        num_results = len(data["results"])
        num_pages = math.ceil(data["count"] / num_results)
        # Create an empty dataframe
        endpoint_df = pd.DataFrame()
        # Create a loop to get all pages
        for i in range(1, (num_pages + 1)):
            # Get the data from the API
            response = requests.get(f"https://swapi.dev/api/{endpoint}/?page=" + str(i))
            # Convert the data to json
            data = response.json()
            # Convert the json to a dataframe
            df = pd.DataFrame(data["results"])
            # Concat the dataframes
            endpoint_df = pd.concat([endpoint_df, df])
            # Reset the index of the df
            endpoint_df = endpoint_df.reset_index(drop=True)
        return endpoint_df

def get_star_wars_data(people, planets, starships):
    # Add prefix to planets and starships dataframes
    planets = planets.add_prefix("planet_")
    starships = starships.add_prefix("starship_")

    # Remove brackets and quotes from people.starships column and split into a list
    people["starships"] = people["starships"].str.replace("[", "")
    people["starships"] = people["starships"].str.replace("]", "")
    people["starships"] = people["starships"].str.replace("'", "")
    people["starships"] = people["starships"].str.split(",")

    # Explode people.starships column into rows
    people = people.explode("starships")

    # Join people dataframe with planets dataframe on homeworld and planet_url columns
    df = people.join(planets.set_index("planet_url"), on="homeworld")

    # Join resulting dataframe with starships dataframe on starships and starship_url columns
    df = df.join(starships.set_index("starship_url"), on="starships")

    # Select subset of columns from resulting dataframe
    df = df[
        [
            "name",
            "height",
            "mass",
            "hair_color",
            "skin_color",
            "eye_color",
            "birth_year",
            "gender",
            "planet_name",
            "planet_climate",
            "planet_gravity",
            "planet_terrain",
            "planet_surface_water",
            "starship_name",
            "starship_model",
            "starship_manufacturer",
            "starship_length",
            "starship_crew",
            "starship_passengers",
            "starship_cargo_capacity",
            "starship_consumables",
            "starship_hyperdrive_rating",
            "starship_MGLT",
            "starship_starship_class",
        ]
    ]

    return df

def read_csvs(csv_list):
    # Create an empty dataframe
    df = pd.DataFrame()
    # Create a loop to read in each csv
    for csv in csv_list:
        # Read in the csv
        df_csv = pd.read_csv(csv)
        # Concatenate the csv to the dataframe
        df = pd.concat([df, df_csv])
    # Reset the index
    df = df.reset_index(drop=True)
    return df

def concat_dfs(df_list, axis=0):
    df = pd.concat(df_list, axis=axis)
    return df

def get_opsd():
    opsd = pd.read_csv(
        "https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv"
    )
    return opsd

def get_sql(
    user=user,
    password=password,
    host=host,
    filename="store.csv",
    database="tsa_item_demand",
):
    """
    This function acquires data from a SQL database and caches it locally, or uses the already cached file.

    :param user: The username for accessing the MySQL database
    :param password: The password is unique per user saved in env
    :param host: The host parameter is the address of the server where the database is hosted
    :return: The function `get_sql` is returning a pandas DataFrame
    """
    # if cached data exists
    if os.path.isfile(filename):
        # read data from cached csv
        df = pd.read_csv(filename)
        # Print size
        print(f"Total rows: {df.shape[0]}")
        print(f"Total columns: {df.shape[1]}")
    # wrangle from sql db if not cached
    else:
        # read sql query into df
        df = pd.read_sql(
            """
                SELECT * FROM sales
                JOIN items ON sales.item_id = items.item_id
                JOIN stores ON sales.store_id = stores.store_id;
                """,
            f"mysql+pymysql://{user}:{password}@{host}/{database}",
        )
        # cache data locally
        df.to_csv(filename, index=False)
        # print total rows and columns
        print(f"Total rows: {df.shape[0]}")
        print(f"Total columns: {df.shape[1]}")
    return df