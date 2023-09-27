import os
import pandas as pd
import matplotlib.pyplot as plt
from env import user, password, host

def check_columns(df, reports=False, graphs=False):
    """
    This function takes a pandas dataframe as input and returns
    a dataframe with information about each column in the dataframe. For
    each column, it returns the column name, the number of
    unique values in the column, the unique values themselves,
    the number of null values in the column, the proportion of null values,
    the data type of the column, and the range of the column if it is float or int. The resulting dataframe is sorted by the
    'Number of Unique Values' column in ascending order.

    Args:
    - df: pandas dataframe

    Returns:
    - pandas dataframe
    """
    print(f"Total rows: {df.shape[0]}")
    print(f"Total columns: {df.shape[1]}")
    if reports == True:
        describe = df.describe().round(2)
        pd.DataFrame(describe)
        print(describe)
    if graphs == True:
        df.hist(bins=20, figsize=(10, 10))
        plt.show()
    data = []
    # Loop through each column in the dataframe
    for column in df.columns:
        # Append the column name, number of unique values, unique values, number of null values, proportion of null values, and data type to the data list
        if df[column].dtype in ["float64", "int64"]:
            data.append(
                [
                    column,
                    df[column].dtype,
                    df[column].nunique(),
                    df[column].isna().sum(),
                    df[column].isna().mean().round(5),
                    df[column].unique(),
                    df[column].describe()[["min", "max", "mean"]].values,
                ]
            )
        else:
            data.append(
                [
                    column,
                    df[column].dtype,
                    df[column].nunique(),
                    df[column].isna().sum(),
                    df[column].isna().mean().round(5),
                    df[column].unique(),
                    None,
                ]
            )
    # Create a pandas dataframe from the data list, with column names 'Column Name', 'Number of Unique Values', 'Unique Values', 'Number of Null Values', 'Proportion of Null Values', 'dtype', and 'Range' (if column is float or int)
    # Sort the resulting dataframe by the 'Number of Unique Values' column in ascending order
    return pd.DataFrame(
        data,
        columns=[
            "col_name",
            "dtype",
            "num_unique",
            "num_null",
            "pct_null",
            "unique_values",
            "range (min, max, mean)",
        ],
    )


def prep_store(store):
    """
    This function prepares the store data by removing timezone information, converting the sale_date column to a
    datetime index, and adding columns for month and day of the week.

    Parameters:
    store (pandas.DataFrame): The store data to be prepared.

    Returns:
    pandas.DataFrame: The prepared store data.
    """
    # Remove the timezone information
    store["sale_date"] = store["sale_date"].str.replace(" 00:00:00 GMT", "")
    store["sale_date"] = store["sale_date"].str.strip()

    # Convert column
    store["sale_date"] = pd.to_datetime(store["sale_date"])

    # Set index and sort by date
    store = store.set_index("sale_date").sort_index()

    # Add columns for month and day of the week
    store["month"] = store.index.month
    store["day_of_week"] = store.index.day_of_week

    # Get the total value of the sale
    store["sales_total"] = store["sale_amount"] * store["item_price"]

    return store


def prep_opsd(df):
    """
    This function takes in a dataframe and performs the following operations:
    - Makes all columns lowercase
    - Renames the 'wind+solar' column to 'wind_and_solar'
    - Converts the 'date' column to datetime format
    - Sets the 'date' column as the index and sorts the index
    - Adds a 'month' and 'year' column
    - Fills null values in 'wind' column where year < 2010 with 0
    - Fills null values in 'solar' column where year < 2012 with 0
    - Fills null values in 'wind_and_solar' column where year < 2012 with the wind value counts
    - Fills remaining null values with forward fill method

    It returns a cleaned dataframe.
    """
    # Make all column names lowercase
    df.columns = df.columns.str.lower()

    # Rename the 'wind+solar' column to 'wind_and_solar'
    df.rename(columns={"wind+solar": "wind_and_solar"}, inplace=True)

    # Convert the 'date' column to datetime format
    df["date"] = pd.to_datetime(df["date"])

    # Set the 'date' column as the index and sort the index
    df = df.set_index("date").sort_index()

    # Add a 'month' and 'year' column
    df["month"] = df.index.month
    df["year"] = df.index.year

    # Fill null values in 'wind' column where year < 2010 with 0
    df.loc[df["year"] < 2010, "wind"] = df.loc[df["year"] < 2010, "wind"].fillna(0)

    # Fill null values in 'solar' column where year < 2012 with 0
    df.loc[df["year"] < 2012, "solar"] = df.loc[df["year"] < 2012, "solar"].fillna(0)

    # Fill null values in 'wind_and_solar' column where year < 2012 with the wind value counts
    df.loc[df["year"] < 2012, "wind_and_solar"] = df.loc[
        df["year"] < 2012, "wind_and_solar"
    ].fillna(df["wind"].value_counts().index[0])

    # Fill remaining null values with forward fill method
    df = df.fillna(method="ffill")

    # Return the cleaned dataframe
    return df
