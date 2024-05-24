import pandas as pd


# Constants
RUNNING_TOTAL_FILE_PATH = "data/totals.csv"
INCOMING_DATA_FILE_PATH = "data/incoming.csv"
EXCEPTION_TO_COUNT = "late"


def get_current_total_df():
    """
    Reads the running total file and returns a DataFrame.

    If the file is not found, an empty DataFrame with columns ["id", "name", "count"]
    is returned.

    Returns:
        pandas.DataFrame: The DataFrame containing the running total data.
    """
    try:
        return pd.read_csv(RUNNING_TOTAL_FILE_PATH)
    except FileNotFoundError:
        print("Running Total File not found: creating a new df")
        return pd.DataFrame(columns=["id", "name", "count"])


def merge_exception_counts_for_id(
    current_total_df: pd.DataFrame, summed_employee_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Merges the current total dataframe with the summed employee dataframe
    based on the 'id' and 'name' columns, performs an outer join, fills missing values
    with 0, sums the 'count_x' and 'count_y' columns into a new 'count' column, drops
    the 'count_x' and 'count_y' columns, and converts the 'count' column from
    float to int.

    Args:
        current_total_df (pandas.DataFrame): The current total dataframe.
        summed_employee_df (pandas.DataFrame): The summed employee dataframe.

    Returns:
        pandas.DataFrame: The merged and summed dataframe.

    """
    return (
        pd.merge(current_total_df, summed_employee_df, on=["id", "name"], how="outer")
        .fillna(0)
        .assign(count=lambda df: df["count_x"] + df["count_y"])
        .drop(columns=["count_x", "count_y"])
        .astype({"count": "int64"})  # convert count from float to int
    )


def sum_exceptions_for_id(
    incoming_df: pd.DataFrame, exception_name: str
) -> pd.DataFrame:
    """
    Calculate the total count of a specific exception for each unique id and
    name in the incoming DataFrame.

    Parameters:
    - incoming_df (DataFrame): The input DataFrame containing the data.
    - exception_name (str): The name of the exception to calculate the count for.

    Returns:
    - DataFrame: A DataFrame with columns 'id', 'name', and 'count', representing the
                 unique id, name, and count of the specified exception.
    """
    return (
        incoming_df[incoming_df["exception"] == exception_name]
        .groupby(["id", "name"])
        .size()
        .reset_index(name="count")
    )


def main():
    """
    Main function to read data, process it and save the result.
    """
    try:
        incoming_unfiltered_df = pd.read_csv(INCOMING_DATA_FILE_PATH)
    except FileNotFoundError as e:
        print(f"Given file for incoming data not found: {e}")
        return

    running_total_df = get_current_total_df()

    summed_diff = sum_exceptions_for_id(incoming_unfiltered_df, EXCEPTION_TO_COUNT)
    merged_df = merge_exception_counts_for_id(running_total_df, summed_diff)

    print(merged_df)

    try:
        merged_df.to_csv(RUNNING_TOTAL_FILE_PATH, index=False)
    except Exception as e:
        print(f"Error occurred while writing to running total file: {e}")


if __name__ == "__main__":
    main()
