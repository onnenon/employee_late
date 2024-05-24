import pandas as pd


# Constants
RUNNING_TOTAL_FILE_PATH = "data/totals.csv"
IMCOMING_DATA_FILE_PATH = "data/incoming.csv"
EXCEPTION_TO_COUNT = "late"

def get_current_total_df():
    """
    Reads the running total file and returns a DataFrame.

    If the file is not found, an empty DataFrame with columns ["id", "name", "count"] is returned.

    Returns:
        pandas.DataFrame: The DataFrame containing the running total data.
    """
    try:
        return pd.read_csv(RUNNING_TOTAL_FILE_PATH)
    except FileNotFoundError as e:
        print(f"Running Total File not found: creating a new df")
        return pd.DataFrame(columns=["id", "name", "count"])

def sum_exceptions_for_id(current_total_df, incoming_df):
    """
    Sum the occurrences of a specific exception for each 'id' and 'name' in the incoming DataFrame,
    and merge the results with the current total DataFrame.

    Parameters:
        current_total_df (pandas.DataFrame): The current total DataFrame.
        incoming_df (pandas.DataFrame): The incoming DataFrame containing the exceptions to count.
        exception_to_count (str, optional): The exception to count occurrences for. Defaults to "late".

    Returns:
        pandas.DataFrame: The merged DataFrame with the summed counts of the exception for each 'id' and 'name'.
    """

    # Filter incoming_df for the exception we're interested in
    # Group by 'id' and 'name', count the number of occurrences of the exception, and reset the index
    # This will give us a DataFrame with columns 'id', 'name', and 'count'
    # containing the number of occurrences of the exception for each 'id' and 'name'
    summed_incoming_df = (
        incoming_df[incoming_df["exception"] == EXCEPTION_TO_COUNT]
        .groupby(["id", "name"])
        .size()
        .reset_index(name="count")
    )

    # Merge, fill NaNs, sum counts, drop unnecessary columns, and convert 'count' to int
    return (
        pd.merge(current_total_df, summed_incoming_df, on=["id", "name"], how="outer")
        .fillna(0)
        .assign(count=lambda df: df["count_x"] + df["count_y"])
        .drop(columns=["count_x", "count_y"])
        .astype({"count": "int64"})  # convert count from float to int
    )


def main():
    """
    Main function to read data, process it and save the result.
    """
    try:
        incoming_unfiltered_df = pd.read_csv(IMCOMING_DATA_FILE_PATH)
    except FileNotFoundError as e:
        print(f"Given file for incoming data not found: {e}")
        return
    
    running_total_df = get_current_total_df()

    merged_df = sum_exceptions_for_id(running_total_df, incoming_unfiltered_df)

    print(merged_df)

    try:
        merged_df.to_csv(RUNNING_TOTAL_FILE_PATH, index=False)
    except Exception as e:
        print(f"Error occurred while writing to running total file: {e}")


if __name__ == "__main__":
    main()
