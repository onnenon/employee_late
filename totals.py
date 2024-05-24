import pandas as pd


def sum_exceptions_for_id(current_total_df, incoming_df, exception_to_count="late"):
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
        incoming_df[incoming_df["exception"] == exception_to_count]
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
        .astype({"count": 'int64'}) # convert count from float to int
    )


def main():
    RUNNING_TOTAL_FILE_PATH = "data/totals.csv"
    IMCOMING_DATA_FILE_PATH = "data/incoming.csv"
    EXCEPTION_TO_COUNT = "late"

    running_total_df = pd.read_csv(RUNNING_TOTAL_FILE_PATH)
    incoming_unfiltered_df = pd.read_csv(IMCOMING_DATA_FILE_PATH)
    merged_df = sum_exceptions_for_id(running_total_df, incoming_unfiltered_df, EXCEPTION_TO_COUNT)

    print(merged_df)

    merged_df.to_csv(RUNNING_TOTAL_FILE_PATH, index=False)


if __name__ == "__main__":
    main()