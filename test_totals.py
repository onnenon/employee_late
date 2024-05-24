import pandas as pd
from pandas.testing import assert_frame_equal
import totals


def test_merge_exception_counts():
    # Create example data
    current_total_df = pd.DataFrame(
        {"id": [1, 2], "name": ["Mike", "John"], "count": [10, 20]}
    )
    incoming_df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Mike", "John", "Anna"],
            "count": [2, 1, 1],
        }
    )

    # Call the function with the example data
    result = totals.merge_exception_counts_by_id(current_total_df, incoming_df)

    # Define the expected result
    expected = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Mike", "John", "Anna"], "count": [12, 21, 1]}
    )

    # Check that the result matches the expected result
    assert_frame_equal(result, expected)


def test_sum_exceptions_by_id():
    # Create example data
    incoming_df = pd.DataFrame(
        {
            "id": [1, 2, 2, 3, 1],
            "name": ["Mike", "John", "John", "Anna", "Mike"],
            "exception": ["late", "late", "on time", "late", "late"],
        }
    )

    # Call the function with the example data
    result = totals.sum_exceptions_by_id(incoming_df, "late")

    # Define the expected result
    expected = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Mike", "John", "Anna"], "count": [2, 1, 1]}
    )

    # Check that the result matches the expected result
    assert_frame_equal(result, expected)
