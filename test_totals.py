import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
import totals


class TestTotals(unittest.TestCase):
    def test_sum_exceptions_for_id(self):
        # Create example data
        current_total_df = pd.DataFrame(
            {"id": [1, 2], "name": ["Mike", "John"], "count": [10, 20]}
        )
        incoming_df = pd.DataFrame(
            {
                "id": [1, 2, 2, 3],
                "name": ["Mike", "John", "John", "Anna"],
                "exception": ["late", "late", "on time", "late"],
            }
        )

        # Call the function with the example data
        result = totals.sum_exceptions_for_id(current_total_df, incoming_df, "late")

        # Define the expected result
        expected = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Mike", "John", "Anna"], "count": [11, 21, 1]}
        )

        # Check that the result matches the expected result
        assert_frame_equal(result, expected)


if __name__ == "__main__":
    unittest.main()
