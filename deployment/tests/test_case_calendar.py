"""Real local Spark proof that calendars are declared by a case, not Treasury."""

import os
import unittest
from datetime import date

from test_gold_runtime import gold


@unittest.skipUnless(
    os.environ.get("MEDALLION_SPARK_TESTS") == "1", "Opt-in case-neutral Spark tier"
)
class CaseCalendarTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("case-calendar-contract")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_january_case_does_not_inherit_april_fiscal_year(self):
        dates = self.spark.createDataFrame([(date(2026, 1, 15),)], "event_date date")
        january = gold.calendar_frames(
            self.spark, dates, {"fiscal_year_start_month": 1}
        )
        april = gold.calendar_frames(self.spark, dates, {"fiscal_year_start_month": 4})
        left = january["dim_fiscal_period"].filter("fiscal_period_sk = 202601").first()
        right = april["dim_fiscal_period"].filter("fiscal_period_sk = 202601").first()
        self.assertEqual((left.fiscal_year_start, left.fiscal_month), (2026, 1))
        self.assertEqual((right.fiscal_year_start, right.fiscal_month), (2025, 10))
        self.assertEqual(left.calendar_basis, "DECLARED_FISCAL_CALENDAR")


if __name__ == "__main__":
    unittest.main()
