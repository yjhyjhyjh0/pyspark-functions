import shutil
import tempfile
from contextlib import contextmanager

from pyspark.sql import SparkSession
from core.simple_compute import simple_compute


def setup_module(module):
    """
    Will execute before this py file execute
    """
    print()


def teardown_module(module):
    """
    Will execute after this py file finish
    """


class TestSimpleComput(object):
    spark: SparkSession

    def create_fake_df1(self, spark):
        df1 = spark.createDataFrame([(1, "value1")], ["id", "col1"])
        return df1

    def create_fake_df2(self, spark):
        df2 = spark.createDataFrame([(1, "value2")], ["id", "col2"])
        return df2

    def test_run(self):
        df1 = self.create_fake_df1(self.spark)
        df2 = self.create_fake_df2(self.spark)
        input = simple_compute.simple_compute_input(df1, df2)
        df_output = simple_compute.run(input, False)
        print("hello")
        assert df_output.count() == 1

        aa=df_output.select("id").collect()[0]
        print(type(aa))
        print(aa)
        assert df_output.select("id").collect()[0]['id'] == 1
        print("hello2")

    # @contextmanager
    # def temp_dir(self):
    #     tmp =tempfile.mktemp()
    #     try:
    #         yield tmp
    #     finally:
    #         shutil.rmtree(tmp)

    @classmethod
    def setup_class(cls):
        """
        Will execute this method before every test
        """
        cls.spark = SparkSession \
            .builder \
            .master("local") \
            .getOrCreate()

    # @classmethod
    # def teardown_class(cls):
    #     """
    #     Will execute this method after every test
    #     """
