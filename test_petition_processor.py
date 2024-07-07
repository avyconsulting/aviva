import os

from pyspark.sql import DataFrame

from interview.aviva.petition_processor import PetitionDataProcessor


def test_create_word_as_columns():
    petition_processor = PetitionDataProcessor(input_file="test_input_data.json")

    df: DataFrame = petition_processor.filter_top_hit_words(word_length=5)
    df = petition_processor.top_hit_words_as_columns(words_df=df, max_limit=20)

    assert df.count() == 2
    assert len(df.columns) == 5
    assert sorted(df.columns) == ['gover', 'government', 'heavymetal', 'ignore', 'petition_id']

    # Check the values of the first row
    assert df.collect()[0].petition_id == 1
    assert df.collect()[0].government == 4
    assert df.collect()[0].heavymetal == 0
    assert df.collect()[0].ignore == 1
    assert df.collect()[0].gover == 0

    # Check the values of the second row
    assert df.collect()[1].petition_id == 2
    assert df.collect()[1].government == 2
    assert df.collect()[1].heavymetal == 2
    assert df.collect()[1].ignore == 0
    assert df.collect()[1].gover == 1


def test_df_to_csv():
    try:
        petition_processor = PetitionDataProcessor(input_file="test_input_data.json")

        df: DataFrame = petition_processor.filter_top_hit_words(word_length=5)
        df = petition_processor.top_hit_words_as_columns(words_df=df)
        petition_processor.df_to_csv(df, "test_output_data.csv")

        # Check if the file was created
        assert os.path.exists("data/test_output_data.csv")
    finally:
        os.remove("data/test_output_data.csv")
