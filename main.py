from pyspark.sql import DataFrame

from interview.aviva.petition_processor import PetitionDataProcessor

if __name__ == '__main__':
    _input_file = "input_data.json"
    _output_file = "output_data.csv"

    petition_processor: PetitionDataProcessor = PetitionDataProcessor(input_file=_input_file)

    words_df: DataFrame = petition_processor.filter_top_hit_words(word_length=5)
    final_df = petition_processor.top_hit_words_as_columns(words_df=words_df, max_limit=20)

    petition_processor.df_to_csv(final_df, "output_data.csv")
