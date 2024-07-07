from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import (regexp_replace, col, explode, split, desc,
                                   count, expr, row_number, lower)
from pyspark.sql.window import Window


class PetitionDataProcessor:
    def __init__(self, input_file: str = "data/input_data.json") -> None:
        self.spark_session = SparkSession.builder.master("local[*]").appName("PetitionDataProcessor").getOrCreate()

        # read the input data from the json file, and select the relevant column i.e., title
        self.input_data: DataFrame = (self.spark_session.read.format("json")
                                      .option("multiLine", "true")
                                      .load(f"data/{input_file}")
                                      .withColumn("title", col("label._value"))
                                      )

    def filter_top_hit_words(self, word_length: int) -> DataFrame:
        """
        Filter the words in the label/title column that are greater than or equal to the given word length.
        :param word_length: Word length to filter the word
        :return: DataFrame with columns: title, words, top_hit_words
        """
        return (
            self.input_data.withColumn("words", split(regexp_replace(lower(col("title")), "[^a-zA-Z\\s]", ""), "\\s+"))
            .withColumn("top_hit_words", expr(f"filter(words, x -> length(x) >= {word_length})"))
        )

    @staticmethod
    def top_hit_words_as_columns(words_df: DataFrame, max_limit: int = 20) -> DataFrame:
        """
        Create a DataFrame with top occurring words as columns and count of each word in the label/title column.
        :param words_df: Input DataFrame with columns: title, words, top_hit_words
        :param max_limit: Maximum number of top words to consider
        :return: Pivoted DataFrame with columns: petition_id, top_words
        """
        df_exploded = words_df.withColumn("word", explode("top_hit_words"))

        # Top occurring words
        top_words = (
            df_exploded.groupBy("word").count().orderBy(col("count").desc()).limit(max_limit)
            .select("word").rdd.flatMap(lambda x: x).collect()
        )

        # Filter for top words and count occurrences of each word in each title row
        df_word_count = (
            df_exploded.filter(col("word").isin(top_words))
            .groupBy("title", "word").agg(count("word").alias("ws_count"))
        )

        # Now, Pivot the DataFrame to have column for each word
        df_pivot = df_word_count.groupBy("title").pivot("word").sum("ws_count").fillna(0)

        # Finally, add the unique identifier i.e., petition_id
        df_final = df_pivot.withColumn("petition_id",
                                       row_number().over(Window.orderBy(desc('title'))))

        return df_final.select("petition_id", *top_words)

    @staticmethod
    def df_to_csv(out_df: DataFrame, output_file: str = "data/output_data.csv"):
        """
        Save the DataFrame to a CSV file.
        :param out_df: DataFrame to save
        :param output_file: Output file name
        :return: None
        """
        out_df.repartition(1).toPandas().to_csv(f"data/{output_file}", sep=',', header=True, index=False)
