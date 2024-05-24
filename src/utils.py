import yaml


def read_yaml(file_path):
    """
    Reads a YAML configuration file and returns the content as a dictionary.

    :param file_path: Path to the YAML configuration file.
    :return: Dictionary containing the configuration details.
    :raises FileNotFoundError: If the file does not exist.
    :raises yaml.YAMLError: If there is an error parsing the YAML file.
    """
    try:
        with open(file_path, "r") as f:
            config = yaml.safe_load(f)
            return config
    except FileNotFoundError as e:
        print(f"Error: The file {file_path} was not found.")
        raise e
    except yaml.YAMLError as e:
        print(f"Error: Failed to parse the YAML file {file_path}.")
        raise e


def load_csv_data_to_df(spark, file_path):
    """
    Reads CSV data into a DataFrame using the provided Spark session.

    :param spark: SparkSession instance.
    :param file_path: Path to the CSV file.
    :return: DataFrame containing the CSV data.
    """
    df = (
        spark.read
        .format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(file_path)
    )
    return df


def write_output(df, file_path, write_format):
    """
    Write a DataFrame to the specified file path in the given format.

    :param df: DataFrame to be written.
    :param file_path: Output file path.
    :param write_format: Write file format (e.g., "csv", "parquet", "json").
    :return: None
    """

    df.coalesce(1).write.format(write_format).mode("overwrite").option("header", "true").save(file_path)
    # print(f"Data successfully written to {file_path} in {write_format} format.")
