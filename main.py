from src.utils import read_yaml
from src.car_crash_analysis import CarCrashAnalysis

import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def main_analysis(car_crash_analysis_object: CarCrashAnalysis, output_file_paths: str, file_format: str) -> None:

    print("Car Crash Analysis - Results\n")
    # 1. Find the number of crashes (accidents) in which number of males killed are greater than 2?
    print(
        "1. ",
        car_crash_analysis_object.count_male_accidents(
          output_file_paths.get(1), file_format.get("write_format")
        )
    )

    # 2. How many two-wheelers are booked for crashes?
    print(
        "2. ",
        car_crash_analysis_object.count_2_wheeler_accidents(
          output_file_paths.get(2), file_format.get("write_format")
        )
    )

    # 3. Determine the Top 5 Vehicles made of the cars present in the crashes in which a driver died and Airbags did
    # not deploy.
    print(
        "3. ",
        car_crash_analysis_object.top_5_vehicle_makes_for_fatal_crashes_without_airbags(
          output_file_paths.get(3), file_format.get("write_format")
        )
    )

    # 4. Determine the number of Vehicles with a driver having valid licences involved in hit-and-run?
    print(
        "4. ",
        car_crash_analysis_object.count_hit_and_run_with_valid_licenses(
          output_file_paths.get(4), file_format.get("write_format")
        )
    )

    # 5. Which state has the highest number of accidents in which females are not involved?
    print(
        "5. ",
        car_crash_analysis_object.get_state_with_no_female_accident(
          output_file_paths.get(5), file_format.get("write_format")
        )
    )

    # 6. Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print(
        "6. ",
        car_crash_analysis_object.get_top_vehicle_contributing_to_injuries(
            output_file_paths.get(6), file_format.get("write_format")
        )
    )

    # 7. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    print("7. ")
    car_crash_analysis_object.get_top_ethnic_ug_crash_for_each_body_style(
        output_file_paths.get(7), file_format.get("write_format")
    ).show(truncate=False)

    # 8. Among the crashed cars, what are the Top 5 Zip Codes with the highest number of crashes with alcohol as the
    # contributing factor to a crash (Use Driver Zip Code)
    print(
        "8. ",
        car_crash_analysis_object.get_top_5_zip_codes_with_alcohols_as_cf_for_crash(
            output_file_paths.get(8), file_format.get("write_format")
        )
    )

    # 9. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above
    # 4 and car avails Insurance
    print(
        "9. ",
        car_crash_analysis_object.get_crash_ids_with_no_damage(
            output_file_paths.get(9), file_format.get("write_format")
        )
    )

    # 10. Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed
    # Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
    # offenses (to be deduced from the data)
    print(
        "10. ",
        car_crash_analysis_object.get_top_5_vehicle_brand(
            output_file_paths.get(10), file_format.get("write_format")
        )
    )


if __name__ == '__main__':

    # Initialize spark session
    spark = SparkSession.builder.appName("CarCrashAnalysis").getOrCreate()

    # Set log level to ERROR to suppress warnings
    spark.sparkContext.setLogLevel("ERROR")

    # Read the configurations from yaml file
    config_file_name = "config/config.yaml"

    config = read_yaml(config_file_name)
    output_file_path = config.get("OUTPUT_PATH")
    output_file_format = config.get("FILE_FORMAT")

    car_crash_analysis = CarCrashAnalysis(spark, config)

    main_analysis(car_crash_analysis, output_file_path, output_file_format)

    spark.stop()
