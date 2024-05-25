## Case Study:
### Dataset:
Data Set folder has 6 csv files. Please use the data dictionary (attached in the mail) to understand the dataset and then develop your approach to perform below analytics.

### Analytics: 
Application should perform below analysis and store the results for each analysis.
* Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
* Analysis 2: How many two wheelers are booked for crashes? 
*  Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
* Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
* Analysis 5: Which state has highest number of accidents in which females are not involved? 
* Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
* Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
* Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
* Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
* Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)


### Expected Output:
1. Develop an application which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions, config driven, command line executable through spark-submit)
2. Code should be properly organized in folders as a project.
3. Input data sources and output should be config driven
4. Code should be strictly developed using Dataframe APIs (Do not use Spark SQL)
5. Share the entire project as zip or link to project in GitHub repo.


### Project Folder Structure

```
BCG-CaseStudy-CarCrashAnalysis/
├── config/
│   ├── config.yaml
├── data/
│   ├── Charges_use.csv
│   ├── Damages_use.csv
│   ├── Endorse_use.csv
│   ├── Primary_Person_use.csv
│   ├── Restrict_use.csv
│   └── Units_use.csv
├── output/
│   ├── analysis-1
│   ├── analysis-2
│   ├── analysis-3
│   ├── analysis-4
│   ├── analysis-5
│   ├── analysis-6
│   ├── analysis-7
│   ├── analysis-8
│   ├── analysis-9
│   └── analysis-10
├── src/
│   ├── __init__.py
│   ├── car_crash_analysis.py
│   └── utils.py
├── main.py
├── README.md
└── requirements.txt
```

### Steps to Run in Windows Machine:
1. Go to the Project Directory: `"{LocalDisc}:\{FolderPath}\CarCrashAnaysis"`
2. Open cmd or powershell and navigate to Project Folder using "cd" in cmd.
   ```commandline
   cd "{LocalDisc}:\{FolderPath}\CarCrashAnaysis"
3. Run the requirements.txt file
   ```commandline
   pip install -r requirements.txt
4. Spark Submit:
   ```commandline
    spark-submit --master local[4] main.py --config config/config.yaml
   
* Note : Tested in Windows OS (Windows 11), this can also work in Windows 10.
