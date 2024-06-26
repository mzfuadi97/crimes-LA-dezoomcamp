# Data Engineering | Zoomcamp Course Project
![Screenshot 2024-04-15 080947](https://github.com/mzfuadi97/crimes-LA-dezoomcamp/assets/70827786/beb4e7f2-3f60-471e-9fdc-fa2f46aeb04b)

### Overview

The Los Angeles Police Department (LAPD) is transitioning to a new Records Management System (RMS) to comply with the FBI's National Incident-Based Reporting System (NIBRS) mandate. This transition aims to improve reporting efficiency and accuracy. However, challenges such as data inaccuracies and technical issues have arisen, impacting data updates and reliability.\

you can access the dataset : [https://catalog.data.gov/dataset/crime-data-from-2020-to-present]

### Problem Statement:
The LAPD's adoption of a new RMS faces several challenges:

- Data Transition: Migrating historical crime data to the new system may introduce inaccuracies, affecting dataset reliability.
- Technical Issues: The LAPD is experiencing delays in posting crime data due to technical issues, hampering timely updates.
- Data Accuracy: Inherent inaccuracies in transcribing paper-based crime reports may compromise data reliability.
- Public Trust: Timely resolution of technical issues and ensuring data accuracy are vital for maintaining public trust in the LAPD's transparency and accountability.

Addressing these challenges requires resolving technical issues, enhancing data accuracy, and prioritizing transparency to bolster public trust.

There will be two running pipelines (DAG):
- **Daily_DAG**: this DAG will run daily to extract new data starting from the installation time.
- **Historical_DAG**: this DAG will run once to extract the historical crime data (2020, 2021, 2022, 2023 till today).

The dashboard will have three parts with control filters on time and area that demonstrate the analytics points below:
* **Historical data analytics:**
    * Crimes trending with times
    * Crimes counts per area
    * Comparison victims by gender
* **Spatial data analytics:**
    * Area map with crimes geolocation
    * Heat area map that shows the crimes  (intense)
* **Last 24 hours analytics:**
    * Crimes trending with times
    * Crimes counts per city

To accelerate queries and data processing, the final table "full_data" has been partitioned by date of crimes (column 'time') as this column is one of the filter control in the dashboard also one of the dashboard's sections considers taking the latest date partition only (where the date is equal today) and the table is clustered by geodata (column 'area') which is a filter control in the dashboard too.
The original column 'time' type is transformed from string to date type in order to be able to partition by time in spark transformation steps.




### Data schema

| Kolom          | Tipe           |
|----------------|----------------|
| dr_no          | StringType     |
| date_rptd      | TimestampType       |
| date_occ       | TimestampType       |
| time_occ       | IntegerType     |
| area           | StringType     |
| area_name      | StringType     |
| rpt_dist_no    | StringType     |
| part_1_2      | StringType     |
| crm_cd         | IntegerType    |
| crm_cd_desc   | StringType     |
| mocodes        | StringType     |
| vict_age      | IntegerType    |
| vict_sex       | StringType     |
| vict_descent   | StringType     |
| premis_cd     | StringType     |
| premis_desc   | StringType     |
| weapon_used_cd | StringType     |
| weapon_desc    | StringType     |
| status	         | StringType     |
| status_desc   | StringType     |
| crm_cd_1       | IntegerType    |
| crm_cd_2      | IntegerType    |
| crm_cd_3       | IntegerType    |
| crm_cd_4       | IntegerType    |
| location       | StringType     |
| cross_street   | StringType     |
| lat            | FloatType      |
| lon            | FloatType      |


[reference](https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/2nrs-mtv8/about_data)

## Data Pipeline 

* **Full pipeline**
   ![Infrasructure Arch](https://github.com/mzfuadi97/crimes-LA-dezoomcamp/assets/70827786/5c5e7d42-31a7-4c1e-87f9-b1b13b3b984d)

* **Daily_DAG**
   ![image](https://user-images.githubusercontent.com/98602171/235377455-f82b774d-c4fe-425a-b813-aa3c6b18f697.png)

* **Historical_DAG**
   ![image](https://user-images.githubusercontent.com/98602171/235377439-be686e2c-1d4e-478c-a55d-887c6821bb57.png)


## Technologies and Tools

- Cloud - [**Google Cloud Platform**](https://cloud.google.com)
- Infrastructure as Code software (IaC) - [**Terraform**](https://www.terraform.io)
-  Containerization - [**Docker**](https://www.docker.com), [**Docker Compose**](https://docs.docker.com/compose/)
- Workflow Orchestration - [**Airflow**](https://airflow.apache.org)
- Batch processing - [**Apache Spark**](https://spark.apache.org/), [**PySpark**](https://spark.apache.org/docs/latest/api/python/)
- Data Lake - [**Google Cloud Storage**](https://cloud.google.com/storage)
- Data Warehouse - [**BigQuery**](https://cloud.google.com/bigquery)
- Data Visualization - [**Looker Studio (Google Data Studio)**](https://lookerstudio.google.com/overview?)
- Language - [**Python**](https://www.python.org)



## Analytics Dashboard

The dashboard will have three parts with control filters on time and area that demonstrate the analytics points below:
* Historical data analytics:
    * Crimes trending with times
    * Crimes counts per area
    * Comparison victims by gender
    ![Screenshot 2024-04-15 101427](https://github.com/mzfuadi97/crimes-LA-dezoomcamp/assets/70827786/927ed6ea-8014-4096-badc-6cf19d7a72c4)


* Spatial data analytics:
    * Area map with crimes geolocation
    * Heat area map that shows the crimes  (intense)
   

* Last 24 hours analytics:
    * Crimes trending with times
    * Crimes counts per city


You can check the live dashboard [**here**](https://lookerstudio.google.com/reporting/929c6e49-b59c-4f30-9bad-f551eba99cbb). (the last 24 hours part of dashboard may not have data if the pipeline is not running live so please choose and filter on one date from historical)

## Setup
1. Setup your google cloud project and service account [step1](setup/gcp_account.md)
2. install terraform on your local machine [step2](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp)
3. Setup terraform to create pipeline required infrastructure [step3](setup/terraform_vm.md)
4. SSH to your google compute engine VM [step4](setup/gcp_vm.md)
5. Clone the repo to your google compute engine VM
    ```bash
    git clone https://github.com/mzfuadi97/data-engineering-zoomcamp-project.git
    ```
6. Setup Anaconda + docker  + docker-compose
     ```bash
    cd data-engineering-zoomcamp-project
    bash scripts/vm_setup.sh
    ```
7. Update the enviroment variables in below file with your specific project_id and buckets
    ```bash
    cat data-engineering-zoomcamp-project/scripts/setup_config.sh
    ```
8. Setup pipeline docker image (airflow+spark)
     ```bash
    cd data-engineering-zoomcamp-project
    bash scripts/airflow_startup.sh
    ```
9. in Visual Studio code click on ports and forward port 8080<br>
  ![ForwardPort](https://user-images.githubusercontent.com/7443591/160403735-7c40babc-7d63-4b51-90da-c065e5b254a0.png)

go to localhost:8080<br>
  
and login with (airflow:airflow) for the credentials<br>
![AirflowLogin](https://user-images.githubusercontent.com/7443591/160413081-4f4e606f-09f6-4d4f-9b94-5241f37091a6.png)

9. Enable the historical_DAG and you should see it run. It takes 10-15 minutres to finish
10. Enable the daily_DAG
11. You can check your data in bigquery tables.
12. if you want to stop docker image you can run below command
    ```bash
    cd data-engineering-zoomcamp-project
    bash scripts/airflow_stop.sh
    ```
    or to delete and clean all docker image related file
    ```bash
    cd data-engineering-zoomcamp-project
    bash scripts/airflow_clear.sh
    ```
## Reference
[DataTalks Club](https://datatalks.club/)<br>
[Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp#week-1-introduction--prerequisites)<br>
[AliaHa3's setup steps](https://github.com/AliaHa3/shoemaker-de-zoomcamp-final-project/blob/main/GitLikeMe.md)<br>
