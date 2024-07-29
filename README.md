In this project I  have done the data processing of the ipl json data into the meaning structured data format.
Basically I wanted to create a dashboards on the ipl data to do that I decided to first the data processing .

Tools I have used :
* Snowflake 
* SnowSQL
* JSON Visio
* VS Code
* DBeaver

Download the json files form the [here](https://cricsheet.org/downloads/ipl_male_json.zip) ;

Keep the seperate folder for storing the all json files with folder name ipl on desktop(or your convinent location)

Open the SnowSQL in the local machine 

Connect to the snowflake account with usermane and account name;


    snowsql -a vwbssau-ud04226 -u manjunath58

Now Enter the password of the snowflake account
--Enter the Password

--Connect to the warehouse named ipl_warehouse;

    use warehouse ipl_warehouse;

--if you haven't created the warehouse create it.
--Command to create warehouse named ipl_warehouse;

    create warehouse if not exists ipl_warehouse
    warehouse_size = xsmall
    warehouse_type = standard
    auto_suspend = 60
    auto_resume =true
    initially_suspended = true;


--Now connect to the database

    use database ipl_database;

--if you haven't created the database create it.
--Command to create database named ipl_database;

    create database if not exists ipl_database;


    use schema land;

--if you haven't created the schema create it.
--Command to create schema named land;

    create or replace schema land;

-- if you haven't created the stage in the land schema create it using the following command;

    create or replace stage land.my_stg;

-- Now run the following command by replacing the file path with the path you downloaded the json files --

    PUT file:///Users/Desktop/ipl/*.json @my_stg AUTO_COMPRESS=TRUE OVERWRITE=TRUE PARALLEL=8;


Now open the snowflake web and follow the steps mentioned in the second file 2.ipl_data_processing.sql;

Finally after completion of everything in the snowflake the ER diagram with fact and dimension tables looks as follow:

![1](https://github.com/user-attachments/assets/5748fdea-b7eb-4c17-8e3a-e4bef7b1ef9c)

After completion of all the steps in 2nd file now we need to move all the files into the new stage in consumption.
To do this just follow the steps mentioned in the third file .

Finally we need to move the staged files into the local machine to do that we need to open the SnowSQL and follow the steps metioned in the 4th file.

In this we way we will get the structured csv files with fact and dimension differentiation. 

This files can be used to create the dashboards in different visualisation tools like Tableau , PowerBI etc.,

