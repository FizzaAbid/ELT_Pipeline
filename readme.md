### Steps for installing Dependenices ########
1. Switch to virtual environment using source env/bin/activate or for initiating it for the first time enter "python -m venv env"
2. Download the following libraries in virtual environment if needed; however, the attached env folder has setup with all dependencies so just enter "python -m venv env"
3. Following commands have been used for downloading all the dependencies:
- pip install requests
- pip install pandas
- brew install postgres
- pip install psycopg2
- pip install sqlalchemy
- pip install postgres
4. Download Docker Desktop and login into it using any personal id
5. Open the code in editor like VS code and run the following commands:
- cd Docker
- docker compose up -d
This will start the image, check in docker whether the image is running or not.
6. Go to local browser and enter localhost:8080
7. In system, choose "Postgres", set server= database, username and password is "docker". Enter database name as "postgres"
8. Press login
9. Run file using command "python ELT.py"

<img width="989" alt="image" src="https://user-images.githubusercontent.com/31180223/175987008-88ffdb3b-cc04-4c03-9432-25df8e2b6c62.png">
<img width="963" alt="image" src="https://user-images.githubusercontent.com/31180223/175987093-e16b073e-0133-4bb5-86aa-459af7d24095.png">
<img width="1189" alt="image" src="https://user-images.githubusercontent.com/31180223/175987225-13d57d06-26f9-4042-905e-f357703f3dc8.png">
<img width="1266" alt="image" src="https://user-images.githubusercontent.com/31180223/175987439-a68ddb96-5c7a-44ae-b5dd-a1ea7e3598ac.png">
<img width="632" alt="image" src="https://user-images.githubusercontent.com/31180223/175987534-810e9d00-a905-49d4-94e7-aff2bd6a2b39.png">



### Code Structure ##
1. Extract.py has two functions, which extract data from the API using request library.
2. Load.py has two functions for loading data into Postgres sql
3. Transform.py perform transformations on data, such as converting json data into columns. 
4. Queries.py run queries on the data present in the database.


##### Commands For running DockerFile ######
cd Docker (Go into docker folder)
run command docker compose up -d
Docker will start Postgres and adminier container
Access the database UI from localhost:8080

### Flow of the Code ###
1. Data will be extracted from 2 APIs Laureates and Nobel Prize API
2. Data is initially loaded into staging table in json format
3. Afterwards, data is read into panda dataframe and transformations have been performed; for instance, separating
multi-valued attributes to normalize the table. Data types have been changed. Some columns have been renamed.
4. Json has been normalized.
5. Metadata has been discarded. 
6. Queries have been executed for selecting specific columns

##### Future Recommendation###
- Normalization can be done to reduce redundancy in depth.
- Same code can be executed on pyspark
- Separate tables for categories, locations, links, etc. can be created with one mutual column id
