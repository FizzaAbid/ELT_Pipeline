### Dependenices ########
--Download the following libraries using command line--
pip install requests
pip install pandas
brew install postgres
pip install psycopg2
pip install postgres
pip install sqlalchemy
--Download Docker Desktop and login into it--
--Download any editor for running the code, such as VS code intelliJ.


### Code Structure ##
1. Extract.py has two functions, which extract data from the API using request library.
2. Load.py has three functions for loading data into Postgres sql
3. Transform.py perform transformations on data, such as converting data into columns. It also has query functions.


##### Commands For running Docker File ######
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
- Normalization can be done to reduce redundancy
- Separate tables for categories, locations, links, etc. can be created with one mutual column. 
