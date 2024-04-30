# Data Engineering with PostgreSQL, Docker, and Apache Spark
Data engineering: processing, normalization, queries, pyspark containers.

## Project Description

This project aims to replicate the responsibilities of a data engineer by processing raw data from various sources, transforming it, and loading it into a database. I developed Python scripts to handle CSV files, ensuring idempotency and utilized Apache Spark within a Docker container. The dataset used was the Central Park Squirrel Census, which includes data about the squirrel population in Central Park. I normalized the data for efficient querying and delivered a populated database schema along with executable queries in the mentioned environment.

## Development Workflow

### Analysis and Schema Design
- Conducted a thorough analysis of the data sources (`park-data.csv` and `squirrel-data.csv`).
- Developed an Entity-Relationship model on draw.io to guide the structuring and best practices of the database schema, consisting of 18 tables such as:
  - `Activities`, `Animals`, `Areas`, `Colors`, `Interactions`, `Locations`, `ParkConditions`, `ParkLitters`, `Parks`, `ParkSections`, `ParkStatus`, `ParkStatusAnimals`, `ParkStatusWeather`, `SquirrelActivities`, `SquirrelHumanInteractions`, `SquirrelLocations`, `Squirrels`, `Weathers`.

### Docker Implementation and Data Ingestion
- Set up two Docker environments: one for development and one for production, choosing PostgreSQL as the database.
- Created a `source` directory to store the CSV data files.
- Handled data ingestion from `/source/park-data.csv` and `/source/squirrel-data.csv`.

### Data Staging and Processing
- Preprocessed the data, including cleaning, null field validation, removing strange characters, and deduplicating rows.
- Generated the relational model based on the designed diagram.
- Developed procedures for data updating to ensure idempotence.

### SQL Query and Output Configuration
- Generated the necessary SQL queries, which can be viewed directly in the Jupyter notebook within dataframes.
- Configured an `output` directory to automatically deposit the results of each query.

## PostgreSQL Database Access
- **Host:** postgres
- **User:** postgres
- **Password:** postgres
- **Database:** verusen

## Development Environment Execution
1. Download this git repository.
2. Navigate to the `development` folder.
3. Open the terminal and execute `docker-compose up -d`.
4. Access the database manager by navigating to http://localhost:8080/ in your browser to reach "Adminer".
5. To access PySpark from the terminal, execute `docker-compose logs jupyter` in the development folder. Look for the access link with the token in the terminal. Example link for orientation: http://127.0.0.1:8888/lab?token=8d52be46dd4c4da60aa838301403bdd44.
6. Once in the notebook, you can interactively analyze the code.

> **Note:** If the "main.ipynb" notebook is modified, convert it to a Python script using the "convert_script.ipynb" notebook by running the first cell, which will generate the "main.py" script for use in the production environment.

## Production Environment Execution
1. Download this git repository.
2. Navigate to the `development` folder.
3. Open the terminal and execute `docker-compose up -d`.
4. To view the execution logs of the notebook, access the terminal again and execute `docker-compose logs`. Once execution concludes, the PySpark container stops automatically while the PostgreSQL container remains running.
5. Access the database manager by navigating to http://localhost:8080/ in your browser to reach "Adminer".
