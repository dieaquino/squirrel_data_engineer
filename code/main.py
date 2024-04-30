#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Importing modules from PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, current_timestamp, date_format, regexp_replace, col, split, lower, to_date, substring_index, substring
from pyspark.sql.functions import initcap, regexp_extract, expr, when, row_number, count, explode, trim, length, lit, array_min, array_max, array_remove, to_timestamp, concat_ws
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Park Data Normalization") \
    .config("spark.driver.extraClassPath", "jdbc/postgresql-42.2.23.jar") \
    .getOrCreate()



# # Functions

# In[2]:


def update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, df_table):
    try:
        # Check if the table exists
        existing_tables = spark.read.jdbc(url=jdbc_url, table="information_schema.tables", properties=connection_properties)
    
        if existing_tables.filter((existing_tables["table_name"] == table_name) & (existing_tables["table_schema"] == "public")).count() == 0:
            raise AnalysisException("Table '{}' does not exist in the database.".format(table_name))
    
        # Load the table into a Spark DataFrame
        table_db = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
        
        # Create a temporary view for the database table
        table_db.createOrReplaceTempView("View_table_db")

        # Get the maximum value of ParkConditionID from View_ParkConditions_db
        max_table_cond_id = spark.sql(max_cond_sql).collect()[0]["max_table_cond_id"]
        
        # Format the common_rows query with the obtained maximum ID
        formatted_common_rows_sql = common_rows_sql.format(max_table_cond_id=max_table_cond_id)
        
        # Execute the SQL query to obtain common records
        common_rows = spark.sql(formatted_common_rows_sql)

        # Assuming you have a DataFrame called df
        rows = common_rows.collect()
        
        # Get the names of the original columns
        column_names = common_rows.columns
        
        # Create a list of tuples with original column names
        list_of_tuples = [tuple(row[column_names.index(name)] for name in column_names) for row in rows]
        
        # Create DataFrame
        df_table = spark.createDataFrame(list_of_tuples, column_names)
        # Sort DataFrame by the first column
        df_table = df_table.orderBy(column_names[0])
        
        # Write the DataFrame into the PostgreSQL table
        df_table.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

    except AnalysisException as e:
        # Print the error message and exception type
        # print(f"An error occurred: {str(e)}")
        # print(f"Exception type: {type(e).__name__}")
        
        # Write the DataFrame into the PostgreSQL table
        df_table.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)
        
        print("DataFrame successfully exported to table '{}' in the PostgreSQL database.".format(table_name))
    
    # Return the DataFrame
    df_table.show()
    return df_table


# # Database Conection (PostgreSQL)

# In[3]:


# JDBC connection configuration
jdbc_url = "jdbc:postgresql://postgres:5432/verusen"
connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

try:
    # Conection Test
    df = spark.read.jdbc(url=jdbc_url, table="(SELECT CURRENT_TIMESTAMP AS current_time) AS temp", properties=connection_properties)
    df.show()
    
except Exception as e:
    # If there's any error while connecting or executing the query, display an error message.
    print("Error al conectar a PostgreSQL o al ejecutar la consulta:", e)


# # Load park-data
# 

# In[4]:


# Load data from the CSV file
df_park = spark.read.csv("source/park-data.csv", header=True, inferSchema=True, encoding="UTF-8")

# Rename columns by removing whitespace
for col_name in df_park.columns:
    new_col_name = col_name.replace(" ", "")
    df_park = df_park.withColumnRenamed(col_name, new_col_name)

def clean_column_names(df):
    for col_name in df.columns:
        # Remove parentheses and commas from column names
        new_col_name = col_name.replace("(", "").replace(")", "").replace(",", "")
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

# Apply the function to clean column names
df_park = clean_column_names(df_park)

# Show the result
df_park.show(truncate=False)


# ### Preprocessing park-data

# In[5]:


############
# AreaName #
############
# Clean the "AreaName" column by replacing special characters and trimming whitespace
df_park = df_park.withColumn(
    "AreaName",
    trim(regexp_replace(col("AreaName"), "[^a-zA-Z0-9\s#(),:]", ""))
)

# Replace empty and NULL values with "ND"
df_park = df_park.withColumn(
    "AreaName",
    when((col("AreaName") == "") | (col("AreaName").isNull()), "ND").otherwise(col("AreaName"))
)


############
#  AreaID  #
############
# Clean the "AreaID" column by replacing special characters and trimming whitespace
df_park = df_park.withColumn(
    "AreaID",
    trim(regexp_replace(col("AreaID"), "[^a-zA-Z0-9\s#(),:]", ""))
)

# Replace empty and NULL values with "ND"
df_park = df_park.withColumn(
    "AreaID",
    when((col("AreaID") == "") | (col("AreaID").isNull()), "ND").otherwise(col("AreaID"))
)


############
# ParkName #
############
# Clean the "ParkName" column by replacing special characters and trimming whitespace
df_park = df_park.withColumn(
    "ParkName",
    trim(regexp_replace(col("ParkName"), "[^a-zA-Z0-9\s#(),:]", ""))
)

# Replace empty and NULL values with "ND"
df_park = df_park.withColumn(
    "ParkName",
    when((col("ParkName") == "") | (col("ParkName").isNull()), "ND").otherwise(col("ParkName"))
)


########
# Date # 
########
# Assuming the column containing dates is named "date_column"
df_park = df_park.withColumn("Date", date_format(to_date("Date", "d/M/yy"), "dd/MM/yyyy"))

########
# Time # 
########
# Format time columns
df_park = df_park.withColumn("StartTime", date_format(to_timestamp("StartTime", "h:mm:ss a"), "HH:mm:ss"))
df_park = df_park.withColumn("EndTime", date_format(to_timestamp("EndTime", "h:mm:ss a"), "HH:mm:ss"))


#################################
# TotalTimeinminutesifavailable #
#################################

# Clean the "TotalTimeinminutesifavailable" column by replacing special characters and trimming whitespace
df_park = df_park.withColumn(
    "TotalTimeinminutesifavailable",
    trim(regexp_replace(col("TotalTimeinminutesifavailable"), "[^a-zA-Z0-9\s#(),:]", ""))
)

# Replace empty and NULL values in "TotalTimeinminutesifavailable"
df_park = df_park.withColumn(
    "TotalTimeinminutesifavailable",
    when((col("TotalTimeinminutesifavailable") == "") | (col("TotalTimeinminutesifavailable").isNull()), 
         expr("datediff(EndTime, StartTime) * 24 * 60")
        ).otherwise(col("TotalTimeinminutesifavailable"))
)

##################
# ParkConditions #     
##################

# Clean the "ParkConditions" column by replacing special characters and trimming whitespace
df_park = df_park.withColumn(
    "ParkConditions",
    trim(regexp_replace(col("ParkConditions"), "[^a-zA-Z0-9\s#(),:]", ""))
)

# Replace empty and NULL values with "ND"
df_park = df_park.withColumn(
    "ParkConditions",
    when((col("ParkConditions") == "") | (col("ParkConditions").isNull()), "ND").otherwise(col("ParkConditions"))
)

########################
# OtherAnimalSightings #
########################

# Clean the "OtherAnimalSightings" column by replacing special characters and trimming whitespace
df_park = df_park.withColumn(
    "OtherAnimalSightings",
    trim(regexp_replace(col("OtherAnimalSightings"), "[^a-zA-Z0-9\s#(),:]", ""))
)

# Replace empty and NULL values with "ND"
df_park = df_park.withColumn(
    "OtherAnimalSightings",
    when((col("OtherAnimalSightings") == "") | (col("OtherAnimalSightings").isNull()), "ND").otherwise(col("OtherAnimalSightings"))
)


##########
# Litter #
##########

# Clean the "Litter" column by replacing special characters and trimming whitespace
df_park = df_park.withColumn(
    "Litter",
    trim(regexp_replace(col("Litter"), "[^a-zA-Z0-9\s#(),:]", ""))
)

# Replace empty and NULL values with "ND"
df_park = df_park.withColumn(
    "Litter",
    when((col("Litter") == "") | (col("Litter").isNull()), "ND").otherwise(col("Litter"))
)

#######################
# Temperature&Weather #
#######################

# Clean the "Temperature&Weather" column by replacing special characters and trimming whitespace
df_park = df_park.withColumn(
    "Temperature&Weather",
    trim(regexp_replace(col("Temperature&Weather"), "[^a-zA-Z0-9\s#(),:]", ""))
)

# Replace empty and NULL values with "ND"
df_park = df_park.withColumn(
    "Temperature&Weather",
    when((col("Temperature&Weather") == "") | (col("Temperature&Weather").isNull()), "ND").otherwise(col("Temperature&Weather"))
)


# Show the result
df_park.show(truncate=False)





# # Load squirrel-data
# 

# In[6]:


# Load data from the CSV file into a DataFrame
df_squirrel = spark.read.csv("source/squirrel-data.csv", header=True, inferSchema=True, encoding="UTF-8")

# Rename columns by removing whitespace
for col_name in df_squirrel.columns:
    new_col_name = col_name.replace(" ", "")
    df_squirrel = df_squirrel.withColumnRenamed(col_name, new_col_name)

def clean_column_names(df):
    # Function to clean column names by removing parentheses and commas
    for col_name in df.columns:
        new_col_name = col_name.replace("(", "").replace(")", "").replace(",", "")
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

# Apply the function to clean column names
df_squirrel = clean_column_names(df_squirrel)

# Show the resulting DataFrame
df_squirrel.show(truncate=False)


# # STAGING

# ### Preprocessing squirrel-data

# In[7]:


# Assigning the DataFrame df_squirrel to squirrel_data
squirrel_data = df_squirrel

###################
# PrimaryFurColor #
###################
# Clean the "PrimaryFurColor" column by replacing special characters and trimming whitespace
squirrel_data = squirrel_data.withColumn(
    "PrimaryFurColor",
    trim(regexp_replace(col("PrimaryFurColor"), "[^a-zA-Z0-9\s#,]", ""))
)

# Replace empty and NULL values with "ND"
squirrel_data = squirrel_data.withColumn(
    "PrimaryFurColor",
    when((col("PrimaryFurColor") == "") | (col("PrimaryFurColor").isNull()), "ND").otherwise(col("PrimaryFurColor"))
)

########################
# HighlightsinFurColor #
########################
# Clean the "HighlightsinFurColor" column by replacing special characters and trimming whitespace
squirrel_data = squirrel_data.withColumn(
    "HighlightsinFurColor",
    trim(regexp_replace(col("HighlightsinFurColor"), "[^a-zA-Z0-9\s#,]", ""))
)

# Replace empty and NULL values with "ND"
squirrel_data = squirrel_data.withColumn(
    "HighlightsinFurColor",
    when((col("HighlightsinFurColor") == "") | (col("HighlightsinFurColor").isNull()), "ND").otherwise(col("HighlightsinFurColor"))
)

##############
# ColorNotes #
##############
# Clean the "ColorNotes" column by replacing special characters and trimming whitespace
squirrel_data = squirrel_data.withColumn(
    "ColorNotes",
    trim(regexp_replace(col("ColorNotes"), "[^a-zA-Z0-9\s#(),:]", ""))
)

# Replace empty and NULL values with "ND"
squirrel_data = squirrel_data.withColumn(
    "ColorNotes",
    when((col("ColorNotes") == "") | (col("ColorNotes").isNull()), "ND").otherwise(col("ColorNotes"))
)

#################################
# Above Ground (Height in Feet) #
#################################
# Clean the "Above Ground (Height in Feet)" column by replacing special characters
squirrel_data = squirrel_data.withColumn(
    "AboveGroundHeightinFeet",
    regexp_replace(col("AboveGroundHeightinFeet"), "[^\d\s><]", ",")
)

# Replace empty and NULL values with "0"
squirrel_data = squirrel_data.withColumn(
    "AboveGroundHeightinFeet",
    when((col("AboveGroundHeightinFeet") == "") | (col("AboveGroundHeightinFeet").isNull()), "0").otherwise(col("AboveGroundHeightinFeet"))
)

# Split the column "Above Ground (Height in Feet)" into two columns: minimum and maximum
squirrel_data = squirrel_data.withColumn(
    "Min_Height_Feet",
    when(col("AboveGroundHeightinFeet").contains("<"), 0).otherwise(col("AboveGroundHeightinFeet"))
)

squirrel_data = squirrel_data.withColumn(
    "Max_Height_Feet",
    when(col("AboveGroundHeightinFeet").contains("<"), 
         split(col("AboveGroundHeightinFeet"), "<")[1]).otherwise(col("AboveGroundHeightinFeet"))
)

# Split each row into a list of words using comma as delimiter
squirrel_data = squirrel_data.withColumn("Min_Height_List", split("Min_Height_Feet", ","))

# Remove empty elements from the list
squirrel_data = squirrel_data.withColumn(
    "Min_Height_List",
    array_remove("Min_Height_List", "")
)

# Convert elements of the list to integers
squirrel_data = squirrel_data.withColumn(
    "Min_Height_List",
    expr("transform(Min_Height_List, x -> CAST(x AS INT))")
)

# Find the minimum in the list and store it in the column "Min_Height"
squirrel_data = squirrel_data.withColumn(
    "Min_Height_Feet",
    expr("CAST(array_min(Min_Height_List) AS INT)")
)

# Drop the column "Min_Height_List" if no longer needed
squirrel_data = squirrel_data.drop("Min_Height_List")

# Split each row into a list of words using comma as delimiter
squirrel_data = squirrel_data.withColumn("Max_Height_List", split("Max_Height_Feet", ","))

# Remove empty elements from the list
squirrel_data = squirrel_data.withColumn(
    "Max_Height_List",
    array_remove("Max_Height_List", "")
)

# Convert elements of the list to integers
squirrel_data = squirrel_data.withColumn(
    "Max_Height_List",
    expr("transform(Max_Height_List, x -> CAST(x AS INT))")
)

# Find the minimum in the list and store it in the column "Min_Height"
squirrel_data = squirrel_data.withColumn(
    "Max_Height_Feet",
    expr("CAST(array_max(Max_Height_List) AS INT)")
)

# Drop the column "Min_Height_List" if no longer needed
squirrel_data = squirrel_data.drop("Max_Height_List")

# Drop the column "AboveGroundHeightinFeet" if no longer needed
squirrel_data = squirrel_data.drop("AboveGroundHeightinFeet")

#############
#  Location #
#############
# Clean the "Location" column by replacing special characters and trimming whitespace
squirrel_data = squirrel_data.withColumn(
    "Location",
    trim(regexp_replace(col("Location"), "[^a-zA-Z0-9\s#(),:]", ""))
)

# Replace empty and NULL values with "ND"
squirrel_data = squirrel_data.withColumn(
    "Location",
    when((col("Location") == "") | (col("Location").isNull()), "ND").otherwise(col("Location"))
)

#####################
# Specific Location #
#####################
# Clean the "Specific Location" column by replacing special characters and trimming whitespace
squirrel_data = squirrel_data.withColumn(
    "SpecificLocation",
    trim(regexp_replace(col("SpecificLocation"), "[^a-zA-Z0-9\s#(),:]", ""))
)

# Replace empty and NULL values with "ND"
squirrel_data = squirrel_data.withColumn(
    "SpecificLocation",
    when((col("SpecificLocation") == "") | (col("SpecificLocation").isNull()), "ND").otherwise(col("SpecificLocation"))
)

################
# "Activities" #
################
# Clean the "Activities" column by replacing special characters
squirrel_data = squirrel_data.withColumn(
    "Activities",
    trim(regexp_replace(col("Activities"), "[^a-zA-Z0-9\s#,/?():]", " "))
)

# Replace empty and NULL values with "ND"
squirrel_data = squirrel_data.withColumn(
    "Activities",
    when((col("Activities") == "") | (col("Activities").isNull()), "ND").otherwise(col("Activities"))
)

##########################
# InteractionswithHumans #
##########################
# Clean the "Activities" column by replacing special characters
squirrel_data = squirrel_data.withColumn(
    "InteractionswithHumans",
    trim(regexp_replace(col("InteractionswithHumans"), "[^a-zA-Z0-9\s#,/?():]", " "))
)

# Replace empty and NULL values with "ND"
squirrel_data = squirrel_data.withColumn(
    "InteractionswithHumans",
    when((col("InteractionswithHumans") == "") | (col("InteractionswithHumans").isNull()), "ND").otherwise(col("InteractionswithHumans"))
)

############################
# OtherNotesorObservations #
############################
# Clean the "Activities" column by replacing special characters
squirrel_data = squirrel_data.withColumn(
    "OtherNotesorObservations",
    trim(regexp_replace(col("OtherNotesorObservations"), "[^a-zA-Z0-9\s#,/?():]", " "))
)

# Replace empty and NULL values with "ND"
squirrel_data = squirrel_data.withColumn(
    "OtherNotesorObservations",
    when((col("OtherNotesorObservations") == "") | (col("OtherNotesorObservations").isNull()), "ND").otherwise(col("OtherNotesorObservations"))
)

# Show the results
squirrel_data.show(truncate=False)


# # RELATIONAL - PARK

# ## Areas

# In[8]:


# Create an areas table, removing duplicates and renaming columns
# Selecting only "AreaID" and "AreaName" columns and removing duplicates
Areas = df_park.select("AreaID", "AreaName").distinct()

# Rename the 'AreaID' column to 'Area_ID'
Areas = Areas.withColumnRenamed("AreaID", "Area_ID")

# Sort by Area_ID in ascending order
Areas = Areas.orderBy("Area_ID")

# Create a new column "ParkStatusID" with unique identifiers starting from 1
# Assign unique IDs to each row using monotonically_increasing_id() function
Areas = Areas.withColumn("AreaID", monotonically_increasing_id() + 1)

# Select the columns in the desired order
Areas = Areas.select("AreaID", "Area_ID", "AreaName")

# Show the resulting areas table
Areas.show()


# ### Areas | Table Update

# In[9]:


# Create or replace a temporary view from the DataFrame 
Areas.createOrReplaceTempView("View_Areas")

# Define the name of the table containing park conditions
table_name = "areas"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(AreaID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.AreaID,
                            b.Area_ID,
                            b.AreaName
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.AreaName) AS AreaID,
                            a.Area_ID,
                            a.AreaName
                        FROM View_Areas a
                        LEFT JOIN View_table_db b ON trim(a.AreaName) = trim(b.AreaName) AND
                                                     a.Area_ID        = b.Area_ID
                                                    
                        WHERE b.AreaID IS NULL
            """

# Call the function to update park conditions
# The function update_table_db updates the areas table in the database
Areas = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, Areas)

# Create or replace a temporary view from the updated DataFrame
Areas.createOrReplaceTempView("View_Areas")


# ## Parks

# In[10]:


# Select relevant columns from the DataFrame
Parks = df_park.select("ParkID", "AreaID", "ParkName")

# Remove everything between parentheses in the "Park Name" column
Parks = Parks.withColumn("ParkName", regexp_replace(col("ParkName"), "\\s*\\(.*?\\)\\s*", ""))

# Convert values in the "Park ID" column to integers
Parks = Parks.withColumn("ParkID", Parks["ParkID"].cast(IntegerType()))

# Perform distinct operation on the resulting table
Parks = Parks.distinct()

# Sort by the "Park ID" column
Parks = Parks.orderBy("ParkID")

# Create or replace a temporary view from the DataFrame 
Parks.createOrReplaceTempView("View_tmpParks")

# SQL query to join with the areas table and select relevant columns
Parks = spark.sql("""
    SELECT a.ParkID, b.AreaID, a.ParkName
    FROM View_tmpParks a LEFT JOIN View_Areas b ON a.AreaID = b.Area_ID
    ORDER BY a.ParkID
""")


# ### Parks | Table Update

# In[11]:


# Create or replace a temporary view from the DataFrame 
Parks.createOrReplaceTempView("View_Parks_tmp")

# Define the name of the table containing park conditions
table_name = "parks"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(ParkID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.ParkID,
                            b.AreaID,
                            b.ParkName
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.ParkName) AS ParkID,
                            a.AreaID,
                            a.ParkName
                        FROM View_Parks_tmp a
                        LEFT JOIN View_table_db b ON trim(a.ParkName) = trim(b.ParkName) AND
                                                          a.AreaID    = b.AreaID
                        WHERE b.ParkID IS NULL
            """
# Call the function to update park conditions
Parks = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, Parks)

# Create or replace a temporary view from the DataFrame 
Parks.createOrReplaceTempView("View_Parks")


# ## ParkSections

# In[12]:


# Filter rows containing the word "section" in the "Park Name" column (case-insensitive)
ParkSections = df_park.filter(lower(df_park["ParkName"]).contains("section"))

# Create a table of park sections, removing duplicates and renaming columns
ParkSections = ParkSections.select("ParkID", "ParkName")

# Keep only what comes after the word "section" in the "Park Name" column (case-insensitive)
ParkSections = ParkSections.withColumn("ParkName", split(lower(ParkSections["ParkName"]), "section")[1])

# Remove the ")" at the end of the text
ParkSections = ParkSections.withColumn("ParkName", regexp_replace(col("ParkName"), "\\)$", ""))

# Capitalize the first letter of each word
ParkSections = ParkSections.withColumn("ParkName", initcap(col("ParkName")))

# Create a new column containing only the integer part of "Park ID"
ParkSections = ParkSections.withColumn("Park ID", regexp_extract(col("ParkID").cast("string"), r"\d+", 0))

# Select columns in the desired order
ParkSections = ParkSections.select("ParkID", "Park ID", "ParkName")

# Rename columns
ParkSections = ParkSections.withColumnRenamed("ParkID", "SectionID") \
                           .withColumnRenamed("Park ID", "ParkID") \
                           .withColumnRenamed("ParkName", "ParkSectionName")

# Select only what comes after the dot in the "SectionID" column
ParkSections = ParkSections.withColumn("SectionID", split(col("SectionID").cast("string"), "\\.").getItem(1))

# Create a new column "ParkSectionID" with unique identifiers starting from 1
ParkSections = ParkSections.withColumn("ParkSectionID", monotonically_increasing_id() + 1)

# Select the desired columns
ParkSections = ParkSections.select("ParkSectionID", "SectionID", "ParkID", "ParkSectionName")


# ### ParkSections | Table Update

# In[13]:


# Create or replace a temporary view from the DataFrame 
ParkSections.createOrReplaceTempView("View_ParkSections")

# Define the name of the table containing park conditions
table_name = "parksections"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(ParkSectionID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.ParkSectionID,
                            b.ParkID,
                            b.SectionID,
                            b.ParkSectionName
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.ParkSectionName) AS ParkSectionID,
                            a.ParkID,
                            a.SectionID,
                            a.ParkSectionName
                        FROM View_ParkSections a
                        LEFT JOIN View_table_db b ON trim(a.ParkSectionName) = trim(b.ParkSectionName)
                        WHERE b.ParkSectionID IS NULL
            """

# Call the function to update park conditions
ParkSections = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, ParkSections)

# Create or replace a temporary view from the DataFrame 
ParkSections.createOrReplaceTempView("View_ParkSections")


# ## ParkConditions

# In[14]:


# Select the "ParkConditions" column from the DataFrame
ParkConditions = df_park.select("ParkConditions")

# Fill null values in the "Park Conditions" column with "ND"
ParkConditions = ParkConditions.withColumn("ParkConditions", when(col("ParkConditions").isNull(), "ND").otherwise(col("ParkConditions")))

# Remove everything that comes after the comma
ParkConditions = ParkConditions.withColumn("ParkConditions", split(col("ParkConditions"), ",")[0])

# Perform group by operation on the "Park Conditions" column without aggregation
ParkConditions = ParkConditions.groupBy("ParkConditions").count().distinct()

# Remove any quotes within the values
ParkConditions = ParkConditions.withColumn("ParkConditions", regexp_replace(col("ParkConditions"), '"', ''))

# Sort the results in alphabetical order
ParkConditions = ParkConditions.orderBy("ParkConditions")

# Create a new column "ParkConditionID" with unique identifiers starting from 1
ParkConditions = ParkConditions.withColumn("ParkConditionID", monotonically_increasing_id() + 1)

# Rename the column to "ParkConditionName"
ParkConditions = ParkConditions.withColumnRenamed("ParkConditions", "ParkConditionName")

# Reorder the columns to place "ParkConditionID" at the beginning
ParkConditions = ParkConditions.select("ParkConditionID", "ParkConditionName")


# ### ParkConditions | Table Update

# In[15]:


# Create or replace a temporary view from the DataFrame 
ParkConditions.createOrReplaceTempView("View_ParkConditions")

# Define the name of the table containing park conditions
table_name = "parkconditions"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(ParkConditionID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.ParkConditionID,
                            b.ParkConditionName
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.ParkConditionName) AS ParkConditionID,
                            a.ParkConditionName
                        FROM View_ParkConditions a
                        LEFT JOIN View_table_db b ON trim(a.ParkConditionName) = trim(b.ParkConditionName)
                        WHERE b.ParkConditionID IS NULL
            """

# Call the function to update park conditions
ParkConditions = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, ParkConditions)

# Create or replace a temporary view from the DataFrame 
ParkConditions.createOrReplaceTempView("View_ParkConditions")


# ## ParkLitters
# 

# In[16]:


# Select the "Litter" column from the DataFrame
ParksLitters = df_park.select("Litter")

# Fill null values in the "Litter" column with "ND"
ParkLitters = ParksLitters.withColumn("Litter", when(col("Litter").isNull(), "ND").otherwise(col("Litter")))

# Remove everything that comes after the comma
ParkLitters = ParkLitters.withColumn("Litter", split(col("Litter"), ",")[0])

# Perform group by operation on the "Litter" column without aggregation
ParkLitters = ParkLitters.groupBy("Litter").count().distinct()

# Remove any quotes within the values
ParkLitters = ParkLitters.withColumn("Litter", regexp_replace(col("Litter"), '"', ''))

# Sort the results in alphabetical order
ParkLitters = ParkLitters.orderBy("Litter")

# Create a new column "ParkLitterID" with unique identifiers starting from 1
ParkLitters = ParkLitters.withColumn("ParkLitterID", monotonically_increasing_id() + 1)

# Reorder the columns to place "ParkLitterID" at the beginning
ParkLitters = ParkLitters.select("ParkLitterID", "Litter")

# Rename the column to "ParkLitterName"
ParkLitters = ParkLitters.withColumnRenamed("Litter", "ParkLitterName")


# ### ParkLitters | Table Update

# In[17]:


# Create or replace a temporary view from the DataFrame 
ParkLitters.createOrReplaceTempView("View_ParkLitters")

# Define the name of the table containing park conditions
table_name = "parklitters"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(ParkLitterID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.ParkLitterID,
                            b.ParkLitterName
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.ParkLitterName) AS ParkLitterID,
                            a.ParkLitterName
                        FROM View_ParkLitters a
                        LEFT JOIN View_table_db b ON trim(a.ParkLitterName) = trim(b.ParkLitterName)
                        WHERE b.ParkLitterID IS NULL
            """

# Call the function to update park conditions
ParkLitters = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql,  ParkLitters)

# Create or replace a temporary view from the DataFrame 
ParkLitters.createOrReplaceTempView("View_ParkLitters")


# ## Animals

# In[18]:


# Select the "OtherAnimalSightings" column from the DataFrame
ParksAnimals = df_park.select("OtherAnimalSightings")

# Fill null values in the "OtherAnimalSightings" column with "ND"
Animals = ParksAnimals.withColumn("OtherAnimalSightings", when(col("OtherAnimalSightings").isNull(), "ND").otherwise(col("OtherAnimalSightings")))

# Use regexp_replace to remove everything within parentheses, including the parentheses
Animals = Animals.withColumn("OtherAnimalSightings", regexp_replace("OtherAnimalSightings", "\([^)]+\)", ""))

# Split each row into a list of words using comma as delimiter
Animals = Animals.withColumn("Animal_List", split("OtherAnimalSightings", ","))

# Expand the list into separate rows
Animals = Animals.withColumn("AnimalName", explode("Animal_List"))

# Apply trim to each word in the list
Animals = Animals.withColumn("AnimalName", trim("AnimalName"))

# Remove duplicates
Animals = Animals.dropDuplicates(["AnimalName"])

# Sort the results in alphabetical order
Animals  = Animals.orderBy("AnimalName")

# Create a new column "AnimalID" with unique identifiers starting from 1
Animals = Animals.withColumn("AnimalID", monotonically_increasing_id() + 1)

# Select only the "AnimalID" and "AnimalName" columns
Animals = Animals.select("AnimalID", "AnimalName")


# ### Animals | Table Update

# In[19]:


# Create or replace a temporary view from the DataFrame 
Animals.createOrReplaceTempView("View_Animals")

# Define the name of the table containing park conditions
table_name = "animals"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(AnimalID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.AnimalID,
                            b.AnimalName
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.AnimalName) AS AnimalID,
                            a.AnimalName
                        FROM View_Animals a
                        LEFT JOIN View_table_db b ON trim(a.AnimalName) = trim(b.AnimalName)
                        WHERE b.AnimalID IS NULL
            """

# Call the function to update park conditions
Animals = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, Animals)

# Create or replace a temporary view from the DataFrame 
Animals.createOrReplaceTempView("View_Animals")


# ## ParkStatus - Data

# ## Weather

# In[20]:


# Select the "Temperature&Weather" column from the DataFrame
ParksTemperature = df_park.select("Temperature&Weather")

# Fill null values in the "Temperature & Weather" column with "ND"
Weather = ParksTemperature.withColumn("Temperature&Weather", when(col("Temperature&Weather").isNull(), "ND").otherwise(col("Temperature&Weather")))

# Define a regular expression pattern to extract what follows "degrees, "
degree_pattern = r'.*\b(\d+(?:-\w+)?)\s+degrees,\s*(.*)'

# Apply regexp_extract to get what comes after "degrees, " if it exists, or leave the column as it is
Weather_1 = Weather.withColumn("WeatherName", regexp_extract(Weather["Temperature&Weather"], degree_pattern, 2))

# Add a new column "WeatherName" with null values
Weather_2 = Weather_1.withColumn("WeatherName", when(col("Temperature&Weather").contains("degree") | col("Temperature&Weather").contains("ND"), None).otherwise(col("Temperature&Weather")))
Weather_2 = Weather_2.withColumn("WeatherName", when(col("WeatherName").isNull(), "").otherwise(col("WeatherName")))

# Create temporary views for the DataFrames
Weather_1.createOrReplaceTempView("Weather_1")
Weather_2.createOrReplaceTempView("Weather_2")

# Perform a full join using SQL
# Add the positions of "with" and "but" in the columns of interest to the DataFrame
Weathers = spark.sql("""
    SELECT 
        COALESCE(w1.`Temperature&Weather`, w2.`Temperature&Weather`) AS `Temperature&Weather`,
        COALESCE(w1.WeatherName, w2.WeatherName) AS WeatherName,
        INSTR(COALESCE(w1.WeatherName, w2.WeatherName), 'with') AS PositionWith,
        INSTR(COALESCE(w1.WeatherName, w2.WeatherName), 'but') AS PositionBut
    FROM Weather_1 w1
    FULL OUTER JOIN Weather_2 w2
    ON w1.`Temperature&Weather` = w2.`Temperature&Weather`
""")

# Create a temporary view for the resulting DataFrame
Weathers.createOrReplaceTempView("Weather_3")

# Extract weather conditions from the combined "WeatherName" column
Weathers = spark.sql("""
    SELECT `Temperature&Weather`,
           CASE
                WHEN PositionWith > 0 THEN 
                    SUBSTRING(COALESCE(WeatherName), 1, PositionWith - 1)
                WHEN PositionBut > 0 THEN 
                    SUBSTRING(COALESCE(WeatherName), 1, PositionBut - 1)
                ELSE 
                    WeatherName
        END AS WeatherName
    FROM Weather_3 
""")

# Define a regular expression pattern to detect absence of the word "degrees"
regex_pattern = r"^(?!.*\bdegrees\b).*$"

# Use regexp_extract to identify entries that do not contain the word "degrees" and are not "ND"
Weathers = Weathers.withColumn(
    "WeatherName",
    when(col("Temperature&Weather") == "ND", col("WeatherName")).otherwise(
        when(regexp_extract(col("Temperature&Weather"), regex_pattern, 0) != "", col("Temperature&Weather")).otherwise(col("WeatherName"))
    )
)

# Fill null values in the "Temperature & Weather" column with "ND" if it is empty
Weathers = Weathers.withColumn("WeatherName", when(col("WeatherName").isNull() | (col("WeatherName") == ""), "ND").otherwise(col("WeatherName")))

# Sort the results alphabetically
Weathers = Weathers.orderBy("WeatherName")

# Split each row into a list of words using comma as delimiter
Weathers = Weathers.withColumn("WeatherName_List", split("WeatherName", ","))

# Expand the list into separate rows
Weathers = Weathers.withColumn("WeatherName", explode("WeatherName_List"))

# Apply trim to each word in the list
Weathers = Weathers.withColumn("WeatherName", trim(Weathers["WeatherName"]))

# Select only the "WeatherName" column
Weathers = Weathers.select("WeatherName")

# Fill null values in the "Temperature & Weather" column with "ND" if it is empty
Weathers = Weathers.withColumn("WeatherName", when(col("WeatherName").isNull() | (col("WeatherName") == ""), "ND").otherwise(col("WeatherName")))

# Remove duplicates
Weathers = Weathers.dropDuplicates(["WeatherName"])

# Sort the results alphabetically
Weathers = Weathers.orderBy(F.lower(F.col("WeatherName")))

# Create a new column "WeatherID" with unique identifiers starting from 1
Weathers = Weathers.withColumn("WeatherID", monotonically_increasing_id() + 1)

# Select only the "WeatherID" and "WeatherName" columns
Weathers = Weathers.select("WeatherID", "WeatherName")


# ### Weathers | Table Update

# In[21]:


# Create or replace a temporary view from the DataFrame 
Weathers.createOrReplaceTempView("View_Weathers")

# Define the name of the table containing park conditions
table_name = "weathers"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(WeatherID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.WeatherID,
                            b.WeatherName
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.WeatherName) AS WeatherID,
                            a.WeatherName
                        FROM View_Weathers a
                        LEFT JOIN View_table_db b ON trim(a.WeatherName) = trim(b.WeatherName)
                        WHERE b.WeatherID IS NULL
            """

# Call the function to update park conditions
Weathers = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, Weathers)

Weathers.createOrReplaceTempView("View_Weathers")


# ## ParkStatus processing

# In[22]:


# Select relevant columns from the DataFrame
ParkStatus = df_park.select("ParkID", "Date", "StartTime", "EndTime", "TotalTimeinminutesifavailable", "ParkConditions", "Litter", "Temperature&Weather", "OtherAnimalSightings")

# Extract temperature information from the "Temperature&Weather" column
ParkStatus = ParkStatus.withColumn(
    "Weather",
    when(
        col("Temperature&Weather").isNull() | (col("Temperature&Weather") == ""),
        "ND"
    ).otherwise(
        trim(substring_index(col("Temperature&Weather"), "degrees,", -1))
    )
)

# Replace content with "ND" if it contains "degrees"
ParkStatus = ParkStatus.withColumn(
    "Weather",
    when(col("Weather").contains("degrees"), "ND").otherwise(col("Weather"))
)

# Extract temperature in degrees from the "Temperature&Weather" column
ParkStatus = ParkStatus.withColumn(
    "Temperature_degrees",
    regexp_extract(col("Temperature&Weather"), r"(\d+)", 1)
)

# Replace non-numeric values with "ND"
ParkStatus = ParkStatus.withColumn(
    "Temperature_degrees",
    when(col("Temperature_degrees").cast("int").isNull(), "ND").otherwise(col("Temperature_degrees"))
)

# Rename the column
ParkStatus = ParkStatus.withColumnRenamed("ParkConditions", "ParkConditions_1")

# Extract the primary park condition
ParkStatus = ParkStatus.withColumn(
    "ParkConditions",
    regexp_replace(col("ParkConditions_1"), ",.*", "")
)

# Extract observations related to park conditions
ParkStatus = ParkStatus.withColumn(
    "ParkConditionsObs",
    when(col("ParkConditions_1").contains(','),  # If comma is present
         trim(expr("substring(ParkConditions_1, instr(ParkConditions_1, ',') + 1)")))
    .otherwise("")  # If no comma is present
)

# Rename the column
ParkStatus = ParkStatus.withColumnRenamed("Litter", "Litter_1")

# Extract the primary litter condition
ParkStatus = ParkStatus.withColumn(
    "ParkLitter",
    regexp_replace(col("Litter_1"), ",.*", "")
)

# Extract observations related to litter conditions
ParkStatus = ParkStatus.withColumn(
    "ParkLitterObs",
    when(col("Litter_1").contains(','),  # If comma is present
         trim(expr("substring(Litter_1, instr(Litter_1, ',') + 1)")))
    .otherwise("")  # If no comma is present
)

# Create a temporary view for the DataFrame
ParkStatus.createOrReplaceTempView("View_ParkStatus")

# Perform a left join using SQL
ParkStatus = spark.sql("""
    SELECT PkSt.*, Ltt.ParkLitterID, Cnd.ParkConditionID
    FROM View_ParkStatus PkSt LEFT JOIN View_ParkLitters    Ltt ON trim(PkSt.ParkLitter)     = trim(Ltt.ParkLitterName)
                              LEFT JOIN View_ParkConditions Cnd ON trim(PkSt.ParkConditions) = trim(Cnd.ParkConditionName)
""")

# Create a new column "ParkStatusID" with unique identifiers starting from 1
ParkStatus = ParkStatus.withColumn("ParkStatusID", monotonically_increasing_id() + 1)

# Create a temporary view for the DataFrame
ParkStatus.createOrReplaceTempView("View_ParkStatus")

# Show the DataFrame
ParkStatus.show()


# ## ParkStatusWeather

# In[23]:


###########
# Weather #
###########

# Remove trailing comma from "Weather", if it exists
ParkStatus = ParkStatus.withColumn(
    "Weather",
    regexp_replace(col("Weather"), ",$", "")
)

# Extract text following "but" or "with" (including "but" or "with") into the "WeatherObs" column
ParkStatus = ParkStatus.withColumn(
    "WeatherObs",
    regexp_extract(col("Weather"), "(but|with).*$", 0)
)

# Remove the extracted text from "Weather" and leave only the text before "but" or "with"
ParkStatus = ParkStatus.withColumn(
    "Weather",
    trim(regexp_replace(col("Weather"), "(but|with).*$", ""))
)

# Create a temporary view for the DataFrame
ParkStatus.createOrReplaceTempView("View_ParkStatus")

# Perform a full join using SQL
ParkStatusWheater = spark.sql("""
    SELECT ParkStatusID, Weather, WeatherObs
    FROM View_ParkStatus 
""")

# Split the column into separate rows and remove additional whitespace
ParkStatusWheater = ParkStatusWheater.withColumn("Weather", explode(split(trim(ParkStatusWheater["Weather"]), ", ")))

# Remove commas from the column
ParkStatusWheater = ParkStatusWheater.withColumn('Weather', regexp_replace('Weather', ',', ''))

# Replace empty values in the 'Weather' column with 'ND'
ParkStatusWheater = ParkStatusWheater.withColumn('Weather', regexp_replace(col('Weather'), '^$', 'ND'))

# Create a new column "ParkStatusWheaterID" with unique identifiers starting from 1
ParkStatusWheater  = ParkStatusWheater.withColumn("ParkStatusWheaterID", monotonically_increasing_id() + 1)

ParkStatusWheater.createOrReplaceTempView("View_ParkStatusWheater")

# Perform a full join using SQL
ParkStatusWheater = spark.sql("""
    SELECT a.ParkStatusWheaterID, a.ParkStatusID, b.WeatherID, a.WeatherObs
    FROM View_ParkStatusWheater a LEFT JOIN View_Weathers b on trim(a.Weather) = trim(b.WeatherName)
""")


# ### ParkStatusWheater | Table Update

# In[24]:


# Create or replace a temporary view from the DataFrame 
ParkStatusWheater.createOrReplaceTempView("View_ParkStatusWheater")

# Define the name of the table containing park conditions
table_name = "parkstatuswheater"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(ParkStatusWheaterID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.ParkStatusWheaterID,
                            b.ParkStatusID,
                            b.WeatherID,
                            b.WeatherObs
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.ParkStatusID, a.WeatherID) AS ParkStatusWheaterID,
                            a.ParkStatusID,
                            a.WeatherID,
                            a.WeatherObs
                        FROM View_ParkStatusWheater a
                        LEFT JOIN View_table_db b ON a.ParkStatusID = b.ParkStatusID AND
                                                     a.WeatherID    = b.WeatherID    AND
                                                     a.WeatherObs   = b.WeatherObs 
                        WHERE b.ParkStatusWheaterID IS NULL
            """

# Call the function to update park conditions
ParkStatusWheater = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, ParkStatusWheater)

ParkStatusWheater.createOrReplaceTempView("View_ParkStatusWheater")


# ## ParkStatusAnimals

# In[25]:


ParkStatusAnimals = spark.sql("""
    SELECT ParkStatusID, OtherAnimalSightings as Animal
    FROM View_ParkStatus 
""")

# Define the regular expression to find text within parentheses containing commas
pattern = "\(([^)]*),([^)]*)\)"

# Replace commas within parentheses with "xxzxx"
ParkStatusAnimals = ParkStatusAnimals.withColumn("Animal", regexp_replace(ParkStatusAnimals["Animal"], pattern, "($1xxzxx$2)"))

# Split the column into separate rows and remove additional whitespace
ParkStatusAnimals = ParkStatusAnimals.withColumn("Animal", explode(split(trim(ParkStatusAnimals["Animal"]), ", ")))

# Extract text within parentheses into a new column
ParkStatusAnimals = ParkStatusAnimals.withColumn("ParkStatusAnimalObs", regexp_extract(col("Animal"), "\(([^)]*)\)", 1))

# Remove parentheses from the OtherAnimalSightings column
ParkStatusAnimals = ParkStatusAnimals.withColumn("Animal", regexp_replace(col("Animal"), "\([^)]*\)", ""))

# Replace "xxzxx" with commas in the ParkStatusAnimalObs column
ParkStatusAnimals = ParkStatusAnimals.withColumn("ParkStatusAnimalObs", regexp_replace(col("ParkStatusAnimalObs"), "xxzxx", ", "))

# Create a new column "ParkStatusAnimalID" with unique identifiers starting from 1
ParkStatusAnimals  = ParkStatusAnimals.withColumn("ParkStatusAnimalID", monotonically_increasing_id() + 1)

ParkStatusAnimals.createOrReplaceTempView("View_ParkStatusAnimals")

# Perform a left join using SQL
ParkStatusAnimals = spark.sql("""
    SELECT ParkStatusAnimalID, ParkStatusID, AnimalID, ParkStatusAnimalObs
    FROM View_ParkStatusAnimals a LEFT JOIN View_Animals b on trim(a.Animal) = trim(b.AnimalName)  
""")


# ### ParkStatusAnimals | Table Update

# In[26]:


# Create or replace a temporary view from the DataFrame 
ParkStatusAnimals.createOrReplaceTempView("View_ParkStatusAnimals")

# Define the name of the table containing park conditions
table_name = "parkstatusanimals"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(ParkStatusAnimalID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.ParkStatusAnimalID, 
                            b.ParkStatusID, 
                            b.AnimalID, 
                            b.ParkStatusAnimalObs
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.ParkStatusID, a.AnimalID) AS ParkStatusAnimalID,
                            a.ParkStatusID,
                            a.AnimalID, 
                            a.ParkStatusAnimalObs
                        FROM View_ParkStatusAnimals a
                        LEFT JOIN View_table_db b ON a.ParkStatusID        = b.ParkStatusID  AND
                                                     a.AnimalID            = b.AnimalID      AND
                                                     a.ParkStatusAnimalObs = b.ParkStatusAnimalObs 
                        WHERE b.ParkStatusAnimalID IS NULL
            """

# Call the function to update park conditions
ParkStatusAnimals = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, ParkStatusAnimals)

ParkStatusAnimals.createOrReplaceTempView("View_ParkStatusAnimals")


# ## ParkStatus

# In[27]:


# Remove columns from "ParkStatus"
ParkStatus = ParkStatus.drop("Weather")
ParkStatus = ParkStatus.drop("WeatherObs")
ParkStatus = ParkStatus.drop("ParkConditions")
ParkStatus = ParkStatus.drop("OtherAnimalSightings")
ParkStatus = ParkStatus.drop("ParkConditions_1")
ParkStatus = ParkStatus.drop("Temperature&Weather")
ParkStatus = ParkStatus.drop("Litter_1")

# Split the value of ParkID into two columns: IntegerPart and DecimalPart
split_col = split(ParkStatus['ParkID'], '\.')
ParkStatus = ParkStatus.withColumn('ParkID_real', split_col.getItem(0))
ParkStatus = ParkStatus.withColumn('SectionID', split_col.getItem(1))

# Create a new column "ParkStatusID" with unique identifiers starting from 1
ParkStatus = ParkStatus.withColumn("ParkStatusID", monotonically_increasing_id() + 1)

ParkStatus.createOrReplaceTempView("View_ParkStatus")

# Perform a left join using SQL
ParkStatus = spark.sql("""
    SELECT a.ParkStatusID, CAST(a.ParkID_real AS INT) as ParkID, b.ParkSectionID, a.Date  as ParkStatusDate, a.StartTime as ParkStatusStartTime, a.EndTime as ParkStatusEndTime, a.TotalTimeinminutesifavailable as ParkStatusTotalTme,
           a.ParkConditionID, a.ParkConditionsObs as ParkStatusConditionsObs, a.ParkLitterID, a.ParkLitterObs as ParkStatusLitterObs, a.Temperature_degrees as ParkStatusTemperatureDegrees
    FROM View_ParkStatus a LEFT JOIN View_ParkSections b ON a.ParkID_real = b.ParkID and a.SectionID = b.SectionID
""")

ParkStatus.show()


# ### ParkStatus | Table Update

# In[28]:


# Create or replace a temporary view from the DataFrame 
ParkStatus.createOrReplaceTempView("View_ParkStatus")

# Define the name of the table containing park conditions
table_name = "parkstatus"


# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(ParkStatusID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.ParkStatusID,
                            b.ParkID,
                            b.ParkSectionID,
                            b.ParkStatusDate,
                            b.ParkStatusStartTime,
                            b.ParkStatusEndTime,
                            b.ParkStatusTotalTme,
                            b.ParkConditionID,
                            b.ParkStatusConditionsObs,
                            b.ParkLitterID,
                            b.ParkStatusLitterObs,
                            b.ParkStatusTemperatureDegrees
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY   a.ParkID,
                                                                                a.ParkSectionID,
                                                                                a.ParkStatusDate,
                                                                                a.ParkStatusStartTime,
                                                                                a.ParkStatusEndTime,
                                                                                a.ParkStatusTotalTme,
                                                                                a.ParkConditionID,
                                                                                a.ParkStatusConditionsObs,
                                                                                a.ParkLitterID,
                                                                                a.ParkStatusLitterObs,
                                                                                a.ParkStatusTemperatureDegrees) AS ParkStatusAnimalID,
                            a.ParkID,
                            a.ParkSectionID,
                            a.ParkStatusDate,
                            a.ParkStatusStartTime,
                            a.ParkStatusEndTime,
                            a.ParkStatusTotalTme,
                            a.ParkConditionID,
                            a.ParkStatusConditionsObs,
                            a.ParkLitterID,
                            a.ParkStatusLitterObs,
                            a.ParkStatusTemperatureDegrees
                        FROM View_ParkStatus a
                        LEFT JOIN View_table_db b ON    a.ParkID                  = b.ParkID AND
                                                        a.ParkSectionID           = b.ParkSectionID AND
                                                        a.ParkStatusDate          = b.ParkStatusDate  AND
                                                        a.ParkStatusStartTime     = b.ParkStatusStartTime AND  
                                                        a.ParkStatusEndTime       = b.ParkStatusEndTime AND 
                                                        a.ParkStatusTotalTme      = b.ParkStatusTotalTme AND
                                                        a.ParkConditionID         = b.ParkConditionID  AND
                                                        a.ParkStatusConditionsObs = b.ParkStatusConditionsObs  AND
                                                        a.ParkLitterID            = b.ParkLitterID   AND
                                                        a.ParkStatusLitterObs     = b.ParkStatusLitterObs AND
                                                        a.ParkStatusTemperatureDegrees = b.ParkStatusTemperatureDegrees
                        WHERE b.ParkStatusID IS NULL
            """

# Call the function to update park conditions
ParkStatus = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, ParkStatus)

ParkStatus.createOrReplaceTempView("View_ParkStatus")


# # RELATIONAL - SQUIRREL

# In[29]:


# Rename columns
squirrel_data = squirrel_data.withColumnRenamed("SquirrelLatitudeDD.DDDDDD", "SquirrelLatitude") \
                             .withColumnRenamed("SquirrelLongitude-DD.DDDDDD", "SquirrelLongitude")

# Create a new column "SquirrelDataID" with unique identifiers starting from 1
squirrel_data = squirrel_data.withColumn("SquirrelDataID", monotonically_increasing_id() + 1)

squirrel_data.createOrReplaceTempView("View_squirrel_data")

squirrel_data.show(20)


# # Colors

# In[30]:


# Get a unique list of colors by combining PrimaryFurColor and HighlightsinFurColor
Colors = spark.sql("""
    SELECT EXPLODE(SPLIT(CONCAT_WS(', ', PrimaryFurColor, HighlightsinFurColor), ', ')) AS ColorName
    FROM View_squirrel_data
""")

# Remove extra white spaces and obtain unique colors
Colors = Colors.withColumn("ColorName", trim(Colors["ColorName"]))
Colors = Colors.distinct()

# Sort and add a row number to the colors
window = Window.orderBy("ColorName")
Colors = Colors.withColumn("ColorID", row_number().over(window))

Colors = Colors.select("ColorID", "ColorName")


# ### Colors | Table Update

# In[31]:


# Create or replace a temporary view from the DataFrame 
Colors.createOrReplaceTempView("View_Colors")

# Define the name of the table containing park conditions
table_name = "colors"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(ColorID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.ColorID,
                            b.ColorName
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.ColorName) AS ColorID,
                            a.ColorName
                        FROM View_Colors a
                        LEFT JOIN View_table_db b ON trim(a.ColorName) = trim(b.ColorName)
                        WHERE b.ColorID IS NULL
            """

# Call the function to update park conditions
Colors = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, Colors)

Colors.createOrReplaceTempView("View_Colors")


# # Locations

# In[32]:


# Get unique locations and split them by commas
Locations = spark.sql("""
    SELECT EXPLODE(SPLIT(Location, ', ')) AS LocationName
    FROM View_squirrel_data
    WHERE Location IS NOT NULL
""")

# Remove extra white spaces and obtain unique locations
Locations = Locations.withColumn("LocationName", trim(Locations["LocationName"]))
Locations = Locations.distinct()

# Sort and add a row number to the locations
window = Window.orderBy("LocationName")
Locations = Locations.withColumn("LocationID", F.row_number().over(window))

Locations = Locations.select("LocationID", "LocationName")


# ### Locations | Table Update

# In[33]:


# Create or replace a temporary view from the DataFrame 
Locations.createOrReplaceTempView("View_Locations")

# Define the name of the table containing park conditions
table_name = "locations"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(LocationID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.LocationID,
                            b.LocationName
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.LocationName) AS LocationID,
                            a.LocationName
                        FROM View_Locations a
                        LEFT JOIN View_table_db b ON trim(a.LocationName) = trim(b.LocationName)
                        WHERE b.LocationID IS NULL
            """

# Call the function to update park conditions
Locations = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, Locations)

Locations.createOrReplaceTempView("View_Locations")


# # Activities

# In[34]:


# Get unique activities and split them by commas
Activities = spark.sql("""
    SELECT EXPLODE(SPLIT(Activities, ', ')) AS ActivityName
    FROM View_squirrel_data
    WHERE Activities IS NOT NULL
""")

# Remove extra white spaces from activity names
Activities = Activities.withColumn("ActivityName", trim(Activities["ActivityName"]))

# Replace text within parentheses and the parentheses themselves with an empty string
Activities = Activities.withColumn('ActivityName', trim(regexp_replace('ActivityName', r'\(.*?\)', '')))

# Obtain unique activities
Activities = Activities.distinct()

# Sort and add a row number to the activities
window = Window.orderBy("ActivityName")
Activities = Activities.withColumn("ActivityID", F.row_number().over(window))

# Selecting the unique activity identifiers and their names
Activities = Activities.select("ActivityID", "ActivityName")


# ### Activities | Table Update

# In[35]:


# Create or replace a temporary view from the DataFrame 
Activities.createOrReplaceTempView("View_Activities")

# Define the name of the table containing park conditions
table_name = "activities"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(ActivityID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.ActivityID,
                            b.ActivityName
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.ActivityName) AS ActivityID,
                            a.ActivityName
                        FROM View_Activities a
                        LEFT JOIN View_table_db b ON trim(a.ActivityName) = trim(b.ActivityName)
                        WHERE b.ActivityID IS NULL
            """

# Call the function to update park conditions
Activities = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, Activities)

Activities.createOrReplaceTempView("View_Activities")


# # Interactions

# In[36]:


# Obtaining unique interactions and splitting them by commas
Interactions = spark.sql("""
    SELECT EXPLODE(SPLIT(InteractionswithHumans, ', ')) AS InteractionName
    FROM View_squirrel_data
    WHERE InteractionswithHumans IS NOT NULL
""")

# Removing additional whitespace and obtaining unique interactions
Interactions = Interactions.withColumn("InteractionName", trim(Interactions["InteractionName"]))

# Replacing text within parentheses and the parentheses themselves with an empty string
Interactions = Interactions.withColumn('InteractionName', trim(regexp_replace('InteractionName', r'\(.*?\)', '')))

# Obtaining unique interactions
Interactions = Interactions.distinct()

# Sorting and adding a row number to the interactions
window = Window.orderBy("InteractionName")
Interactions = Interactions.withColumn("InteractionID", F.row_number().over(window))

Interactions = Interactions.select("InteractionID", "InteractionName")


# ### Interactions | Table Update

# In[37]:


# Create or replace a temporary view from the DataFrame 
Interactions.createOrReplaceTempView("View_Interactions")

# Define the name of the table containing park conditions
table_name = "interactions"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(InteractionID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.InteractionID,
                            b.InteractionName
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.InteractionName) AS InteractionID,
                            a.InteractionName
                        FROM View_Interactions a
                        LEFT JOIN View_table_db b ON trim(a.InteractionName) = trim(b.InteractionName)
                        WHERE b.InteractionID IS NULL
            """

# Call the function to update park conditions
Interactions = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, Interactions)

Interactions.createOrReplaceTempView("View_Interactions")


# # Squirrels

# In[38]:


# Selecting squirrel data with joined color IDs
Squirrels = spark.sql("""
    SELECT a.SquirrelDataID, a.SquirrelID, a.ParkID,  b.ColorID as SquirrelPrimFurColorID, c.ColorID as SquirrelHighFurColorID, a.ColorNotes as SquirrelColorNotes, SpecificLocation as SquirrelSpecificLocation,
           OtherNotesorObservations as SquirrelObservations, a.SquirrelLatitude, a.SquirrelLongitude, a.Min_Height_Feet as SquirrelMinHeightFeet,
           a.Max_Height_Feet as SquirrelMaxHeightFeet
    FROM View_squirrel_data a LEFT JOIN View_Colors b on trim(a.PrimaryFurColor)      = trim(b.ColorName)
                              LEFT JOIN View_Colors c on trim(a.HighlightsinFurColor) = trim(c.ColorName)
""")


# ### Squirrels | Table Update

# In[39]:


# Create or replace a temporary view from the DataFrame 
Squirrels.createOrReplaceTempView("View_Squirrels")

# Define the name of the table containing park conditions
table_name = "squirrels"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(SquirrelDataID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.SquirrelDataID, 
                            b.SquirrelID, 
                            b.ParkID, 
                            b.SquirrelPrimFurColorID, 
                            b.SquirrelHighFurColorID, 
                            b.SquirrelColorNotes, 
                            b.SquirrelSpecificLocation, 
                            b.SquirrelObservations, 
                            b.SquirrelLatitude, 
                            b.SquirrelLongitude, 
                            b.SquirrelMinHeightFeet, 
                            b.SquirrelMaxHeightFeet
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.SquirrelDataID) AS SquirrelDataID,
                            a.SquirrelID, 
                            a.ParkID, 
                            a.SquirrelPrimFurColorID, 
                            a.SquirrelHighFurColorID, 
                            a.SquirrelColorNotes, 
                            a.SquirrelSpecificLocation, 
                            a.SquirrelObservations, 
                            a.SquirrelLatitude, 
                            a.SquirrelLongitude, 
                            a.SquirrelMinHeightFeet, 
                            a.SquirrelMaxHeightFeet
                        FROM View_Squirrels a
                        LEFT JOIN View_table_db b ON trim(a.SquirrelID)         = trim(b.SquirrelID)         AND
                                                     a.ParkID                   = b.ParkID                   AND   
                                                     a.SquirrelPrimFurColorID   = b.SquirrelPrimFurColorID   AND
                                                     a.SquirrelHighFurColorID   = b.SquirrelHighFurColorID   AND
                                                     a.SquirrelColorNotes       = b.SquirrelColorNotes       AND
                                                     a.SquirrelSpecificLocation = b.SquirrelSpecificLocation AND
                                                     a.SquirrelObservations     = b.SquirrelObservations     AND 
                                                     a.SquirrelLatitude         = b.SquirrelLatitude         AND
                                                     a.SquirrelLongitude        = b.SquirrelLongitude        AND
                                                     a.SquirrelMinHeightFeet    = b.SquirrelMinHeightFeet    AND
                                                     a.SquirrelMaxHeightFeet    = b.SquirrelMaxHeightFeet 
                        WHERE b.SquirrelID IS NULL
            """

# Call the function to update park conditions
Squirrels = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, Squirrels)

Squirrels.createOrReplaceTempView("View_Squirrels")


# # SquirrelHumanInteractions

# In[40]:


# Retrieving data from the view and exploding the InteractionswithHumans column
SquirrelHumanInteractions = spark.sql("""
    SELECT SquirrelDataID, ParkID, SquirrelID, explode(split(trim(InteractionswithHumans), ', ')) AS InteractionswithHumans
    FROM View_squirrel_data
""")

# Extracting text within parentheses into a new column
SquirrelHumanInteractions = SquirrelHumanInteractions.withColumn("SquirrelHumanInteractionObs", regexp_extract(col("InteractionswithHumans"), "\(([^)]*)\)", 1))

# Removing parentheses from the InteractionswithHumans column
SquirrelHumanInteractions = SquirrelHumanInteractions.withColumn("InteractionswithHumans", regexp_replace(col("InteractionswithHumans"), "\([^)]*\)", ""))

# Replacing "xxzxx" with commas in the SquirrelHumanInteractionObs column
SquirrelHumanInteractions = SquirrelHumanInteractions.withColumn("SquirrelHumanInteractionObs", regexp_replace(col("SquirrelHumanInteractionObs"), "xxzxx", ", "))

# Creating a new column "SquirrelHumanInteractionID" with unique identifiers starting from 1
SquirrelHumanInteractions = SquirrelHumanInteractions.withColumn("SquirrelHumanInteractionID", monotonically_increasing_id() + 1)

SquirrelHumanInteractions.createOrReplaceTempView("View_SquirrelHumanInteractions")

SquirrelHumanInteractions = spark.sql("""
    SELECT a.SquirrelHumanInteractionID, a.SquirrelDataID, a.SquirrelID, b.InteractionID, a.SquirrelHumanInteractionObs
    FROM View_SquirrelHumanInteractions a left join View_Interactions b on trim(a.InteractionswithHumans) = trim(b.InteractionName)
    ORDER BY a.SquirrelHumanInteractionID
""")


# ### SquirrelHumanInteractions | Table Update

# In[41]:


# Create or replace a temporary view from the DataFrame 
SquirrelHumanInteractions.createOrReplaceTempView("View_SquirrelHumanInteractions")

# Define the name of the table containing park conditions
table_name = "squirrelhumaninteractions"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(SquirrelHumanInteractionID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.SquirrelHumanInteractionID,
                            b.SquirrelDataID,
                            b.SquirrelID,
                            b.InteractionID,
                            b.SquirrelHumanInteractionObs
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.SquirrelID) AS SquirrelHumanInteractionID,
                            a.SquirrelDataID,
                            a.SquirrelID,
                            a.InteractionID,
                            a.SquirrelHumanInteractionObs
                        FROM View_SquirrelHumanInteractions a
                        LEFT JOIN View_table_db b ON trim(a.SquirrelID) = trim(b.SquirrelID) AND
                                                     a.SquirrelDataID   = b.SquirrelDataID   AND
                                                     a.SquirrelID       = b.SquirrelID       AND
                                                     a.InteractionID    = b.InteractionID    AND
                                                     trim(a.SquirrelHumanInteractionObs) = trim(b.SquirrelHumanInteractionObs)
                        WHERE b.SquirrelDataID IS NULL
            """

# Call the function to update park conditions
SquirrelHumanInteractions = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, SquirrelHumanInteractions)

SquirrelHumanInteractions.createOrReplaceTempView("SquirrelHumanInteractions")


# # SquirelLocations

# In[42]:


# Retrieving data from the view and exploding the Location column
SquirrelLocations = spark.sql("""
    SELECT SquirrelDataID, ParkID, SquirrelID, explode(split(trim(Location), ', ')) AS SquirrelLocations
    FROM View_squirrel_data
""")
# SquirrelLocations.show(20)

# Extracting text within parentheses into a new column
SquirrelLocations = SquirrelLocations.withColumn("SquirrelLocationObs", regexp_extract(col("SquirrelLocations"), "\(([^)]*)\)", 1))

# Removing parentheses from the Location column
SquirrelLocations = SquirrelLocations.withColumn("SquirrelLocations", regexp_replace(col("SquirrelLocations"), "\([^)]*\)", ""))
# SquirrelLocations.show(20)

# Replacing "xxzxx" with commas in the SquirrelLocationObs column
SquirrelLocations = SquirrelLocations.withColumn("SquirrelLocationObs", regexp_replace(col("SquirrelLocationObs"), "xxzxx", ", "))

# Creating a new column "SquirrelLocationID" with unique identifiers
SquirrelLocations = SquirrelLocations.withColumn("SquirrelLocationID", monotonically_increasing_id() + 1)

SquirrelLocations.createOrReplaceTempView("View_SquirrelLocations")

SquirrelLocations = spark.sql("""
    SELECT a.SquirrelLocationID, a.SquirrelDataID, b.LocationID, a.SquirrelLocationObs
    FROM View_SquirrelLocations a left join View_Locations b on trim(a.SquirrelLocations) = trim(b.LocationName)
""")


# ### SquirelLocations | Table Update

# In[43]:


# Create or replace a temporary view from the DataFrame 
SquirrelLocations.createOrReplaceTempView("View_SquirrelLocations")

# Define the name of the table containing park conditions
table_name = "squirrellocations"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(SquirrelLocationID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.SquirrelLocationID,
                            b.SquirrelDataID,
                            b.LocationID,
                            b.SquirrelLocationObs
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.SquirrelDataID) AS SquirrelLocationID,
                            a.SquirrelDataID,
                            a.LocationID,
                            a.SquirrelLocationObs
                        FROM View_SquirrelLocations a
                        LEFT JOIN View_table_db b ON a.SquirrelLocationID        = b.SquirrelDataID      AND
                                                     a.LocationID                = a.LocationID          AND
                                                     trim(a.SquirrelLocationObs) = trim(b.SquirrelLocationObs) 
                                                
                        WHERE b.SquirrelLocationID IS NULL
            """

# Call the function to update park conditions
SquirrelLocations = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, SquirrelLocations)

SquirrelLocations.createOrReplaceTempView("SquirrelLocations")


# # SquirrelActivities

# In[44]:


# Retrieving data from the view and exploding the Activities column
SquirrelActivities = spark.sql("""
    SELECT SquirrelDataID, ParkID, SquirrelID, explode(split(trim(Activities), ', ')) AS SquirrelActivities
    FROM View_squirrel_data
""")

# Extracting text within parentheses into a new column
SquirrelActivities = SquirrelActivities.withColumn("SquirrelActivityObs", regexp_extract(col("SquirrelActivities"), "\(([^)]*)\)", 1))

# Removing parentheses from the Activities column
SquirrelActivities = SquirrelActivities.withColumn("SquirrelActivities", regexp_replace(col("SquirrelActivities"), "\([^)]*\)", ""))


# Replacing "xxzxx" with commas in the SquirrelActivityObs column
SquirrelActivities = SquirrelActivities.withColumn("SquirrelActivityObs", regexp_replace(col("SquirrelActivityObs"), "xxzxx", ", "))

# Creating a new column "SquirrelActivityID" with unique identifiers
SquirrelActivities = SquirrelActivities.withColumn("SquirrelActivityID", monotonically_increasing_id() + 1)

SquirrelActivities.createOrReplaceTempView("View_SquirrelActivities")

SquirrelActivities = spark.sql("""
    SELECT a.SquirrelActivityID, a.SquirrelDataID, b.ActivityID, a.SquirrelActivityObs
    FROM View_SquirrelActivities a left join View_Activities b on trim(a.SquirrelActivities) = trim(b.ActivityName)
""")


# ### SquirrelActivities | Table Update

# In[45]:


# Create or replace a temporary view from the DataFrame 
SquirrelActivities.createOrReplaceTempView("View_SquirrelActivities")

# Define the name of the table containing park conditions
table_name = "squirrelactivities"

# SQL query to retrieve the maximum ParkConditionID from the database
max_cond_sql = """ SELECT COALESCE(MAX(SquirrelActivityID), 0) AS max_table_cond_id 
                   FROM View_table_db"""

# SQL query to identify common rows and add new ones if needed
common_rows_sql = """
                        SELECT
                            b.SquirrelActivityID,
                            b.SquirrelDataID,
                            b.ActivityID,
                            b.SquirrelActivityObs
                        FROM View_table_db b
                        UNION ALL
                        SELECT
                            {max_table_cond_id} + ROW_NUMBER() OVER (ORDER BY a.SquirrelDataID) AS SquirrelActivityID,
                            a.SquirrelDataID,
                            a.ActivityID,
                            a.SquirrelActivityObs
                        FROM View_SquirrelActivities a
                        LEFT JOIN View_table_db b ON a.SquirrelDataID            = b.SquirrelDataID      AND
                                                     a.ActivityID                = a.ActivityID          AND
                                                     trim(b.SquirrelActivityObs) = trim(b.SquirrelActivityObs) 
                                                
                        WHERE b.SquirrelActivityID IS NULL
            """

# Call the function to update park conditions
SquirrelActivities = update_table_db(spark, jdbc_url, connection_properties, table_name, max_cond_sql, common_rows_sql, SquirrelActivities)

SquirrelActivities.createOrReplaceTempView("SquirrelLocations")


# # Query

# ## 1. How many squirrels are there in each Park?

# In[46]:


# SQL query
query = """
    (SELECT q."ParkID", p."ParkName", COUNT(*) AS qty_squirrel
    FROM (SELECT "ParkID", "SquirrelID"
          FROM squirrels
          GROUP BY "ParkID", "SquirrelID"
          ORDER BY "ParkID") q
    LEFT JOIN parks p ON q."ParkID" = p."ParkID"
    GROUP BY q."ParkID", p."ParkName"
    ORDER BY q."ParkID") AS subquery
"""

# Reading data
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
df.show()

df.toPandas().to_csv('output/query_1.csv', index=False)


# ## 2. How many squirrels are there in each Borough?

# In[47]:


# SQL query
query = """
    (select q."AreaID", a."AreaName", count(*) as qty_squirrel
    from (select "AreaID", "SquirrelID"
          from squirrels s left join parks p on s."ParkID" = p."ParkID" 
          group by "AreaID", "SquirrelID" 
          order by "AreaID") q left join areas a on q."AreaID" = a."AreaID" 
    group by q."AreaID", a."AreaName"
    order by q."AreaID") AS subquery
"""

# Reading data
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
df.show()

df.toPandas().to_csv('output/query_2.csv', index=False)


# ## 3. A count of "Other Animal Sightings" by Park.

# In[48]:


# SQL query
query = """
    (select p."ParkName",  an."AnimalName", count(*) as "Total"
    from (select ps."ParkID", pa."AnimalID"
          from parkstatusanimals pa left join parkstatus ps on pa."ParkStatusID" = ps."ParkStatusID"
          order by ps."ParkID") q left join animals an on q."AnimalID" = an."AnimalID" 
                                  left join parks p on q."ParkID"::int = p."ParkID"
    group by q."ParkID", p."ParkName", q."AnimalID",  an."AnimalName" 
    order by q."ParkID", an."AnimalName") AS subquery
"""

# Reading data
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
df.show()

df.toPandas().to_csv('output/query_3.csv', index=False)


# ## 4. What is the most common activity for Squirrels? (e.g. eating, running, etc..)

# In[49]:


# SQL query
query = """
    (SELECT ROW_NUMBER() OVER(ORDER BY COUNT(*) DESC) AS "Top", 
    a."ActivityName" as "TopActivities"
    FROM squirrelactivities s
    LEFT JOIN activities a ON s."ActivityID" = a."ActivityID"
    GROUP BY s."ActivityID", a."ActivityName"
    ORDER BY COUNT(*) desc) AS subquery
"""

# Reading data
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
df.show()

df.toPandas().to_csv('output/query_4.csv', index=False)


# ## 5. A count of all Primary Fur Colors by Park.

# In[50]:


# SQL query
query = """
    (select s."ParkID",  p."ParkName", c."ColorName" as "PrimaryFurColors", count(*) as "Total"
    from squirrels s left join colors c on s."SquirrelPrimFurColorID" = c."ColorID" 
                     left join parks p  on s."ParkID" = p."ParkID" 
    group by s."ParkID", s."SquirrelPrimFurColorID", c."ColorName", p."ParkName"
    order by s."ParkID", count(*), c."ColorName") AS subquery
    """

# Reading data
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
df.show()

df.toPandas().to_csv('output/query_5.csv', index=False)

