#This script downloads an archived access log file from the url

#Then extracts the access log file

#Then the first 4 columns are extracted

#The data is then transformed by replacing # with "," and saved t a csv format

#Finally it is loaded to a pre-created postgres table

# Download the access log file

wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/web-server-access-log.txt.gz"
echo "File successfully Downloaded"


# Unzip the file to extract the .txt file.
gunzip -f web-server-access-log.txt.gz
echo "File successfully Extracted"

#Extract the first 4 columns and save into extracted-data.txt
cut -d "#" -f1-4 web-server-access-log.txt > extracted-data.txt
echo "Data successfully Extracted"

#transform by replacing the delimiter "#" with "," to make it a csv
tr "#" "," < extracted-data.txt > transformed-data.csv
echo "Data successfully Transformed"

#Load the data into the access_log table in PostgreSQL database named "template1"
echo "\c template1;\COPY access_log  FROM '/home/project/transformed-data.csv' DELIMITERS ',' CSV HEADER;" | psql --username=postgres --host=localhost
echo "Data successfully loaded into database"