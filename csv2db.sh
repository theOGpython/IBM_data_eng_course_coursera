#This script extracts data from /etc/passwd file into a csv file

#The csv data file contains the username, user id and 
#home dir of each acct defined in /etc/passwd

#Tranforms the text delimeter from ":" t ","
#Loads the data from the csv into a table in a postgres database

#Extract phase

echo "Extracting data"

# Extract the columns 1 (user name), 2 (user id) and 
# 6 (home directory path) from /etc/passwd and save to a txt file

cut -d":" -f1,3,6 /etc/passwd > extracted-data.txt

# Transform phase

echo "Transforming data"

# read the extracted data and replace the colons with commas to convert to csv.

tr ":" "," < extracted-data.txt > transformed-data.csv

# Load phase

echo "Loading data"

# Send the instructions to connect to 'template1' and
# copy the file to the table 'users' through command pipeline.

#Using syntax--> COPY table_name FROM 'filename' DELIMITERS 'delimiter_character' FORMAT;

echo "\c template1;\COPY users  FROM '/home/project/transformed-data.csv' DELIMITERS ',' CSV;" | psql --username=postgres --host=localhost

