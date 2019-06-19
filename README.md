# ETLPipeLine
AN ETL test assignment
Answers to the questions in the assignment:


During the ETL pipeline, please focus on the following points:
1.	Add a weekly low, high, median, average for every record.

implementation question.


2.	Think of bad data that might break the ETL pipeline and add relevant defense mechanisms against it.
answer:
Possible data that may break the ETL pipeline can be types mismatch, for example, if the date column contains a value that is not in the structure of a date ( ‘dd-mm-yyyy’ ), or non decimal values in the CBETHUSD column. in my script I simply tried to validate the variable type, by creating a new one with the data that is extracted from the csv file, and raised an exception in case the data does not match the desired data type. However, since raising exceptions is not the most optimal approach performance wise, in a real ETL pipeline I would look for a more efficient method. 

3.	Think of bad data that can’t be fixed and build a monitoring ecosystem around it.

answer:
In my solution I ignored rows with corrupt and unfixable data, in order to ensure the correct execution of the script. 

4.	Think of a change of granularity and implement the corresponding calculations (instead of daily data, you will receive hourly data)

answer:
In case of change in granularity, assuming that the ETL pipeline will run on a hourly basis instead of a daily basis, and will handle possibly significantly larger volumes of data, I would try to make a greater emphasize on the efficiency of the ETL execution, by reducing the space and time complexity. For example, I would store the csv file extracted data in a NumPy array instead of a list, because NumPy arrays consume less memory and have a better runtime behavior. I would consider also to use more efficient methods of weekly calculations, perhaps make my own implementations instead of relying on the NumPy library’s methods.


5.	Think of a deployment pipeline. How would you update your ETL pipeline?

