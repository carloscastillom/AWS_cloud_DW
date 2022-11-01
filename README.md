# Data Warehouse Redshift Sparkify

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


# How to run
- Fill out the config file dwh.cfg. the information must completed with your own account details.
- run create_tables.py.
- Open up create_cluster.ipynb and follow the intructions in the jupyter notebook.
- run etl.py
- Do not forget to delete the redshift cluster

 explanation of the files in the repository. Comments are used effectively and each function has a docstring.

# Files
- dwh.cfg it is the configuration file
- create_tables.py python script which creates the tables 
- etl.py python script which extracts the data from S3, tranform the tables and load them in Redshift.
- sql_queries.py file that works as an input for create_tables.py and sql_queries.py. It contains the SQL queries that are used in these files. 
