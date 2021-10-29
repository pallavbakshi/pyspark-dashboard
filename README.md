# pyspark-dashboard

Time taken ~ 5 hours

This is solution to data analysis assignment which involves using PySpark to clean, process and transform data; and visualize the data on Google Data Studio.

Hi,

To run code, you'll need:
1. Virtual env with all the required dependencies ~ `requirements.txt`
2. A folder containing both CSV files -- ad_stream.csv and store_stream.csv
3. A folder to put the results of the script into
4. `python transform_data.py --ad_stream_path "./data/ad_stream.csv" --store_stream_path "./data/store_stream.csv" --path_to_store_data ./data/`
5. Manually upload the transformed data into Google Drive

Tested with python 3.8.12


There is a file named `transform_data.py` which takes in as argument the location (on your storage) of both the csv files.
# How to Check Out Visualizations
I have used Google Data Studio to visualize the metrics. Google Data Studio provides you with easy integrations with a lot of tools that are used in business ~ GA, YouTube analytics, etc. And it's a free tool unlike PowerBI.
I didn't go for something like Plotly and Dash because those tools make it harder to share access with business leaders. Most of the business leaders don't know how to ssh into a machine to look at graphs. With Plotly and Dash, I would also need to worry about security, hosting and maintenance. Therefore, I went for some out-of-the-box visualization tool. It definitely restricted the things I can do with visualization, however, for KPIs and metrics DataStudio seems enough powerful for now.

You can checkout the video zipped in the folder (bad quality because of limited file size). Or you can look at the video in HD at the follower url:
https://drive.google.com/drive/folders/1PuHLXA_33J3LjGo2j9n1IQRhDbUjrJiF?usp=sharing

If you want to play with an interactive version of the dashboard, you can do that here:
https://datastudio.google.com/reporting/5b9fca7e-1e09-406c-b005-6bb1db02ea25

# Definition of Metrics

Monthly Active User: Number of device_ids who took some meaningful action at least once within the month (1-31d).
Daily Active User: Number of device_ids who took some meaningful action at least once within a day (0-23h)

### How to define meaningful action?  
We're defining two types of events -- engaging events and unengaging events.
engaging events require user's attention and effort. These events are used to
define active users. Example -- click on an ad, if a user clicks on an ad, then
we can be sure that the user has at least seen the ad and has taken an action.

unengaging events are often system generated or don't require any action on
user's behalf. Example -- displaying of an ad on your mobile app, just display
of an ad doesn't guarantee that the user has actually seen the ad.

There are 5 types of ad events:
engaging events -- click, close
unengaging events -- logs, display, load

There are 5 types of app store events:
engaging events -- app_view, open, download
unengaging events -- logs

## How to check out output of scripts
You can either run the scripts or directly check the output here:
https://drive.google.com/drive/folders/1PuHLXA_33J3LjGo2j9n1IQRhDbUjrJiF?usp=sharing

# Scope For Future Work
1. Containerize the entire application so that it is easier to build and deploy.
2. Write tests, use types, mypy, flake8, etc. to make code production-grade.
3. Create a Python package out of this to make it easier to scale this project.
4. Create a CI/CD pipeline on Github to make it easier to deploy changes.
5. Create PySpark pipeline which handles everything from fetching data to uploading it on Google Data Studio.
6. Use Apache Airflow to automate the pipeline and triggers.
7. Deploy this app on AWS so that it can actually use Spark workers effectively.
8. Versioning of pipelines and data with something like Singer Taps and Targets.
9. And many more things!

