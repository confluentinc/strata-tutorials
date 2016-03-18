# strata-tutorials

Content for Spring 2016 Strata tutorials
----------------------------------------

This repo contains content for the advnaced strata tutorial (Kafka Connect, Kafka Streams, Security).

Basic Plan
----------
We'll coach users through the following:
* Start a Kafka cluster (and Connect cluster) with Vagrant, create and configure topics for our use cases (TBD: should we set up some security rules at this time, including some quotas?))
* Use Kafka Connect to read in the contents of a database (in our case, a Sqlite database with NY Taxi trips)
* Coach users through some data manipulation with streams: reverse-geocoding coordinates in the data, joining with weather, and some aggregation
* Write out the data into something easy (file? elastic? we'll figure it out later)
* Talk a little more about security

Getting the data
----------------

Repository with materials fo the Advanced Kafka Tutorial. Current directions to get the source data:

* Download the [NY yellow cab data](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) from December 2015
* Uncompress the file
* Run the script ```clean.bash``` to strip out the header (note that it just contains a little awk, so you don't really need a script).
* Install a recent version of [sqlite](https://www.sqlite.org/download.html). (I did ```brew install sqlite``` on my mac.)
* From a shell, run ```sqlite3 yellowCabDataDec2015.db``` to create the database and get a shell 
* Run the contents of the ```taxis.sql``` script to create the taxi data (and import the weather data).

I got the weather data from ftp://ftp.ncdc.noaa.gov/pub/data/noaa/725053-94728-2015.gz. To make it readable, you need to do some
pre-processing. You can find a Java program in ftp://ftp.ncdc.noaa.gov/pub/data/noaa/ to process the contents of this file and 
turn it into CSV. I loaded that content into Excel and did some manual manipulations (I dropped some bad columns, changed the
timestamps to UNIX timestamps, and exported into CSV). You probably don't have to recreate this file (it's small, so I just
dropped it in the data folder).


Neighborhood files
==================
You can get these from http://www.zillow.com/howto/api/neighborhood-boundaries.htm