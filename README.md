# strata-tutorials
Content for Spring 2016 Strata tutorials
----------------------------------------

Repository with materials fo the Advanced Kafka Tutorial. Current diretions:

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

