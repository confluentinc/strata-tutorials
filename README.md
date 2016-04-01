# strata-tutorials

Content for Spring 2016 Strata tutorials
----------------------------------------

This repo contains content for the "Building Scalable Data Pipelines 
With Kafka" tutorial (Kafka Connect, Kafka Streams, Security) at Strata
San Jose 2016

Getting Started:
================
We use [Vagrant](https://www.vagrantup.com) to help manage virtual
machines during development and [VirtualBox](https://www.virtualbox.org)
for running VMs. We use git for version control, maven to build, and
Oracle JDK 7+ for development. If you have this stuff installed, you 
can get started with a set of commands like this:

    git clone https://github.com/confluentinc/strata-tutorials.git
    cd strata-tutorials
    mvn assembly:assembly
    vagrant up

You can connect to the VM by typing

    vagrant ssh
    
and then run other commands.  Within the VM, you need to start some
services. To do this, ssh to the VM then type

    /vagrant/scripts/setup.sh
    /vagrant/scripts/start.sh
    
This will install dependencies and start services (Kafka, Hadoop, etc).

The exercise:
=============
Edit the files `etc/sqlite-taxis.properties` and `/etc/hdfs.properties`
for the Kafka Connect tutorial. (Solutions are in 
`etc/sqlite-taxis-solutions.properties` and 
`/etc/hdfs-solutions.properties`.) 

Edit the file `src/main/java/io/confluent/strata/TaxiStream.java` for
the Kafka Streams tutorial. (Solution is in 
`src/main/java/io/confluent/strata/TaxiStreamSolution.java`.)

To start connect, type

    bash /vagrant/scripts/start-connect.bash
    
Or if you just want to see the solution (or start on the Streams
exercise), type:

    bash /vagrant/scripts/start-connect-solution.bash
    
To run the streams job run

    bash /vagrant/stream-test.bash
    
or to run the solution

    bash /vagrant/stream-test-solution.bash
    
To inspect the geocoded taxi rides
    
    bash /vagrant/show-geocoded-rides.bash
    
To inspect the counts

    bash /show-county-by-day.bash
    
 (Yeah, I know there's a typo. I left it in for funsies.)

Where the data comes from (and how to load it yourself)
-------------------------------------------------------
If you want to rebuild the data, run the script in 
```scripts/get_taxi_rides.bash```. This will download the taxi data, 
clean it, load it into a sqlite database, sample 100k random taxi rides,
and shrink  the DB. You can hack this file to include more data.

But there is a small (250k) sample of lines from that database in the
repo already. You can just use that one.

Running from your host machine
==============================
If you prefer to edit and compile on your host machine, you can do that!
Vagrant maps the project directory from your host machine onto the vm 
under ```/vagrant```. So just use execute the jar in
```/vagrant/target```.


Building and running things
===========================
We assume you have the following tools installed:
* JDK 7+
* Maven
* Git
* sqlite

On the host machine:

    mvn assembly:assembly
    vagrant up
    vagrant ssh
    
Then on the vm

    scripts/setup.sh
    scripts/start.sh
    bash /vagrant/scripts/start-connect.bash
    bash /vagrant/scripts/stream-test.bash
        
To view stream output

    # you can run hive, look at tables, and select data
    # or you can look at data in topics
    bash /home/vagrant/scripts/show_topic.bash geocodedRides


How I got the data
==================

Here's what I did (and what the scripts above does)

* Download the [NY yellow cab data](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) 
from December 2015
* Uncompress the file
* Run the script ```clean.bash``` to strip out the header (note that
 it just contains a little awk, so you don't really need a script).
* Install a recent version of [sqlite](https://www.sqlite.org/download.html).
 (I did ```brew install sqlite``` on my mac.)
* From a shell, run ```sqlite3 yellowCabDataDec2015.db``` to create the
 database and get a shell 
* Run the contents of the ```taxis.sql``` script to create the taxi 
data (and import the weather data).

I got the weather data from ftp://ftp.ncdc.noaa.gov/pub/data/noaa/725053-94728-2015.gz. 
(We don't use the weather data now, but if you want to learn streams
better, you might want to join to this.) To make it readable, you need 
to do some pre-processing. You can find a Java program in
ftp://ftp.ncdc.noaa.gov/pub/data/noaa/ to process the contents of this
file and turn it into CSV. I loaded that content into Excel and did 
some manual manipulations (I dropped some bad columns, changed the
timestamps to UNIX timestamps, and exported into CSV). You probably 
don't have to recreate this file (it's small, so I just  dropped it in
the data folder).


Neighborhood files
==================
You can get these from http://www.zillow.com/howto/api/neighborhood-boundaries.htm
