##Data sources

####GAUL
URL: http://www.fao.org/geonetwork/srv/en/metadata.show?id=12691
License: "FAO grants a license to use, download and print the materials contained in the GAUL dataset solely for non-commercial purposes and in accordance with the conditions specified in the data license. The full GAUL Data License document is available for downloading."
Login: Username:  m_westra / Password: gaul4Akvo
Login location: http://www.fao.org/geonetwork/srv/en/main.home, login and search for GAUL.
Formal permission was received on 9 December 2013 from Fabio Grita (fabio.grita@fao.org) to use the GAUL database.

####GADM
URL: http://www.gadm.org/
License: "These data are freely available for academic and other non-commercial use. Redistribution, or commercial use, is not allowed without prior permission."
No response was received on a request for permission for usage, send on 8 December 2013 to Robert Heijmans of UC Davis (xrhijmans@ucdavis.edu).

##Preparing the data
The GAUL and GADM data sets are retrieved as shapefiles, which need to be transformed into sql files for upload to Postgres.

The GAUL data set consists of shapefiles on admin level 0, 1 and 2, plus a set of countries for which more fine grained data is available. Sql files need to be made for all relevant shapefiles. Of the top level sets, we only use level 2. 

The GADM data set consists of a single shapefile.

Both datasets need to be in different tables, because the naming conventions differ.

1. Open Terminal and go to the directory holding a GAUL shapefile
2. Use shp2pgsql to transform the shapefile into a postgres sql file
    $$ shp2pgsql -s 4326 -W "latin1" SHAPEFILE SCHEMA.TABLE > SQL-FILENAME.sql

where 4326 is the SRID for WGS84, SHAPEFILE is the name of the shapefile without the .shp, SCHEMA is the database schema, TABLE is the table that will hold the data, and SQL-FILENAME is the name for the sql file to be produced.

For example:
    $$ shp2pgsql -s 4326 -W "latin1" G2013_2012_2 public.GAUL > GAUL2013.sql    


2. upload the resulting SQL-FILENAME.sql, using 
    $$ psql -h HOST -p PORT -d DATABASE -U USER -f SQL-FILENAME.sql

where HOST is the hostname where the postgres server is running, PORT is the port, DATABASE is the name of the database, USER is the user of the database, and SQL-FILENAME is the filename of the sql file created in the previous step.

For example:
    $$ psql -h localhost -p 5432 -d shapes -U markwestra -f GAUL2013.sql     


## Preparing the database
First we need to create the tables and make them geo-aware
    $$ psql -d postgres
    # CREATE DATABASE shapes;
    # \c shapes    => You are now connected to database "shapes" as a user.
    # create extension postgis;

####Uploading the data
Upload the data using psql as shown above

####Creating the index
After uploading the data, an index needs to be made for each table.
    CREATE INDEX tablename_gix ON tablename USING GIST (geom);
    VACUUM ANALYZE tablename;

For example:
    CREATE INDEX GAUL_gix ON GAUL USING GIST (geom);
    VACUUM ANALYZE GAUL;

This needs to be done for both GAUL and GADM tables.

##Doing queries
A query checking if a point falls within a geometry can be done like this:
    select name_0,name_1,name_2,name_3,name_4,name_5 from shapes where st_contains(shapes.geom,ST_GeomFromText('POINT(78.736817 22.350076)',4326))=true;