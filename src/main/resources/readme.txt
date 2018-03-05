
1) main class:
StockDataProcessingDriver

2) Please set the corresponding values in config.properties file before running the StockDataProcessingDriver.

3) For integration tests please run StockDataProcessingDriverTest class.
   you need to set corresponding values in testconfig.properties file for kafka as the integration tests are not based on embedded zookeeper and kafka.

4) PLease create the below table in mysql database for report viewing.

create table report(
   Hour TINYINT NOT NULL ,
   Stock VARCHAR(50) NOT NULL,
   Min DECIMAL(10,6) NOT NULL,
   Max DECIMAL(10,6),
   Volume INT(11),
   PRIMARY KEY ( Stock,Hour )
);

5) For viewing only latest updates please set outputMode=jdbc

6) jdbc mode will maintain hourly updates in real time.

7) For viewing the complete change log of set the outputMode=kafka


