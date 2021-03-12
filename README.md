# Code Dependencies
 * maven 3.6+
 * java and javac 10+

# Build
## Compiling the code
    mvn clean install

## Copy the built jar to project directory
This is needed since we want to ensure the `config.properties` file is in the same location as the jar.

    cp target/cse223_f2021-1.0-SNAPSHOT-jar-with-dependencies.jar ./


# Configuration for execution
## Update config.properties
    cat config.properties 
        db0=db1.caezowu6vchx.us-east-1.rds.amazonaws.com/
        db1=db2.caezowu6vchx.us-east-1.rds.amazonaws.com/
        db2=db3.caezowu6vchx.us-east-1.rds.amazonaws.com/
        db3=db4.caezowu6vchx.us-east-1.rds.amazonaws.com/

        0=35000
        1=35001
        2=35002
        3=35003
        
        user=postgres
        password=postgres223
        
        max_txn=3

`db1`, `db2`, `db3`, `db4`, need to be 4 database servers where postgres instance is running.
`0,1,2,3` are the ports of the coordinator and the 3 cohorts respectively.
All database instances have the same username and password, specified by the last 2 lines.

To specify the number of queries for a particular transaction set the `max_txn` value to some specific number. At present the value is set to 3. This means that a transaction will have 3 queries. 

## Specific configuration for postgres server instances. 
All Postgres DB instances must have the flag `max_prepared_transactions` uncommented and set to 100. Without this the 2PC functions of postgres will not be triggered. 
To set this value modify the `postgresql.conf` file and set the `max_prepared_transactions` flag to 200. The file is located on a linux machine at `/etc/postgresql/12/main/postgresql.conf`.
Postgres server needs to be restarted after this flag is set.

If Postgres is setup on AWS RDS (as in our case) each instance of the DB server must be assigned a property group with the `max_prepared_transactions` set to 200.

# Running the Coordinator and Cohorts
## Coordinator
`java -cp cse223_f2021-1.0-SNAPSHOT-jar-with-dependencies.jar edu.uci.ics.cse223.CoordinatorServer`

## Cohorts
### Cohort 1
`java -cp cse223_f2021-1.0-SNAPSHOT-jar-with-dependencies.jar edu.uci.ics.cse223.CohortServer 1`
### Cohort 2
`java -cp cse223_f2021-1.0-SNAPSHOT-jar-with-dependencies.jar edu.uci.ics.cse223.CohortServer 2`
### Cohort 3
`java -cp cse223_f2021-1.0-SNAPSHOT-jar-with-dependencies.jar edu.uci.ics.cse223.CohortServer 3`

## Transaction Client
We created a transaction client to parse the file containing the SQL statements. 
To execute the transaction run the following command:

    java -cp cse223_f2021-1.0-SNAPSHOT-jar-with-dependencies.jar edu.uci.ics.cse223.TransactionClient ./tempqueries.sql