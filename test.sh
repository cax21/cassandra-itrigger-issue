#!/bin/bash

echo "creating keyspace 'test'"
docker exec -it cassandra cqlsh -e "create keyspace if not exists test with replication = { 'class': 'NetworkTopologyStrategy', 'DC1': 1};"

echo "creating table 'alarms'"
docker exec -it cassandra cqlsh -e "create table if not exists test.timestamps (
							id uuid,
							stamps map<text,bigint>,
              primary key(id));"

echo "creating trigger"
docker exec -it cassandra cqlsh -e "create trigger if not exists my_trigger on test.timestamps using 'cassandra.issues.trigger.MyTrigger';"

echo "inserting a new timestamp"
docker exec -it cassandra cqlsh -e "insert into test.timestamps (id,stamps) values (uuid(),{'produced':123488492, 'consumed':987654321});"

