# cassandra-itrigger-issue

```console
mvn clean package

docker-compose up -d

docker logs -f cassandra | grep 'Startup complete'
```
Once started,

```console
./test.sh

docker logs -f cassandra
```

Observe the exception:
ERROR [pool-7-thread-1] 2021-10-26 15:22:21,430 CassandraDaemon.java:579 - Exception in thread Thread[pool-7-thread-1,5,main]
org.apache.cassandra.serializers.MarshalException: Unexpected extraneous bytes after map value
	at org.apache.cassandra.serializers.MapSerializer.deserializeForNativeProtocol(MapSerializer.java:137)
	at org.apache.cassandra.serializers.MapSerializer.deserializeForNativeProtocol(MapSerializer.java:36)

