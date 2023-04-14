# quick explanation of how to run program and tests

## 1. open a terminal and build the project:

`ant && ant build-ecs-jar`

## 2. start ecs:

`java -jar m2-ecs.jar 127.0.0.1 2181`

## 3. open another terminal and run this command:

`zookeeper-3.4.11/bin/zkCli.sh`

do `ls /` and make sure `server_status` exists.

if not run: `create /server_status a`.

if u ever need to delete it for some reason, run `rmr /server_status`.

exit the cli tool once you have confirmed the above.

## 4. open 3 more terminals and start 1 server in each:

`java -jar m2-server.jar 127.0.0.1 8081 10000 i 127.0.0.1 2181`

`java -jar m2-server.jar 127.0.0.1 8082 10000 i 127.0.0.1 2181`

`java -jar m2-server.jar 127.0.0.1 8083 10000 i 127.0.0.1 2181`

`java -jar m2-server.jar 127.0.0.1 8084 10000 i 127.0.0.1 2181` (not needed for tests)

## 5. if running tests, open another terminal and enter the following command:

`ant test`

## 6. to start the client side, open as many terminals as needed and run the client jar. the below is for 2 clients:

`java -jar m2-client.jar 8085`

`java -jar m2-client.jar 8086`


## NOTE:

before running the tests, make sure ecs and the servers are down (take down servers first then ecs).

then make sure that the servers' database properties files are deleted.

then build the project.

then start ecs and the servers (in that order).

now run the tests.