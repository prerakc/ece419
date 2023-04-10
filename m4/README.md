# quick explanation of how to run program and tests

## 1. start ecs in a terminal:

`java -jar m2-ecs.jar 127.0.0.1 2181`

## 2. open another terminal and run this command:

`zookeeper-3.4.11/bin/zkCli.sh`

do `ls /` and make sure `server_status` exists.

if not run: `create /server_status a`.

if u ever need to delete it for some reason, run `rmr /server_status`.

## 3. open 4 more terminals and start 1 server in each:

`java -jar m2-server.jar 127.0.0.1 8081 10000 i 127.0.0.1 2181`

`java -jar m2-server.jar 127.0.0.1 8082 10000 i 127.0.0.1 2181`

`java -jar m2-server.jar 127.0.0.1 8083 10000 i 127.0.0.1 2181`

`java -jar m2-server.jar 127.0.0.1 8084 10000 i 127.0.0.1 2181` (optional for tests)

## 4. open one last terminal. use this to build the project and run tests:

`ant && ant build-ecs-jar`

`ant test`

## 5. with two open terminals, run the clients as follows:

`java -jar m2-client.jar 8085`

`java -jar m2-client.jar 8086`


## NOTE:

before running the tests, make sure ecs and the servers are down (take down servers first then ecs).

then make sure that the servers' database properties files are deleted.

then build the project.

then start ecs and the servers (in that order).

now run the tests.