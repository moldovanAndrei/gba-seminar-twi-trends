# gba-seminar-twi-trends
Demo application for Apache Storm and Heron used for GBA Seminar 2017

Building and running in local mode:

1. Insert your Twitter/Bing credentials in com.twitrends.util.TwitterCredentials
2. Build the project (mvn install)
3. Run com.twitrends.RunTopology to test that the app is running in local mode

Running on a local cluster:

1. Install ZooKeeper (I used version 3.4.6).
2. Start the zookeeper server (zkServer.sh): bin/zkServer.sh start 
3. Start the zookeeper cli (zkCli.sh)
4. In the zookeeper cli create an empty zk node for your heron topologies with path /heron/topologies
5. Install Heron by following https://twitter.github.io/heron/docs/getting-started/ (Compatible version is 0.14.0)
6. Deploy your jar-with-dependencies and activate the topology
7. Start heron tracker and heron ui
8. (Optional) If you want to also publish your trending topics to redis, install it and configure it’s port to 6388

<br> ! Make sure JAVA_HOME is set! (ECHO $JAVA_HOME)
<br> ! Make sure you have write rights (worst case use sudo).
