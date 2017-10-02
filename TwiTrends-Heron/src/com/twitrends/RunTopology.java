package com.twitrends;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.utils.Utils;

import com.twitrends.topology.TwiTrendsTopology;
import com.twitrends.util.TopologyIdentifiers;


/**
 * The main class of the application. Creates a topology and submits it
 * to a local storm cluster.
 *
 * @author Andrei Moldovan
 */
public class RunTopology {

    public static void main(String[] args) throws InvalidTopologyException, AlreadyAliveException, NotAliveException {

        TwiTrendsTopology twiTrendsTopology = new TwiTrendsTopology();
        // create the default config object
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, true);
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            // run it in a live cluster
            conf.setNumWorkers(4);
            StormSubmitter.submitTopology(args[0], conf, twiTrendsTopology.createTopology());
        } else {
            // run it in a simulated local cluster
            // set the number of threads to run - similar to setting number of
            // workers in live cluster
            conf.setMaxTaskParallelism(4);

            // create the local cluster instance.
            LocalCluster cluster = new LocalCluster();

            // create and submit the topology to the local cluster.
            cluster.submitTopology(TopologyIdentifiers.TOPOLOGY_NAME, conf, twiTrendsTopology.createTopology());

            // run topology for 30 minutes, then kill.
            Utils.sleep(30 * 60 * 1000);
            cluster.killTopology(TopologyIdentifiers.TOPOLOGY_NAME);

            // shutdown cluster.
            cluster.shutdown();
        }
    }
}
