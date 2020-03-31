import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import bolts.EntitiesBolt;
import bolts.FileWriterBolt;
import bolts.HereBolt;
import bolts.MongoBolt;
import spouts.TwitterSpout;
import twitter4j.FilterQuery;

import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;


public class TwitterTopology {

    /* Configure you Twitter API access credentials */
    private static String consumerKey = "*****************"; // Known as API Key
    private static String consumerSecret = "*******************"; // Known as API secret key
    private static String accessToken = "******************";
    private static String accessTokenSecret = "********************";

    public static void main(String[] args) throws Exception {

        LogManager.getLogManager().getLogger(Logger.GLOBAL_LOGGER_NAME).setLevel(Level.INFO);

        /**************** SETUP START ****************/
        String remoteClusterTopologyName = null;
        if (args!=null) {
            if (args.length==1) {
                remoteClusterTopologyName = args[0];
            }
            // If credentials are provided as commandline arguments
            else if (args.length==4) {
                consumerKey =args[0];
                consumerSecret =args[1];
                accessToken =args[2];
                accessTokenSecret =args[3];
            }

        }
        /**************** SETUP END ****************/

        /* TODO: Define your own twitter query */
        FilterQuery tweetFilterQuery = new FilterQuery();
        final FilterQuery track = tweetFilterQuery.track(new String[]{"madrid"});
        /* Get details about filtering options: https://github.com/kantega/storm-twitter-workshop/wiki/Basic-Twitter-stream-reading-using-Twitter4j */


        TopologyBuilder builder = new TopologyBuilder();




        TwitterSpout spout = new TwitterSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, tweetFilterQuery);

        builder.setSpout("spout", spout,1);


        Config conf = new Config();
        conf.setDebug(false);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("twitter-fun", conf, builder.createTopology());

            Thread.sleep(460000);

            cluster.shutdown();
        }
    }
}
