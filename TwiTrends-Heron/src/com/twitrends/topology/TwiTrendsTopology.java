package com.twitrends.topology;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.twitrends.bolt.CountHashtagBolt;
import com.twitrends.bolt.CountLocationHashtagBolt;
import com.twitrends.bolt.GeoLocationBolt;
import com.twitrends.bolt.ParseTweetBolt;
import com.twitrends.bolt.RedisBolt;
import com.twitrends.bolt.TweetFilterBolt;
import com.twitrends.bolt.apache.IntermediateRankingsBolt;
import com.twitrends.bolt.apache.TotalRankingsBolt;
import com.twitrends.spout.TweetSpout;
import com.twitrends.tools.geo.ConcurrentLocationCache;
import com.twitrends.util.Constants;
import com.twitrends.util.FieldNames;
import com.twitrends.util.TopologyIdentifiers;
import com.twitrends.util.TwitterCredentials;

public class TwiTrendsTopology {

	TopologyBuilder builder;
	//Debug purpose
	ConcurrentLocationCache cache;
	

	public TwiTrendsTopology() {
		builder = new TopologyBuilder();
		initializeTopology();
	}
	
	/**
	 * Sets the spouts and bolts of the TwiTrends Topology
	 */
	private void initializeTopology() {

		TweetSpout tweetSpout = new TweetSpout(
				TwitterCredentials.KEY,
				TwitterCredentials.SECRET,
				TwitterCredentials.TOKEN,
				TwitterCredentials.TOKEN_SECRET);
	
		// Initialize Cache
		cache = ConcurrentLocationCache.getInstantce();

		// Set Spouts
		builder.setSpout(TopologyIdentifiers.TWEET_SPOUT, tweetSpout, 1);
		
		// Set Bolts..
		builder.setBolt(TopologyIdentifiers.TWEET_FILTER_BOLT, new TweetFilterBolt(), 2)
			.shuffleGrouping(TopologyIdentifiers.TWEET_SPOUT);
		builder.setBolt(TopologyIdentifiers.PARSE_TWEET_BOLT, new ParseTweetBolt(), 4)
			.shuffleGrouping(TopologyIdentifiers.TWEET_FILTER_BOLT);
		builder.setBolt(TopologyIdentifiers.COUNT_BOLT, new CountHashtagBolt(), 2)
			.fieldsGrouping(TopologyIdentifiers.PARSE_TWEET_BOLT, new Fields(FieldNames.HASHTAG));
		
		// Ranking Bolts
		builder.setBolt(TopologyIdentifiers.INTERMEDIATE_RANKER_BOLT, new IntermediateRankingsBolt(Constants.TOP_N), 4)
			.fieldsGrouping(TopologyIdentifiers.COUNT_BOLT, new Fields(FieldNames.HASHTAG));
		builder.setBolt(TopologyIdentifiers.TOTAL_RANKER_BOLT, new TotalRankingsBolt(Constants.TOP_N), 1)
			.globalGrouping(TopologyIdentifiers.INTERMEDIATE_RANKER_BOLT);
		
		// Geolocation Bolts
		builder.setBolt(TopologyIdentifiers.GEOLOCATION_BOLT, new GeoLocationBolt(), 4)
			.shuffleGrouping(TopologyIdentifiers.PARSE_TWEET_BOLT);
		builder.setBolt(TopologyIdentifiers.COUNT_LOCATION_HASHTAG_BOLT, new CountLocationHashtagBolt(), 2)
			.fieldsGrouping(TopologyIdentifiers.GEOLOCATION_BOLT, new Fields(FieldNames.LOCATION));

		// Global Redis Bolt
		builder.setBolt(TopologyIdentifiers.REDIS_BOLT, new RedisBolt(), 1)
			.globalGrouping(TopologyIdentifiers.TOTAL_RANKER_BOLT)
			.globalGrouping(TopologyIdentifiers.COUNT_LOCATION_HASHTAG_BOLT);
	}
	
	/**
	 * Creates and returns a Storm Topology for the TwiTrends Topology
	 */
	public StormTopology createTopology() {
		return builder.createTopology();
	}
}
