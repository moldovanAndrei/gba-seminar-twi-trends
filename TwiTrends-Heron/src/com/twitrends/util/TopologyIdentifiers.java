package com.twitrends.util;

/**
 * Constants used as Identifier inside the TwiTrend Topology.
 * 
 * @author Andrei Moldovan.
 */
public interface TopologyIdentifiers {
	
	final String TOPOLOGY_NAME = "twi-trends-topology";
	final String TWEET_SPOUT = "tweet-spout";
	final String REDIS_BOLT = "reddis-bolt";
	final String PARSE_TWEET_BOLT = "parse-tweet-bolt";
	final String INTERMEDIATE_RANKER_BOLT = "intermediate-ranker";
	final String TOTAL_RANKER_BOLT = "total-ranker";
	final String COUNT_BOLT = "count-hashtag-bolt";
	final String TWEET_FILTER_BOLT = "tweet-filter-bolt";
	final String GEOLOCATION_BOLT = "geolocation-bolt";
	final String COUNT_LOCATION_HASHTAG_BOLT = "count-location-hashtag-bolt";
}
