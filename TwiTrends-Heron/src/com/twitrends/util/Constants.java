package com.twitrends.util;

import com.twitrends.data.Location;

/**
 * Constants used in the TwiTrend Topology.
 * 
 * @author Andrei Moldovan.
 */
public interface Constants {
	
	final int TWEET_BUFFER_SIZE = 1000;
	final int TOP_N = 20;
	final String LOCATION_FILE = "GeoLiteCity-Location.csv";
	final String ISO_LOCATION_CONVERSION_FILE = "ISO 3166-1 alpha-2 country code to country name conversion.csv";
	final Location UNKNOWN_LOCATION = new Location("UNKNOWN","UNKNOWN");
	
	final String SYSTEM_COMPONENT_ID = "__system";
	final String SYSTEM_TICK_STREAM_ID = "__tick";
}
