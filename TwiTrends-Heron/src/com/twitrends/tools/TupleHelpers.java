//package storm.starter.util;
package com.twitrends.tools;

import org.apache.storm.tuple.Tuple;

import com.twitrends.data.TweetLocationData;
import com.twitrends.util.Constants;

import twitter4j.GeoLocation;
import twitter4j.Place;
import twitter4j.Status;
import twitter4j.User;

public final class TupleHelpers {

	private TupleHelpers() {
	}

	public static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
	
	/**
	 * Creates a TweetLocationData from a Tweet.
	 * 
	 * @param tweet
	 * @return
	 */
	public static TweetLocationData createLocationData(Status tweet) {
		GeoLocation geoLocation = tweet.getGeoLocation();
		Place place = tweet.getPlace();
		User user = tweet.getUser();
		
		String placeName = null;
		String placeCountry = null;
		String placeType = null;
		String userLocation = null;
		
		if(place != null) {
			placeName = place.getName();
			placeCountry = place.getCountry();
			placeType = place.getPlaceType();
		}
		if(user != null) {
			userLocation = user.getLocation();
		}
		return new TweetLocationData(geoLocation, placeName, placeCountry, placeType, userLocation);
	}
}
