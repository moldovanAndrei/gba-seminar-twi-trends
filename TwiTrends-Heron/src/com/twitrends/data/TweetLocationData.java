package com.twitrends.data;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

import twitter4j.GeoLocation;

public class TweetLocationData implements Serializable {

	private static final long serialVersionUID = -839850167856451825L;

	private GeoLocation geoLocation;
	private String placeName;
	private String placeCountry;
	private String placeType;
	private String userLocation;
	
	public TweetLocationData(GeoLocation geoLocation, String placeName, String placeCountry, String placeType,
			String userLocation) {
		this.geoLocation = geoLocation;
		this.placeName = placeName;
		this.placeCountry = placeCountry;
		this.placeType = placeType;
		this.userLocation = userLocation;
	}

	public GeoLocation getGeoLocation() {
		return geoLocation;
	}
	
	public String getPlaceName() {
		return placeName;
	}
	
	public String getPlaceCountry() {
		return placeCountry;
	}
	
	public String getPlaceType() {
		return placeType;
	}
	
	public String getUserLocation() {
		return userLocation;
	}

	public boolean hasPlace() {
		return !StringUtils.isEmpty(placeType);
	}
	@Override
	public String toString() {
		return "TweetLocationData [geoLocation=" + geoLocation + ", placeName=" + placeName + ", placeCountry="
				+ placeCountry + ", placeType=" + placeType + ", userLocation=" + userLocation + "]";
	}
}
