package com.twitrends.data;

import java.io.Serializable;

public class LocationHashtagCount implements Serializable {

	private static final long serialVersionUID = -6786571353847506056L;
	
	private Location location;
	private String hashtag;
	private Long count;
	
	
	public LocationHashtagCount(Location location, String hashtag, Long count) {
		this.location = location;
		this.hashtag = hashtag;
		this.count = count;
	}
	
	public Location getLocation() {
		return location;
	}
	public String getHashtag() {
		return hashtag;
	}
	public Long getCount() {
		return count;
	}
}
