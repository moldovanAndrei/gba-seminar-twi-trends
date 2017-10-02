package com.twitrends.bolt;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.common.base.Optional;
import com.twitrends.data.Location;
import com.twitrends.data.TweetLocationData;
import com.twitrends.tools.geo.BingMapsHelper;
import com.twitrends.tools.geo.ConcurrentLocationCache;
import com.twitrends.util.Constants;
import com.twitrends.util.FieldNames;

import twitter4j.GeoLocation;

public class GeoLocationBolt extends BaseRichBolt {

	private static final long serialVersionUID = -8453936081315257672L;
	private static final Logger LOG = Logger.getLogger(GeoLocationBolt.class);
	private long hit = 0;
	private long miss = 0;

	private OutputCollector collector;
	private ConcurrentLocationCache cache;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		collector = outputCollector;
		cache = ConcurrentLocationCache.getInstantce();
	}

	@Override
	public void execute(Tuple tuple) {
		String hashtag = (String) tuple.getValue(0);
		TweetLocationData locationData = (TweetLocationData) tuple.getValue(1);
		Optional<Location> location = Optional.absent();

		GeoLocation tweetGeo = locationData.getGeoLocation();
		if (tweetGeo != null) {
			location = cache.getLocation(tweetGeo);
			if (!location.isPresent()) {
				miss++;
				LOG.debug("Cache miss for " + tweetGeo);
				location = BingMapsHelper.getLocationFromCoords(tweetGeo.getLatitude(), tweetGeo.getLongitude());
				if (location.isPresent()) {
					cache.addLocation(tweetGeo, location.get());
				}
			} else {
				hit++;
				LOG.debug("Cache hit for " + tweetGeo + ", " + location.get());
			}
		}
		
		if(location.isPresent()) {
			if((hit + miss) != 0 && (hit + miss) % 10 == 0) {
				LOG.info("Hit: " + hit + ", Miss: " + miss);
			}
		}
		
		if(location.isPresent()) {
			LOG.debug("Emiting " + location + " for #" + hashtag);
			collector.emit(new Values(location.get(), hashtag));
		} else {
			LOG.debug("Emiting unknown location for #" + hashtag);
			collector.emit(new Values(Constants.UNKNOWN_LOCATION, hashtag));
		}
	}
	// }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.LOCATION, FieldNames.HASHTAG));
	}
}
