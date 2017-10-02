package com.twitrends.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisException;
import com.twitrends.data.Location;
import com.twitrends.data.LocationHashtagCount;
import com.twitrends.tools.ranker.Rankable;
import com.twitrends.tools.ranker.Rankings;
import com.twitrends.util.Constants;
import com.twitrends.util.TopologyIdentifiers;

public class RedisBolt extends BaseRichBolt {

	private static final long serialVersionUID = -6662462536019427865L;
	private static final Logger LOG = Logger.getLogger(RedisBolt.class);
	private long totalHashtagsProcessed = 0;

	transient RedisConnection<String, String> redis = null;
	private Map<String, Long> topHashtagMap = new HashMap<>();
	private Map<String, Long> unknownLocationHashtagMap = new HashMap<>();
	private Table<Location, String, Long> locationHashtagMap = HashBasedTable.create();
	private Table<String, Location, Long> hashtagLocationTable = HashBasedTable.create();
	private Rankings rankedHashtags;

	// Test purpose
	private int count = 0;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) { 
		try {
			RedisClient client = new RedisClient("localhost", 6388);
			redis = client.connect();
		} catch (RedisException e) {
			LOG.error("Could not connect to redis!");
		}
	}

	@Override
	public void execute(Tuple tuple) {

		String componentId = tuple.getSourceComponent();
		if (componentId.equals(TopologyIdentifiers.TOTAL_RANKER_BOLT)) {
			Rankings rankableList = (Rankings) tuple.getValue(0);
			rankedHashtags = rankableList;
			for (Rankable r : rankableList.getRankings()) {
				String hashtag = r.getObject().toString();
				Long count = r.getCount();
				topHashtagMap.put(hashtag, count);
				if(redis != null) {
					redis.publish(TopologyIdentifiers.TOPOLOGY_NAME, hashtag + "|" + count);
				}
			}
		} else if (componentId.equals(TopologyIdentifiers.COUNT_LOCATION_HASHTAG_BOLT)) {
			totalHashtagsProcessed++;
			count++;
			LocationHashtagCount locationHashtag = (LocationHashtagCount) tuple.getValue(0);
			if (Constants.UNKNOWN_LOCATION.equals(locationHashtag.getLocation())) {
				unknownLocationHashtagMap.put(locationHashtag.getHashtag(), locationHashtag.getCount());
			} else {
				locationHashtagMap.put(locationHashtag.getLocation(), locationHashtag.getHashtag(),
						locationHashtag.getCount());
				hashtagLocationTable.put(locationHashtag.getHashtag(), locationHashtag.getLocation(),
						locationHashtag.getCount());
			}
		}
		if (count == 1000) {
			System.out.println("AFTER " + totalHashtagsProcessed + " hashtags:");
			printMaps();
			count = 0;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Final bolt.
	}

	/**
	 * Debug purpose
	 */
	private void printMaps() {
		System.out.println("---------------------------------");
		System.out.println("Top Hashtags: ");
		if (rankedHashtags != null) {
			for (Rankable r : rankedHashtags.getRankings()) {
				System.out.println(r.getObject() + ": " + r.getCount());
			}
		}
		System.out.println("---------------------------------");

		if (!hashtagLocationTable.isEmpty()) {
			for (String hashtag : hashtagLocationTable.rowKeySet()) {
				System.out.println("Location hashtag for #" + hashtag);
				for (Map.Entry<Location, Long> entry : hashtagLocationTable.row((hashtag)).entrySet()) {
					System.out.println("   " + entry.getKey() + ": " + entry.getValue());
				}
			}
		}
	}
}
