package com.twitrends.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.twitrends.data.TweetLocationData;
import com.twitrends.tools.TupleHelpers;
import com.twitrends.util.FieldNames;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class ParseTweetBolt extends BaseRichBolt {

	private static final long serialVersionUID = -175076199840475279L;
	private OutputCollector collector;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		collector = outputCollector;
	}

	@Override
	public void execute(Tuple tuple) {
		Status tweet = (Status) tuple.getValue(0);
		TweetLocationData locationData = TupleHelpers.createLocationData(tweet);
		
		for (HashtagEntity hashtag : tweet.getHashtagEntities()) {
			collector.emit(new Values(hashtag.getText(), locationData));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.HASHTAG, FieldNames.LOCATION_DATA));
	}
}
