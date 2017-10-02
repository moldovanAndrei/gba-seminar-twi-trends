package com.twitrends.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.twitrends.tools.EncodingHelper;
import com.twitrends.util.FieldNames;

import twitter4j.HashtagEntity;
import twitter4j.Status;

/**
 * Reads a stream of tweets and only emits those containing hashtags.
 * Hashtags must also have TwiTrends relevant encoding.
 * 
 * @author Andrei Moldovan.
 */
public class TweetFilterBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2053427206826686117L;

	private OutputCollector collector;
	private EncodingHelper encodingHelper;
//	private long noHashtagCount = 0;
//	private long hashtagCount = 0;
//	private long allHashtagCount = 0;
	
	@Override
	public void execute(Tuple tuple) {
		//TODO: Daca un hashtag ii bun si unul nu, nu emit...
		boolean emit = true;
		Status tweet = (Status) tuple.getValue(0);
		HashtagEntity []hashtags = tweet.getHashtagEntities();
		
//		if((noHashtagCount + hashtagCount) % 1000 == 0) {
//			System.out.println("-------------------------------------------");
//			System.out.println("Total tweets: " + (noHashtagCount + hashtagCount));
//			System.out.println("Tweets without hashtags: " + noHashtagCount);
//			System.out.println("Tweets containing hashtags: " + hashtagCount);
//			System.out.println("Total Hashtags counted: " + allHashtagCount);
//		}
		
		if(hashtags.length == 0) {
			emit = false;
//			noHashtagCount++;
		} else {
//			hashtagCount++;
			for(HashtagEntity hashtag : tweet.getHashtagEntities()) {
//				allHashtagCount++;
				if(!encodingHelper.isValidEncoded(hashtag.getText())) {
					emit = false;
				}
			}
		}
		if(emit) {
			collector.emit(new Values(tweet));
		}
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		collector = outputCollector;
		encodingHelper = new EncodingHelper();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(FieldNames.TWEET));
	}
}
