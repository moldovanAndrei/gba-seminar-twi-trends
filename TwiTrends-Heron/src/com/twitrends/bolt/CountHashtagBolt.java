package com.twitrends.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.twitrends.util.FieldNames;

/**
 * 
 * @author Andrei Moldovan.
 */
public class CountHashtagBolt extends BaseRichBolt {

	private static final long serialVersionUID = 6306141151908983805L;

	private OutputCollector collector;
	//Hashtag as key, count as value.
	private Map<String, Long> hashtagCounter;
	
	@Override
	public void execute(Tuple tuple) {
		String hashtag = tuple.getString(0);
		Long count;
		if(hashtagCounter.containsKey(hashtag)) {
			count = hashtagCounter.get(hashtag);
			count++;
		} else {
			count = new Long(1);
		}
		
		hashtagCounter.put(hashtag, count);
		collector.emit(new Values(hashtag, count));
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		collector = outputCollector;
		hashtagCounter = new HashMap<>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(FieldNames.HASHTAG, FieldNames.COUNT));
	}

}
