package com.twitrends.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.twitrends.data.Location;
import com.twitrends.data.LocationHashtagCount;
import com.twitrends.util.FieldNames;

/**
 * Counts Hashtags based on a location.
 * 
 * @author Andrei Moldovan.
 */
public class CountLocationHashtagBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1531229415080713937L;

	private OutputCollector collector;
	//Location as row, Hashtag as column, Count as value.
	private Table<Location, String, Long> locationHashtagCounter;
	
	@Override
	public void execute(Tuple tuple) {
		Location location = (Location) tuple.getValue(0);
		String hashtag = tuple.getString(1);
		Long count;
		if(locationHashtagCounter.contains(location, hashtag)) {
			count = locationHashtagCounter.get(location, hashtag);
			count++;
		} else {
			count = new Long(1);
		}
		locationHashtagCounter.put(location, hashtag, count);
		final LocationHashtagCount toEmitValue = new LocationHashtagCount(location, hashtag, count);
		collector.emit(new Values(toEmitValue));
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		collector = outputCollector;
		locationHashtagCounter = HashBasedTable.create();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(FieldNames.LOCATION_HASHTAG_COUNT));
	}
}
