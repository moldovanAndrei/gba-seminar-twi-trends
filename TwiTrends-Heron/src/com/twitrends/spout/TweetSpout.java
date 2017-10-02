package com.twitrends.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import com.twitrends.util.Constants;
import com.twitrends.util.FieldNames;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Uses Twitter4J to read a tweet-stream and emit tuples for the topology.
 * 
 * @author Andrei Moldovan.
 */
public class TweetSpout extends BaseRichSpout {

	// Twitter4J credentials:
	private String key;
	private String secret;
	private String token;
	private String tokenSecret;

	// To output tuples from spout to the next stage bolt
	private SpoutOutputCollector collector;
	// Twitter4j - Twitter stream
	private TwitterStream twitterStream;
	// Buffer for tweets
	private LinkedBlockingQueue<Status> queue;

	/**
	 * Constructor that sets the Twitter4J credentials.
	 * 
	 * @param key
	 * @param secret
	 * @param token
	 * @param tokenSecret
	 */
	public TweetSpout(String key, String secret, String token, String tokenSecret) {
		this.key = key;
		this.secret = secret;
		this.token = token;
		this.tokenSecret = tokenSecret;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(FieldNames.TWEET));
	}

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		// create the buffer to block tweets
		queue = new LinkedBlockingQueue<Status>(Constants.TWEET_BUFFER_SIZE);
		// save the output collector for emitting tuples
		collector = spoutOutputCollector;

		// build the config with credentials for twitter 4j
		ConfigurationBuilder config = new ConfigurationBuilder()
				.setOAuthConsumerKey(key)
				.setOAuthConsumerSecret(secret)
				.setOAuthAccessToken(token)
				.setOAuthAccessTokenSecret(tokenSecret);
		
		// create the twitter stream factory with the config
		TwitterStreamFactory fact = new TwitterStreamFactory(config.build());
		// get an instance of twitter stream
		twitterStream = fact.getInstance();
		// provide the handler for twitter stream
		twitterStream.addListener(new TweetListener());
		// start the sampling of tweets
		twitterStream.sample();
	}

	@Override
	public void nextTuple() {
		Status tweet = queue.poll();
		if(tweet == null) {
			Utils.sleep(50);
		} else {
			collector.emit(new Values(tweet));
		}
	}

	@Override
	public void close() {
		 twitterStream.shutdown();
	}

	// Class for listening on the tweet stream - for twitter4j
	private class TweetListener implements StatusListener {

		// Implement the callback function when a tweet arrives
		@Override
		public void onStatus(Status status) {
			// add the tweet into the queue buffer
			queue.offer(status);
		}

		@Override
		public void onDeletionNotice(StatusDeletionNotice sdn) {
		}

		@Override
		public void onTrackLimitationNotice(int i) {
		}

		@Override
		public void onScrubGeo(long l, long l1) {
		}

		@Override
		public void onStallWarning(StallWarning warning) {
		}

		@Override
		public void onException(Exception e) {
			e.printStackTrace();
		}
	};
}
