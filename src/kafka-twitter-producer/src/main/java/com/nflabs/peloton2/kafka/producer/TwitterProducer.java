package com.nflabs.peloton2.kafka.producer;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


import java.util.*;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;


import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;


import org.json.simple.JSONObject;




public class TwitterProducer {
    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    
    /** Information necessary for accessing the Twitter API */
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
    
    /** The actual Twitter stream. It's set up to collect raw JSON data */
    private TwitterStream twitterStream;
    
    private void start(Context context) {
	
	/** Producer properties **/
	Properties props = new Properties();
	props.put("metadata.broker.list", context.getString(TwitterSourceConstant.BROKER_LIST));
	props.put("serializer.class", context.getString(TwitterSourceConstant.SERIALIZER));
	props.put("request.required.acks", context.getString(TwitterSourceConstant.REQUIRED_ACKS));
	
	ProducerConfig config = new ProducerConfig(props);
	
	final Producer<String, String> producer = new Producer<String, String>(config);
	
	/** Twitter properties **/
	consumerKey = context.getString(TwitterSourceConstant.CONSUMER_KEY_KEY);
	consumerSecret = context.getString(TwitterSourceConstant.CONSUMER_SECRET_KEY);
	accessToken = context.getString(TwitterSourceConstant.ACCESS_TOKEN_KEY);
	accessTokenSecret = context.getString(TwitterSourceConstant.ACCESS_TOKEN_SECRET_KEY);
	
	ConfigurationBuilder cb = new ConfigurationBuilder();
	cb.setOAuthConsumerKey(consumerKey);
	cb.setOAuthConsumerSecret(consumerSecret);
	cb.setOAuthAccessToken(accessToken);
	cb.setOAuthAccessTokenSecret(accessTokenSecret);
	cb.setJSONStoreEnabled(true);
	cb.setIncludeEntitiesEnabled(true);
	
	twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
	final Map<String, String> headers = new HashMap<String, String>();
	
	/** Twitter listener **/
	StatusListener listener = new StatusListener() {
		// The onStatus method is executed every time a new tweet comes
		// in.
		public void onStatus(Status status) {
		    // The EventBuilder is used to build an event using the
		    // the raw JSON of a tweet
		    // logger.info(status.getUser().getScreenName() + ": " + status.getText() + "time:"+ status.getCreatedAt());
		    // parse time and change format
		    // System.out.println("fromtwitt:" + status.getCreatedAt());
		    // String allTimeString = status.getCreatedAt().toString();
		    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
		    String zt= ZonedDateTime.now().format(formatter);
		    System.out.println("zt:" + zt);


		    Map<String, Object> mapData = new HashMap<String, Object>();
		    mapData.put("FavoriteCount", status.getFavoriteCount());
		    mapData.put("Lang", status.getLang());
		    mapData.put("Place", status.getPlace());
		    mapData.put("isFavorited", status.isFavorited());
		    mapData.put("isPossiblySensitive", status.isPossiblySensitive());
		    mapData.put("ScreenName", status.getInReplyToScreenName());
		    mapData.put("GeoLocation", status.getGeoLocation());
		    mapData.put("RetweetCount", status.getRetweetCount());
		    // mapData.put("value", 1);
		    mapData.put("timestamp", zt);
		    String idString = String.valueOf(status.getId());
		    mapData.put("id", idString);
		    mapData.put("text", status.getText());
		    mapData.put("source", status.getSource());
		    JSONObject json = new JSONObject();
		    json.putAll( mapData );
		    // logger.info("myjson:" + json.toString());
		    //logger.info("===originaltwitt" + DataObjectFactory.getRawJSON(status) + "===");
		    System.out.println("---customizedtwitt " + json.toString());	
		    
		    KeyedMessage<String, String> data = new KeyedMessage<String, String>(context.getString(TwitterSourceConstant.KAFKA_TOPIC), json.toString());
		    producer.send(data);
		     /*try
		     {	
		     	Thread.sleep(2000);    
	   	     }
		     catch(InterruptedException e)
		     {
		    	// logger.info("onTrackLimitationNotice...");
		     }*/
		  //   KeyedMessage<String, String> data = new KeyedMessage<String, String>(context.getString(TwitterSourceConstant.KAFKA_TOPIC)
									 //, DataObjectFactory.getRawJSON(status).toString());
		//    producer.send(data);
		}
		    
		public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
		    // logger.info("onDeletionNotice...");
		}
		public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
		    // logger.info("onTrackLimitationNotice...");
		}
		public void onScrubGeo(long userId, long upToStatusId) {
		    // logger.info("onScrubGeo...");
		}
		public void onException(Exception ex) {
		    // logger.info("Shutting down Twitter sample stream..." + ex.toString());
		    // twitterStream.shutdown();
		}
		
		public void onStallWarning(StallWarning warning) {}
	    };
	
	/** Bind the listener **/
	twitterStream.addListener(listener);
	/** GOGOGO **/
	twitterStream.sample();   
    }
    
    public static void main(String[] args) {
	try {
	    Context context = new Context(args[0]);
	    TwitterProducer tp = new TwitterProducer();
	    tp.start(context);
	    
	} catch (Exception e) {
	    logger.info(e.getMessage());
	}
	
    }
}

