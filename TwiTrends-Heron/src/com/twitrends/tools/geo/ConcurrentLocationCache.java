package com.twitrends.tools.geo;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.io.Files;
import com.twitrends.data.Location;
import com.twitrends.tools.ConversionHelper;
import com.twitrends.util.Constants;

import twitter4j.GeoLocation;

/**
 * Cache for already computed locations. Can be accessed concurrently. Stores
 * already computed locations based on coordinates to avoid unnecessary access
 * to the Bing API.
 *
 * @author Andrei Moldovan
 */
public class ConcurrentLocationCache {

	private static final String QUOTATION = "\"";
	private static final Logger LOG = Logger.getLogger(ConcurrentLocationCache.class);
	private final ConversionHelper conversionHelper;

	/**
	 * Singleton instance.
	 */
	private static ConcurrentLocationCache instance = new ConcurrentLocationCache();
	private Map<GeoLocation, Location> locationMap;

	private ConcurrentLocationCache() {
		locationMap = new ConcurrentHashMap<GeoLocation, Location>();
		conversionHelper = new ConversionHelper();
		initializeCache();
		LOG.info("Cache initilized with " + locationMap.size() + " entries");
	}

	/**
	 * Initializes cache with default locations
	 */
	private void initializeCache() {

		List<String> tokens;
		Location location;
		GeoLocation coordinates;
		long lineCount = 0;

		try {
			LOG.info("Loading locations from file: " + Constants.LOCATION_FILE);
			File locationFile = new File(getClass().getClassLoader().getResource(Constants.LOCATION_FILE).getFile());
			List<String> lines = Files.readLines(locationFile, Charsets.ISO_8859_1);
			// Skip first line (header)
			lines.remove(0);
			for (String line : lines) {
				lineCount++;
				tokens = Splitter.on(",").trimResults().splitToList(line.replaceAll(QUOTATION, StringUtils.EMPTY));
				coordinates = conversionHelper.createOneDigitPrecisionGeo(tokens.get(5).trim(), tokens.get(6).trim());
				if(coordinates != null) {
					location = new Location(tokens.get(1), tokens.get(3));
					locationMap.put(coordinates, location);
				}
			}
		} catch (IOException e) {
			System.out.println("[WARN] Could not initialize Cache. Error type: " + e.getClass().getName());
		} catch (NumberFormatException e) {
			System.out.println("[ERROR] NumberFormatException at line " + lineCount);
		}
	}

	public synchronized boolean containsGeoLocation(GeoLocation coordinates) {
		GeoLocation oneDigitKey = conversionHelper.convertToOneDigitPrecision(coordinates);
		return locationMap.containsKey(oneDigitKey);
	}

	public synchronized Optional<Location> getLocation(GeoLocation coordinates) {
		GeoLocation oneDigitKey = conversionHelper.convertToOneDigitPrecision(coordinates);
		return Optional.fromNullable(locationMap.get(oneDigitKey));
	}

	public synchronized void addLocation(GeoLocation coordinates, Location location) {
		if (!locationMap.containsKey(location)) {
			locationMap.put(coordinates, location);
			 LOG.info("Added new location to cahce: " + coordinates + ", " + location);
		}
	}

	public Map<GeoLocation, Location> getLocationMap() {
		return locationMap;
	}

	public static ConcurrentLocationCache getInstantce() {
		return instance;
	}
}
