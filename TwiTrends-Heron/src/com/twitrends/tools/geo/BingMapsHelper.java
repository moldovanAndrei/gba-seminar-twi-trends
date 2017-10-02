package com.twitrends.tools.geo;

import static java.net.HttpURLConnection.HTTP_OK;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Optional;
import com.twitrends.data.Location;
import com.twitrends.util.TwitterCredentials;

/**
 * Helper class which computes a Location based on Coordinates using Bing API.
 *
 * @author Andrei Moldovan
 */
public final class BingMapsHelper {

    private static final String BING_MAPS_URL_START = "http://dev.virtualearth.net/REST/v1/Locations/";
    private static final String OUTPUT_SPECIFIER = "?o=";
    private static final String BING_MAPS_KEY_PREFIX = "&key=";
    private static final String OUTPUT_TYPE = "json";
    private static final String COMMA = ",";
    
    private static final Logger LOG = Logger.getLogger(BingMapsHelper.class);
    
    /**
     * Computes City and Country based on latitude and longitude.
     *
     * @param latitude
     * @param longitude
     * @return Optional location
     */

    public final static Optional<Location> getLocationFromCoords(final double latitude, final double longitude) {

        URL bingMapsUrl = buildUrl(latitude, longitude);

        if (bingMapsUrl == null) {
            LOG.error("Could not generate Bing Maps ULR");
            return Optional.absent();
        }

        try {
            HttpURLConnection bingConnection = (HttpURLConnection) bingMapsUrl.openConnection();
            if (bingConnection.getResponseCode() == HTTP_OK) {
                return getLocationFromJson(bingConnection.getInputStream());
            } else {
                LOG.warn("Bing Maps URL response " + bingConnection.getResponseCode());
            }

        } catch (IOException e) {
        	LOG.error("Could not establisch Bing Maps Connection", e);
        }
        return Optional.absent();
    }

    /**
     * Extracts an Location Object from a Json InputStream.
     *
     * @param jsonStream
     * @return
     * @throws IOException
     */
    private static final Optional<Location> getLocationFromJson(InputStream jsonStream) throws IOException {

        //TODO: Refactor hardcoded Strings to Constants...
        final ObjectMapper jsonMapper = new ObjectMapper();
        final Map<String, Object> bingResponse = (Map<String, Object>) jsonMapper.readValue(jsonStream, Map.class);
        if (Integer.parseInt(String.valueOf(bingResponse.get("statusCode"))) == HTTP_OK) {
            final List<Map<String, Object>> resourceSets = (List<Map<String, Object>>) bingResponse.get("resourceSets");
            if (resourceSets != null && !resourceSets.isEmpty()) {
                final List<Map<String, Object>> resources = (List<Map<String, Object>>) resourceSets.get(0).get("resources");
                if (resources != null && !resources.isEmpty()) {
                    final Map<String, Object> address = (Map<String, Object>) resources.get(0).get("address");
                    //TODO: adminDistrict vs adminDistrict2 vs locality
                    return Optional.of((Location) new Location((String) address.get("countryRegion"), (String) address.get("adminDistrict2")));
                }
            }
        }

        return Optional.absent();
    }

    /**
     * Build a BingAPI specific URL
     *
     * @param latitude
     * @param longitude
     * @return
     */
    private static final URL buildUrl(double latitude, double longitude) {

        final StringBuilder urlBuilder = new StringBuilder();
        String urlString = urlBuilder
                .append(BING_MAPS_URL_START)
                .append(latitude)
                .append(COMMA)
                .append(longitude)
                .append(OUTPUT_SPECIFIER)
                .append(OUTPUT_TYPE)
                .append(BING_MAPS_KEY_PREFIX)
                .append(TwitterCredentials.BING_MAPS_API_KEY)
                .toString();

        try {
            return new URL(urlString);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        return null;
    }
}
