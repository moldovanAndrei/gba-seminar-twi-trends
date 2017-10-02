package com.twitrends.tools;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

import twitter4j.GeoLocation;

public final class ConversionHelper {

	private final DecimalFormat formatter;

	public ConversionHelper() {
		formatter = new DecimalFormat("#.#", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
		formatter.setRoundingMode(RoundingMode.HALF_EVEN);
	}

	public GeoLocation createOneDigitPrecisionGeo(String latitude, String longitude) {
		String oneDigitLatitude = formatter.format(Double.parseDouble(latitude));
		String oneDigitLongitude = formatter.format(Double.parseDouble(longitude));
		return new GeoLocation(Double.parseDouble(oneDigitLatitude), Double.parseDouble(oneDigitLongitude));
	}

	public GeoLocation convertToOneDigitPrecision(GeoLocation geo) {
		String latidute = formatter.format(geo.getLatitude());
		String longitude = formatter.format(geo.getLongitude());
		return new GeoLocation(Double.parseDouble(latidute), Double.parseDouble(longitude));
	}

}
