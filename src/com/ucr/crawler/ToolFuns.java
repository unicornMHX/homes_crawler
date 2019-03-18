package com.ucr.crawler;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class ToolFuns {
	
	/*
	 *  Here Geocode
	 *  rate limit: 250,000 / month
	 *  URL Format:
	 *  	https://geocoder.api.here.com/6.2/geocode.{OUTPUT_FORMAT}?app_id={APP_ID}&app_code={APP_CODE}&searchtext={ADDRESS}
	 *  INPUT:
	 *  	address: a formated address
	 *  	hereID: APP_ID
	 *  	hereCode: APP_CODE
	 *  
	 *  OUTPUT: 
	 *  	a String array of size 2: [0] = latitude, [1] = longitude
	 *  
	 */
	public static String[] getLatLongHere(String address, String hereId, String hereCode)
			throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
		int responseCode = 0;

		String api = "https://geocoder.api.here.com/6.2/geocode.xml?app_id=" + hereId + "&app_code=" + hereCode
				+ "&searchtext=" + address;
		URL url = new URL(api);
		HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
		httpConnection.connect();
		responseCode = httpConnection.getResponseCode();
		if (responseCode == 200) {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document document = builder.parse(httpConnection.getInputStream());
			XPathFactory xPathfactory = XPathFactory.newInstance();
			XPath xpath = xPathfactory.newXPath();
			XPathExpression expr = xpath.compile("//Result/Location/DisplayPosition/Latitude");
			String latitude = (String) expr.evaluate(document, XPathConstants.STRING);
			expr = xpath.compile("//Result/Location/DisplayPosition/Longitude");
			String longitude = (String) expr.evaluate(document, XPathConstants.STRING);
			return new String[] { latitude, longitude };
		}
		return null;
	}
}
