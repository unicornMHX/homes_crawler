package com.ucr;

import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;

public class GetLatLng {

	public static void main(String[] args) throws Exception {

		// 1600+Amphitheatre+Parkway,+Mountain+View,+CA

		Connection connection = newConnection();
		Statement statement = null;
		ResultSet resultSet = null;
		statement = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		statement.setFetchSize(Integer.MIN_VALUE);
		resultSet = statement.executeQuery("SELECT home_url,streetaddr,city,state,latitude FROM homes_2018");
		long count = 0;
		
		/*
		 * Typically, this set is designed for rent apartments. Only they share same home_url. 
		 */
		Set<String> set = new HashSet<String>();
		while (resultSet.next()) {
			count++;
			String home_url = resultSet.getString("home_url");
			String latitude = resultSet.getString("latitude");
			String streetadd = resultSet.getString("streetaddr");
			String city = resultSet.getString("city");
			String state = resultSet.getString("state");
			if (streetadd == null || streetadd.equals("") || streetadd.toLowerCase().startsWith("foreclosure")
					|| city == null || state == null) {
				System.out.println(count + ": address is not valid");
				continue;
			}
			if(set.contains(home_url)){
				System.out.println(count + ": same home_url - already done");
				continue;
			}
			
			if (latitude != null) {
				System.out.println(count + ": already done");
				set.add(home_url);
				continue;
			}
			String address = streetadd.replaceAll(" ", "+").replaceAll("[^0-9a-zA-Z\\+]", "") + ",+"
					+ city.replaceAll(" ", "+") + ",+" + state;
			System.out.println(count + ": " + address);
			String apikey = "AIzaSyDEA-8bdEPlTF1mKgYJlakxyPYijXJuVvM";
			// Thread.sleep(500);
			String latLong[] = getLatLongPositions(address, apikey);
			System.out.println(latLong[0] + latLong[1]);

			// update mysql
			Connection connection2 = newConnection();
			String sqlString = "UPDATE homes_2018 SET latitude=?, longitude=? WHERE home_url=?";
			PreparedStatement preparedStatement = connection2.prepareStatement(sqlString);
			preparedStatement.setDouble(1, Double.parseDouble(latLong[0]));
			preparedStatement.setDouble(2, Double.parseDouble(latLong[1]));
			preparedStatement.setString(3, home_url);
			preparedStatement.executeUpdate();
			preparedStatement.close();
			connection2.close();
		}

		connection.close();
		// String apikey = "AIzaSyDEA-8bdEPlTF1mKgYJlakxyPYijXJuVvM";
		// String latLongs[] = getLatLongPositions(address,apikey);
		// System.out.println("Latitude: " + latLongs[0] + " and Longitude: " +
		// latLongs[1]);
	}

	public static String[] getLatLongPositions(String address, String apikey) throws Exception {
		int responseCode = 0;

		String api = "https://maps.googleapis.com/maps/api/geocode/xml?address=" + address + "&key=" + apikey;
		URL url = new URL(api);
		HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
		httpConnection.connect();
		responseCode = httpConnection.getResponseCode();
		if (responseCode == 200) {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document document = builder.parse(httpConnection.getInputStream());
			XPathFactory xPathfactory = XPathFactory.newInstance();
			XPath xpath = xPathfactory.newXPath();
			XPathExpression expr = xpath.compile("/GeocodeResponse/status");
			String status = (String) expr.evaluate(document, XPathConstants.STRING);

			if (status.equals("OK")) {
				expr = xpath.compile("//geometry/location/lat");
				String latitude = (String) expr.evaluate(document, XPathConstants.STRING);
				expr = xpath.compile("//geometry/location/lng");
				String longitude = (String) expr.evaluate(document, XPathConstants.STRING);
				return new String[] { latitude, longitude };
			} else {
				throw new Exception("Error from the API - response status: " + status);
			}
		}
		return null;
	}
	
//	 for local window10 use only
	private static Connection newConnection() throws ClassNotFoundException, SQLException {
		String driver = "com.mysql.cj.jdbc.Driver";
		String url = "jdbc:mysql://localhost:3306/home_data?&useSSL=false&serverTimezone=UTC";
		String username = "root";
		String password = "123456";
		Connection conn = null;

		Class.forName(driver);
		conn = DriverManager.getConnection(url, username, password);
		return conn;
	}
	
	// for sever:v75 use only
//	private static Connection newConnection() throws ClassNotFoundException, SQLException {
//		Class.forName("com.mysql.jdbc.Driver");
//		String urldb = "jdbc:mysql://localhost/homeDB";
//		String user = "mxu054";
//		String password = "123456";
//		Connection conn = DriverManager.getConnection(urldb, user, password);
//		return conn;
//	}

}
