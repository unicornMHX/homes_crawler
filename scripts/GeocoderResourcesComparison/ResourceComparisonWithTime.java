import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class ResourceComparison {

	/*
	 * Author: Minghao Xu
	 * Date: 03-05-2019
	 * 
	 * This script aims to compare accuracy of Geocoding resources
	 * we use Google results as standard
	 *  
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		int testingNum_DEFAULT = 5000;
		int testingNum = Integer.parseInt(args[0]);
		if(testingNum <= 0 || testingNum >10000)
			testingNum = testingNum_DEFAULT;
		
		// USenG2Vx4cNrA43f3lEBABnHIjdJC9js
		String mapQuestKey = args[1];
		// Ap26Mee7Z0fsr3IARVgZmMjTwFj_2qNGeAazGPdb8jQili5B3QgRBSOjAZUU8mAY
		String bingKey = args[2];
		
		String hereId = args[3];
		String hereCode = args[4];
		
		/*
		 *  total_count:
		 *  	total # of records in testing set
		 *  nominatim_error: 
		 *  	the # of records which locate at error point
		 *  	the sum of differences of latitude and longitude is greater than 0.1
		 *  nominatim_loss:
		 *  	the # of records which return NULL
		 *  nominatim_RMSE:
		 *  	Root Mean Squared Error if using nominatim
		 *  nominatim_RMSE_NoExtremum:
		 *  	Root Mean Squared Error if using nominatim (drop errors)
		 *  similar for others
		 */
		int nominatim_error = 0;
		int nominatim_loss = 0;
		double nominatim_RMSE = 0;
		double nominatim_RMSE_NoExtremum = 0;
		int mapquest_error = 0;
		int mapquest_loss = 0;
		double mapquest_RMSE = 0;
		double mapquest_RMSE_NoExtremum = 0;
		int bing_error = 0;
		int bing_loss = 0;
		double bing_RMSE = 0;
		double bing_RMSE_NoExtremum = 0;
		int here_error = 0;
		int here_loss = 0;
		double here_RMSE = 0;
		double here_RMSE_NoExtremum = 0;
		
		// ====================================Nominatim===============================================
		int count = 0;
		long nominatim_start = System.currentTimeMillis();
		Connection connection = newConnection();
		Statement statement = null;
		ResultSet resultSet = null;
		statement = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		statement.setFetchSize(Integer.MIN_VALUE);
		resultSet = statement.executeQuery(
				"SELECT streetaddr,city,state,zipcode,latitude,longitude FROM homes_2018_new" + " limit " + testingNum);
		while (resultSet.next()) {

			String streetaddr = resultSet.getString(1);
			String city = resultSet.getString(2);
			String state = resultSet.getString(3);
			String zipcode = resultSet.getString(4);
			double latitude = resultSet.getDouble(5);
			double longitude = resultSet.getDouble(6);

			if (streetaddr == null || streetaddr.equals("") || streetaddr.toLowerCase().startsWith("foreclosure")
					|| city == null || state == null) {
				continue;
			}
			String address = streetaddr.replaceAll(" ", "+").replaceAll("[^0-9a-zA-Z\\+]", "") + ",+"
					+ city.replaceAll(" ", "+") + ",+" + state;
			
			String latLonNominatim[] = getLatLongNominatim(address);
			Thread.sleep(700);
			count++;
			if (latLonNominatim == null || latLonNominatim[0] == null || latLonNominatim[0].equals("") )
				nominatim_loss++;
			else {
				double latitude_nominatim = Double.parseDouble(latLonNominatim[0]);
				double longitude_nominatim = Double.parseDouble(latLonNominatim[1]);
				nominatim_RMSE +=  Math.pow(latitude_nominatim - latitude, 2)
						+ Math.pow(longitude_nominatim - longitude, 2);
				if (Math.abs(latitude_nominatim - latitude) + Math.abs(longitude_nominatim - longitude) > 0.1) {
					nominatim_error++;
				} else {
					nominatim_RMSE_NoExtremum += Math.pow(latitude_nominatim - latitude, 2)
							+ Math.pow(longitude_nominatim - longitude, 2);
				}
			}
			if(count % 100 == 0){
				System.out.println("total count=" + count + ", nominatim_error=" + nominatim_error
						+ ", nominatim_loss=" + nominatim_loss);
				System.out.println(
						"nominatim_RMSE=" + nominatim_RMSE + ", nominatim_RMSE_NoExtremum=" + nominatim_RMSE_NoExtremum);
			}
		}
		connection.close();
		long nominatim_end = System.currentTimeMillis();

		// ====================================MapQuest===============================================
		int count1 = 0;
		long mapQuest_start = System.currentTimeMillis();
		Connection connection1 = newConnection();
		Statement statement1 = null;
		ResultSet resultSet1 = null;
		statement1 = connection1.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		statement1.setFetchSize(Integer.MIN_VALUE);
		resultSet1 = statement1.executeQuery(
				"SELECT streetaddr,city,state,zipcode,latitude,longitude FROM homes_2018_new" + " limit " + testingNum);
		while (resultSet1.next()) {

			String streetaddr = resultSet1.getString(1);
			String city = resultSet1.getString(2);
			String state = resultSet1.getString(3);
			String zipcode = resultSet1.getString(4);
			double latitude = resultSet1.getDouble(5);
			double longitude = resultSet1.getDouble(6);
			if (streetaddr == null || streetaddr.equals("") || streetaddr.toLowerCase().startsWith("foreclosure")
					|| city == null || state == null) {
				continue;
			}
			String address = streetaddr.replaceAll(" ", "+").replaceAll("[^0-9a-zA-Z\\+]", "") + ",+"
					+ city.replaceAll(" ", "+") + ",+" + state;
			
			String latLonMapQuest[] = getLatLongMapQuest(address, mapQuestKey);
			count1++;
			if (latLonMapQuest == null || latLonMapQuest[0] == null || latLonMapQuest[0].equals("")) {
				mapquest_loss++;
			} else {
				double latitude_mapquest = Double.parseDouble(latLonMapQuest[0]);
				double longitude_mapquest = Double.parseDouble(latLonMapQuest[1]);
				mapquest_RMSE += Math.pow(latitude_mapquest - latitude, 2)
						+ Math.pow(longitude_mapquest - longitude, 2);
				if (Math.abs(latitude_mapquest - latitude) + Math.abs(longitude_mapquest - longitude) > 0.1) {
					mapquest_error++;
				} else {
					mapquest_RMSE_NoExtremum += Math.pow(latitude_mapquest - latitude, 2)
							+ Math.pow(longitude_mapquest - longitude, 2);
				}
			}
			if (count1 % 100 == 0) {
				System.out.println("total count=" + count1 + ", mapquest_error=" + mapquest_error + ", mapquest_loss="
						+ mapquest_loss);
				System.out.println(
						"mapquest_RMSE=" + mapquest_RMSE + ", mapquest_RMSE_NoExtremum=" + mapquest_RMSE_NoExtremum);
			}
		}
		connection1.close();
		long mapQuest_end = System.currentTimeMillis();

		// ====================================BING===================================================
		long bing_start = System.currentTimeMillis();
		int count2 = 0;
		Connection connection2 = newConnection();
		Statement statement2 = null;
		ResultSet resultSet2 = null;
		statement2 = connection2.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		statement2.setFetchSize(Integer.MIN_VALUE);
		resultSet2 = statement2.executeQuery(
				"SELECT streetaddr,city,state,zipcode,latitude,longitude FROM homes_2018_new" + " limit " + testingNum);
		while (resultSet2.next()) {

			String streetaddr = resultSet2.getString(1);
			String city = resultSet2.getString(2);
			String state = resultSet2.getString(3);
			String zipcode = resultSet2.getString(4);
			double latitude = resultSet2.getDouble(5);
			double longitude = resultSet2.getDouble(6);
			if (streetaddr == null || streetaddr.equals("") || streetaddr.toLowerCase().startsWith("foreclosure")
					|| city == null || state == null) {
				continue;
			}

			String address = streetaddr.replaceAll(" ", "+").replaceAll("[^0-9a-zA-Z\\+]", "") + ",+"
					+ city.replaceAll(" ", "+") + ",+" + state;
			String latLonBing[] = getLatLongBing(address + "+" + zipcode, bingKey);
			count2++;
			if (latLonBing == null || latLonBing[0] == null || latLonBing[0].equals(""))
				bing_loss++;
			else {
				double latitude_bing = Double.parseDouble(latLonBing[0]);
				double longitude_bing = Double.parseDouble(latLonBing[1]);
				bing_RMSE += Math.pow(latitude_bing - latitude, 2) + Math.pow(longitude_bing - longitude, 2);
				if (Math.abs(latitude_bing - latitude) + Math.abs(longitude_bing - longitude) > 0.1) {
					bing_error++;
				} else {
					bing_RMSE_NoExtremum += Math.pow(latitude_bing - latitude, 2)
							+ Math.pow(longitude_bing - longitude, 2);
				}
			}
			if (count2 % 100 == 0) {
				System.out
				.println("total count=" + count2 + ", bing_error=" + bing_error + ", bing_loss=" + bing_loss);
				System.out.println("bing_RMSE=" + bing_RMSE + ", bing_RMSE_NoExtremum=" + bing_RMSE_NoExtremum);
			}
		}
		connection2.close();
		long bing_end = System.currentTimeMillis();
		
		// ====================================Here===================================================
		long here_start = System.currentTimeMillis();
		int count3 = 0;
		Connection connection3 = newConnection();
		Statement statement3 = null;
		ResultSet resultSet3 = null;
		statement3 = connection3.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		statement3.setFetchSize(Integer.MIN_VALUE);
		resultSet3 = statement3.executeQuery(
				"SELECT streetaddr,city,state,zipcode,latitude,longitude FROM homes_2018_new" + " limit " + testingNum);
		while (resultSet3.next()) {

			String streetaddr = resultSet3.getString(1);
			String city = resultSet3.getString(2);
			String state = resultSet3.getString(3);
			String zipcode = resultSet3.getString(4);
			double latitude = resultSet3.getDouble(5);
			double longitude = resultSet3.getDouble(6);
			if (streetaddr == null || streetaddr.equals("") || streetaddr.toLowerCase().startsWith("foreclosure")
					|| city == null || state == null) {
				continue;
			}

			String address = streetaddr.replaceAll(" ", "+").replaceAll("[^0-9a-zA-Z\\+]", "") + ",+"
					+ city.replaceAll(" ", "+") + ",+" + state;
			String latLonHere[] = getLatLongHere(address, hereId, hereCode);
			count3++;
			if (latLonHere == null || latLonHere[0] == null || latLonHere[0].equals(""))
				here_loss++;
			else {
				double latitude_here = Double.parseDouble(latLonHere[0]);
				double longitude_here = Double.parseDouble(latLonHere[1]);
				here_RMSE += Math.pow(latitude_here - latitude, 2) + Math.pow(longitude_here - longitude, 2);
				if (Math.abs(latitude_here - latitude) + Math.abs(longitude_here - longitude) > 0.1) {
					here_error++;
				} else {
					here_RMSE_NoExtremum += Math.pow(latitude_here - latitude, 2)
							+ Math.pow(longitude_here - longitude, 2);
				}
			}

			if (count3 % 100 == 0) {
				System.out
				.println("total count=" + count3 + ", here_error=" + here_error + ", here_loss=" + here_loss);
				System.out.println("here_RMSE=" + here_RMSE + ", here_RMSE_NoExtremum=" + here_RMSE_NoExtremum);
			}
		}
		connection3.close();
		long here_end = System.currentTimeMillis();

		nominatim_RMSE = Math.sqrt(nominatim_RMSE / (count - nominatim_loss));
		nominatim_RMSE_NoExtremum = Math.sqrt(nominatim_RMSE_NoExtremum / (count - nominatim_loss - nominatim_error));
		mapquest_RMSE = Math.sqrt(mapquest_RMSE / (count - mapquest_loss));
		mapquest_RMSE_NoExtremum = Math.sqrt(mapquest_RMSE_NoExtremum / (count - mapquest_loss - mapquest_error));
		bing_RMSE = Math.sqrt(bing_RMSE / (count - bing_loss));
		bing_RMSE_NoExtremum = Math.sqrt(bing_RMSE_NoExtremum / (count - bing_loss - bing_error));
		here_RMSE = Math.sqrt(here_RMSE / (count - here_loss));
		here_RMSE_NoExtremum = Math.sqrt(here_RMSE_NoExtremum / (count - here_loss - here_error));
		System.out.println("================================================================");
		System.out.println("NOMINATIM:");
		System.out.println("time=" + (double)(nominatim_end-nominatim_start)/1000 + ", RMSE=" 
						+ nominatim_RMSE + ", RMSE_NoExtremum="+nominatim_RMSE_NoExtremum);
		System.out.println("# of loss=" + nominatim_loss + ", # of error=" + nominatim_error);
		System.out.println("MAPQUEST:");
		System.out.println("time=" + (double)(mapQuest_end-mapQuest_start)/1000 + ", RMSE=" 
						+ mapquest_RMSE + ", RMSE_NoExtremum="+mapquest_RMSE_NoExtremum);
		System.out.println("# of loss=" + mapquest_loss + ", # of error=" + mapquest_error);
		System.out.println("BING:");
		System.out.println("time=" + (double)(bing_end-bing_start)/1000 + ", RMSE=" 
						+ bing_RMSE + ", RMSE_NoExtremum="+bing_RMSE_NoExtremum);
		System.out.println("# of loss=" + bing_loss + ", # of error=" + bing_error);
		System.out.println("HERE:");
		System.out.println("time=" + (double)(here_end-here_start)/1000 + ", RMSE=" 
						+ here_RMSE + ", RMSE_NoExtremum="+here_RMSE_NoExtremum);
		System.out.println("# of loss=" + here_loss + "# of error=" + here_error);

	}
	
	/*
	 * Nominatim - a free Geocode API
	 * limit: 
	 * 		1 transaction / second (in fact it is not applied) 
	 * 		no total limit
	 * INPUT: 
	 * 		address
	 * OUTPUT:
	 * 		a String array of size 2: [0] = latitude, [1] = longitude
	 */
	
	public static String[] getLatLongNominatim(String address) {
		for (int i = 0; i < 2; i++) {
			try {
				int responseCode = 0;
				String api = "https://nominatim.openstreetmap.org/search?q=" + address + "&format=xml&limit=1";
				URL url = new URL(api);
				HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
				httpConnection.connect();
				responseCode = httpConnection.getResponseCode();
				if (responseCode == 200) {
					DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
					Document document = builder.parse(httpConnection.getInputStream());
					NodeList elements = document.getElementsByTagName("place");
					String latitude = "";
					String longitude = "";
					for (int j = 0; j < elements.getLength(); j++) {
						Element element = (Element) elements.item(j);
						latitude = element.getAttribute("lat");
						longitude = element.getAttribute("lon");
					}
					return new String[] { latitude, longitude };
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	
	/*
	 *  MAPQUEST Geocode
	 *  rate limit: 15000 / month
	 *  URL Format:
	 *  	http://www.mapquestapi.com/geocoding/v1/batch?key=<>&location=<>&...&location=<>&maxResults=1&outFormat=xml
	 *  INPUT:
	 *  	address: a formated address
	 *  	mapQuestKey: an API key
	 *  
	 *  OUTPUT: 
	 *  	a String array of size 2: [0] = latitude, [1] = longitude
	 *  
	 */
	public static String[] getLatLongMapQuest(String address, String mapQuestKey) {

		for (int i = 0; i < 2; i++) {
			try {
				int responseCode = 0;

				String api = "http://www.mapquestapi.com/geocoding/v1/batch?key=" + mapQuestKey + "&location=" + address
						+ "&maxResults=1&outFormat=xml";
				URL url = new URL(api);
				HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
				httpConnection.connect();
				responseCode = httpConnection.getResponseCode();
				if (responseCode == 200) {
					DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
					Document document = builder.parse(httpConnection.getInputStream());
					XPathFactory xPathfactory = XPathFactory.newInstance();
					XPath xpath = xPathfactory.newXPath();
					XPathExpression expr = xpath
							.compile("/response/results/result/locations/location/displayLatLng/latLng/lat");
					String latitude = (String) expr.evaluate(document, XPathConstants.STRING);
					expr = xpath.compile("/response/results/result/locations/location/displayLatLng/latLng/lng");
					String longitude = (String) expr.evaluate(document, XPathConstants.STRING);
					return new String[] { latitude, longitude };

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	/*
	 *  MAPQUEST Batch Geocode
	 *  rate limit: up to 100 locations at a time for Batch Geocode
	 *  URL Format:
	 *  	http://www.mapquestapi.com/geocoding/v1/batch?key=<>&location=<>&...&location=<>&maxResults=1&outFormat=xml
	 *  INPUT:
	 *  	addressVector: a vector contains up to 100 addresses
	 *  	mapQuestKey: an API key
	 *  
	 *  OUTPUT: 
	 *  	a String array and String format: <latitude, longitude>
	 *  
	 */
	public static String[] getLatLongMapQuest(Vector<String> addressVector, String mapQuestKey)
			throws IOException, ParserConfigurationException, SAXException {
		if (addressVector.size() > 100)
			return null;
		int responseCode = 0;

		String[] result = new String[addressVector.size()];
		String api = "http://www.mapquestapi.com/geocoding/v1/batch?key=" + mapQuestKey;
		for (int i = 0; i < addressVector.size(); i++) {
			api += "&location=" + addressVector.get(i);
		}
		api += "&maxResults=1&outFormat=xml";
		URL url = new URL(api);
		HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
		httpConnection.connect();
		responseCode = httpConnection.getResponseCode();
		if (responseCode == 200) {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			;
			Document document = builder.parse(httpConnection.getInputStream());
			NodeList elements = document.getElementsByTagName("displayLatLng");
			for (int i = 0; i < elements.getLength(); i++) {
				Node element = elements.item(i).getFirstChild();
				String latitude = element.getFirstChild().getTextContent();
				String longitude = element.getLastChild().getTextContent();
				if (latitude != null && !latitude.equals(""))
					result[i] = latitude + "," + longitude;
			}

		}
		return result;
	}
	
	/*
	 *  Bing Geocode
	 *  rate limit: 125000 / year
	 *  URL Format:
	 *  	http://www.mapquestapi.com/geocoding/v1/batch?key=<>&location=<>&...&location=<>&maxResults=1&outFormat=xml
	 *  INPUT:
	 *  	address: a formated address
	 *  	mapQuestKey: an API key
	 *  
	 *  OUTPUT: 
	 *  	a String array of size 2: [0] = latitude, [1] = longitude
	 *  
	 */
	public static String[] getLatLongBing(String address, String bingKey) {
		for (int i = 0; i < 2; i++) {
			try {
				int responseCode = 0;

				String api = "https://dev.virtualearth.net/REST/v1/Locations?q=" + address + "&o=xml&maxResults=1&key="
						+ bingKey;
				URL url = new URL(api);
				HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
				httpConnection.connect();
				responseCode = httpConnection.getResponseCode();
				if (responseCode == 200) {
					DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
					Document document = builder.parse(httpConnection.getInputStream());
					XPathFactory xPathfactory = XPathFactory.newInstance();
					XPath xpath = xPathfactory.newXPath();
					XPathExpression expr = xpath.compile("/Response/StatusDescription");
					String status = (String) expr.evaluate(document, XPathConstants.STRING);

					if (status.equals("OK")) {
						expr = xpath.compile("//ResourceSets/ResourceSet/Resources/Location/Point/Latitude");
						String latitude = (String) expr.evaluate(document, XPathConstants.STRING);
						expr = xpath.compile("//ResourceSets/ResourceSet/Resources/Location/Point/Longitude");
						String longitude = (String) expr.evaluate(document, XPathConstants.STRING);
						return new String[] { latitude, longitude };
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	/*
	 *  Here Geocode
	 *  rate limit: 250000 / month
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

//	 for local window10 use only: mysql-connector-java-8.0.11.jar
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
}
