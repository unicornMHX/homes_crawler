package com.ucr;

import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class Worker implements Runnable{

	private String apikey;
	private Vector<String> links;
	
	public Worker(String apikey, Vector<String> links) {
		// TODO Auto-generated constructor stub
		this.apikey = apikey;
		this.links = links;
	}
	
	@Override
	public void run(){
		// TODO Auto-generated method stub
		
		int count = 0;
		String threadid = Thread.currentThread().getName();
		
		for(String link : links){
			count++;
			try {
				/*
				 * check whether this record has been transferred
				 * why check here?
				 * For each home_url, we only use geocoding once
				 * then, we will insert all records into new table
				 */
				Connection connection1 = newConnection();
				Statement statement1 = null;
				ResultSet resultSet1 = null;
				statement1 = connection1.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY);
				statement1.setFetchSize(Integer.MIN_VALUE);
				resultSet1 = statement1.executeQuery("SELECT count(*) as num FROM homes_2018_new WHERE home_url='" +link+"'");
				boolean flag = false;
				while(resultSet1.next()){
					int num = resultSet1.getInt("num");
					if(num > 0)
						flag = true;
				}
				connection1.close();
				if(flag)
					continue;
				/*
				 * use Vector to store different parts
				 * yearbuilt/garage/e.t.c.: They are stored in int type. But they may be null, hence, we extract String
				 * one more tip here: A home can be put on rent category for some time and later be put on sale category.
				 * 					In this case, they may share same home_url. The occurrence rate is approximately 0.15%.
				 */
				String streetaddr = "";
				String city = "";
				String state = "";
				String zipcode = "";
				Vector<String> property_type = new Vector<String>();
				Vector<String> record_type = new Vector<String>();
				Vector<Integer> numbed = new Vector<Integer>();
				Vector<Integer> num_bath_full = new Vector<Integer>();
				Vector<Integer> num_bath_part = new Vector<Integer>();
				Vector<Integer> rentalprice_min = new Vector<Integer>();
				Vector<Integer> rentalprice_max = new Vector<Integer>();
				Vector<Integer> saleprice = new Vector<Integer>();
				Vector<String> yearbuilt = new Vector<String>();
				Vector<String> floor_plan = new Vector<String>();
				Vector<Integer> garage = new Vector<Integer>();
				Vector<String> lotsize = new Vector<String>();
				Vector<Integer> stories = new Vector<Integer>();
				Vector<Integer> size = new Vector<Integer>();
				Vector<String> pool = new Vector<String>();
				Vector<String> style = new Vector<String>();
				Vector<Integer> stainlessAppliances = new Vector<Integer>();
				Vector<Integer> fireplace = new Vector<Integer>();
				Vector<String> description = new Vector<String>();
				Vector<Timestamp> crawl_time = new Vector<Timestamp>();
				Vector<Date> datelisted = new Vector<Date>();
				Vector<String> dogAllowed = new Vector<String>();
				Vector<String> catAllowed = new Vector<String>();
				Vector<String> MLSNumber = new Vector<String>();
				
 				
				Connection connection = newConnection();
				Statement statement = null;
				ResultSet resultSet = null;
				statement = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY);
				statement.setFetchSize(Integer.MIN_VALUE);
				resultSet = statement.executeQuery("SELECT * FROM homes_2018 WHERE home_url='" +link+"'");
				while(resultSet.next()){
					streetaddr = resultSet.getString("streetaddr");
					city = resultSet.getString("city");
					state = resultSet.getString("state");
					zipcode = resultSet.getString("zipcode");
					property_type.add(resultSet.getString("property_type"));
					record_type.add(resultSet.getString("record_type"));
					numbed.add(resultSet.getInt("numbed"));
					num_bath_full.add(resultSet.getInt("num_bath_full"));
					num_bath_part.add(resultSet.getInt("num_bath_part"));
					rentalprice_min.add(resultSet.getInt("rentalprice_min"));
					rentalprice_max.add(resultSet.getInt("rentalprice_max"));
					saleprice.add(resultSet.getInt("saleprice"));
					yearbuilt.add(resultSet.getString("yearbuilt"));
					floor_plan.add(resultSet.getString("floor_plan"));
					garage.add(resultSet.getInt("garage"));
					lotsize.add(resultSet.getString("lotsize"));
					stories.add(resultSet.getInt("stories"));
					size.add(resultSet.getInt("size"));
					pool.add(resultSet.getString("pool"));
					style.add(resultSet.getString("style"));
					stainlessAppliances.add(resultSet.getInt("stainlessAppliances"));
					fireplace.add(resultSet.getInt("fireplace"));
					description.add(resultSet.getString("description"));
					crawl_time.add(resultSet.getTimestamp("crawl_time"));
					datelisted.add(resultSet.getDate("datelisted"));
					dogAllowed.add(resultSet.getString("dogAllowed"));
					catAllowed.add(resultSet.getString("catAllowed"));
					MLSNumber.add(resultSet.getString("MLSNumber"));
				}			
				connection.close();
				
				/*
				 * Format address and get latitude and longitude
				 */
				if (streetaddr == null || streetaddr.equals("") || streetaddr.toLowerCase().startsWith("foreclosure")
						|| city == null || state == null) {
					System.out.println(count + ": address is not valid");
					continue;
				}

				String address = streetaddr.replaceAll(" ", "+").replaceAll("[^0-9a-zA-Z\\+]", "") + ",+"
						+ city.replaceAll(" ", "+") + ",+" + state;
				
				String latLong[] = getLatLongPositions(address, apikey);
				
				System.out.println(threadid + " " + count + ": " + address + " [" + latLong[0] + ", " + latLong[1]+"]");
				
				/*
				 * insert into a new table
				 * why?
				 * It takes approximately 30 seconds per update while it just takes several seconds to 
				 * 	do insert sentence 
				 * 
				 */
				
				Connection connection2 = newConnection();
				for (int i = 0; i < numbed.size(); i++) {
					String sqlString = "insert into homes_2018_new(website,home_url,property_type, record_type, "
							+ "streetaddr,city,state,zipcode,numbed,num_bath_full,rentalprice_min,rentalprice_max,"
							+ "yearbuilt,floor_plan,garage,lotsize,stories,size,pool,style,stainlessAppliances,"
							+ "fireplace,description,crawl_time,datelisted,dogAllowed,catAllowed,MLSNumber,saleprice,"
							+ "latitude,longitude) "
							+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
					PreparedStatement preparedStatement = connection2.prepareStatement(sqlString);
					preparedStatement.setString(1, "https://www.homes.com/");
					preparedStatement.setString(2, link);
					if (property_type.get(i) != null) {
						preparedStatement.setString(3, property_type.get(i));
					} else {
						preparedStatement.setNull(3, Types.VARCHAR);
					}
					preparedStatement.setString(4, record_type.get(i));
					if (streetaddr != null) {
						preparedStatement.setString(5, streetaddr);
					} else {
						preparedStatement.setNull(5, Types.VARCHAR);
					}
					if (city != null) {
						preparedStatement.setString(6, city);
					} else {
						preparedStatement.setNull(6, Types.VARCHAR);
					}
					if (state != null) {
						preparedStatement.setString(7, state);
					} else {
						preparedStatement.setNull(7, Types.VARCHAR);
					}
					if (zipcode != null) {
						preparedStatement.setInt(8, Integer.parseInt(zipcode));
					} else {
						preparedStatement.setNull(8, Types.INTEGER);
					}
					preparedStatement.setInt(9, numbed.get(i));
					preparedStatement.setInt(10, num_bath_full.get(i));
					preparedStatement.setInt(11, rentalprice_min.get(i));
					preparedStatement.setInt(12, rentalprice_max.get(i));
					if (yearbuilt.get(i) != null) {
						preparedStatement.setInt(13, Integer.parseInt(yearbuilt.get(i)));
					} else {
						preparedStatement.setNull(13, Types.INTEGER);
					}
					preparedStatement.setString(14, floor_plan.get(i));
					preparedStatement.setInt(15, garage.get(i));
					if (lotsize.get(i) != null) {
						preparedStatement.setInt(16, Integer.parseInt(lotsize.get(i)));
					} else {
						preparedStatement.setNull(16, Types.INTEGER);
					}
					preparedStatement.setInt(17, stories.get(i));
					preparedStatement.setInt(18, size.get(i));
					if (pool.get(i) != null) {
						preparedStatement.setInt(19, Integer.parseInt(pool.get(i)));
					} else {
						preparedStatement.setNull(19, Types.TINYINT);
					}
					if (style.get(i) != null) {
						preparedStatement.setString(20, style.get(i));
					} else {
						preparedStatement.setNull(20, Types.VARCHAR);
					}
					preparedStatement.setInt(21, stainlessAppliances.get(i));
					preparedStatement.setInt(22, fireplace.get(i));
					preparedStatement.setString(23, description != null ? description.get(i) : null);
					preparedStatement.setTimestamp(24, crawl_time.get(i));
					if (datelisted.get(i) != null) {
						preparedStatement.setDate(25, datelisted.get(i));
					} else {
						preparedStatement.setNull(25, Types.DATE);
					}
					if (dogAllowed.get(i) != null) {
						preparedStatement.setInt(26, Integer.parseInt(dogAllowed.get(i)));
					} else {
						preparedStatement.setNull(26, Types.TINYINT);
					}
					if (catAllowed.get(i) != null) {
						preparedStatement.setInt(27, Integer.parseInt(catAllowed.get(i)));
					} else {
						preparedStatement.setNull(27, Types.TINYINT);
					}
					preparedStatement.setString(28, MLSNumber.get(i));
					preparedStatement.setInt(29, saleprice.get(i));
					if (latLong[0] == null || latLong[0].equals("")) {
						preparedStatement.setNull(30, Types.DOUBLE);
					} else {
						preparedStatement.setDouble(30, Double.parseDouble(latLong[0]));
					}
					if (latLong[1] == null || latLong[1].equals("")) {
						preparedStatement.setNull(31, Types.DOUBLE);
					} else {
						preparedStatement.setDouble(31, Double.parseDouble(latLong[1]));
					}
					
					
					preparedStatement.executeUpdate();
					preparedStatement.close();
				}
				connection2.close();
//				System.out.println(count + ": " + address + " [" + latLong[0] + ", " + latLong[1]+"]");
				
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
	}
	
	
	public static String[] getLatLongPositions(String address, String apikey) throws Exception {
	    int responseCode = 0;
	    
	    //
	    String api = "https://nominatim.openstreetmap.org/search?q=" + address + "&format=xml&limit=1";
	    URL url = new URL(api);
	    HttpURLConnection httpConnection = (HttpURLConnection)url.openConnection();
	    httpConnection.connect();
	    responseCode = httpConnection.getResponseCode();
	    if(responseCode == 200)
	    {
			 DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();;
			 Document document = builder.parse(httpConnection.getInputStream());
			 NodeList elements = document.getElementsByTagName("place");
			 String latitude = "";
			 String longitude = "";
			 for(int i = 0; i < elements.getLength(); i++){
				 Element element = (Element) elements.item(i);
				 latitude = element.getAttribute("lat");
				 longitude = element.getAttribute("lon");
			 }
			 return new String[] {latitude, longitude};
	    }
	    return null;	    
	}
	
//	public static String[] getLatLongPositions(String address, String apikey) throws Exception {
//		try {
//			int responseCode = 0;
//
//			String api = "https://maps.googleapis.com/maps/api/geocode/xml?address=" + address + "&key=" + apikey;
//			URL url = new URL(api);
//			HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
//			httpConnection.connect();
//			responseCode = httpConnection.getResponseCode();
//			if (responseCode == 200) {
//				DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
//				Document document = builder.parse(httpConnection.getInputStream());
//				XPathFactory xPathfactory = XPathFactory.newInstance();
//				XPath xpath = xPathfactory.newXPath();
//				XPathExpression expr = xpath.compile("/GeocodeResponse/status");
//				String status = (String) expr.evaluate(document, XPathConstants.STRING);
//
//				if (status.equals("OK")) {
//					expr = xpath.compile("//geometry/location/lat");
//					String latitude = (String) expr.evaluate(document, XPathConstants.STRING);
//					expr = xpath.compile("//geometry/location/lng");
//					String longitude = (String) expr.evaluate(document, XPathConstants.STRING);
//					return new String[] { latitude, longitude };
//				} else {
//					throw new Exception("Error from the API - response status: " + status);
//				}
//			}
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
//	}
	
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
