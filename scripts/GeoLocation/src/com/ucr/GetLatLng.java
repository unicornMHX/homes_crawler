package com.ucr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Vector;


public class GetLatLng {

	public static void main(String[] args) throws Exception {

		// 1600+Amphitheatre+Parkway,+Mountain+View,+CA
		
		/*
		 * input argument:
		 * 1. relative path of the file which contains Google API keys
		 * 2. how many threads are created for each key
		 */
		Vector<String> keys = new Vector<String>();
		File file = new File(args[0]);
		if(file.exists()){
			try {
				Reader reader = new FileReader(file);
				BufferedReader bufferedReader = new BufferedReader(reader);
				String line = "";
				line = bufferedReader.readLine();
				keys.add(line);
				while (line!=null && !line.equals("")) {
					line = bufferedReader.readLine();
					if(line!=null && !line.equals(""))
						keys.add(line);
				}
				bufferedReader.close();
				reader.close();
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}
		
		int threadsForOneKey = Integer.parseInt(args[1]);
		int threadsTotal = threadsForOneKey * keys.size();
		
		Connection connection = newConnection();
		Statement statement = null;
		ResultSet resultSet = null;
		statement = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		statement.setFetchSize(Integer.MIN_VALUE);
		resultSet = statement.executeQuery("SELECT DISTINCT(home_url) FROM homes_2018");

		Vector<Vector<String>> lists = new Vector<Vector<String>>();
		for (int i = 0; i < threadsTotal; i++) {
			lists.add(new Vector<String>());
		}
		int index = 0;
		while (resultSet.next()) {
			String home_url = resultSet.getString("home_url");
			lists.get(index % threadsTotal).add(home_url);
			index++;
		}
		connection.close();
		
		
		for (int i = 0; i < keys.size(); i++) {
			for (int j = 0; j < threadsForOneKey; j++){
				Worker worker = new Worker(keys.get(i), lists.get(i * threadsForOneKey + j));
//				System.out.println(lists.get(i).size());
				Thread thread = new Thread(worker);
				thread.start();
			}
		}
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
