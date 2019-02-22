package com.ucr.crawler;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Vector;


import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;
import org.openqa.selenium.firefox.FirefoxProfile;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class Worker implements Runnable {

	private Vector<String> states;
	private String recordType;
	private int life;
	
	public Worker(Vector<String> states, String recordType, int life){
		this.states = states;
		this.recordType = recordType;
		this.life = life;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		for(String str1 : states){
			crawlhomes(str1, recordType, life);
		}

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

	 //for local window10 use only
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
	
	public static void crawlhomes(String str1, String recordType, int life) {
		/*
		 * Version 2 revised on 17-Dec-2018 by Minghao Xu
		 * This version includes:
		 * 1. use a txt file to record what cities were crawled
		 *    it supports the function that continue crawling if the program was shut down by accident
		 * 2. fix a bug: get right results if total page equals 1
		 * 3. fix a bug: when save the html, set append to be false (we wont save same pages twice now)
		 * 4. use more try-catch. Now, we wont use the outer try-catch. Find why we cannot get all data
		 * 5. use log files to record errors 
		 * 
		 * Version 2.1 revised on 18-Dec-2018 by Minghao Xu
		 * 1. remove thread.sleep()
		 * 2. optimize memory utilization: disable firefox cache and image
		 * 3. add one more argument: life
		 *    	for each browser, we only use "life" times. Then we destroy it and create a new one
		 *    	it designed for running program on ubuntu OS on UCR server since memory easily leaks on v97
		 *      and if so, we will lose left data on current city.
		 */
		System.out.println(str1);
		String threadid = Thread.currentThread().getName();
		try {
			Connection connection1 = newConnection();
			Statement statement = null;
			ResultSet resultSet = null;
			
			Vector<String> cities = new Vector<String>();
		
			try {
				statement = connection1.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY);
				statement.setFetchSize(Integer.MIN_VALUE);

				resultSet = statement
						.executeQuery("SELECT distinct(city),state_code from cities_extended WHERE state_code='" + str1
								+ "' order by state_code");
				// city list of one state
				
				while (resultSet.next()) {
					// we need to use '-' to replace ' '
					cities.add(resultSet.getString("city").toLowerCase().trim().replace(" ", "-"));
				}
				connection1.close();
			} catch (SQLException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			
			System.setProperty("webdriver.gecko.driver", "tools\\geckodriver.exe");
			//System.setProperty("webdriver.gecko.driver", "geckodriver");
			
			FirefoxOptions firefoxOptions = new FirefoxOptions();
			
			/*
			 *  try to use memory as least as possible
			 *  disable cache and images
			 */
			FirefoxProfile profile = new FirefoxProfile();
			profile.setPreference("browser.cache.disk.enable", false);
			profile.setPreference("network.http.use-cache", false);
			profile.setPreference("browser.cache.memory.enable", false);
			profile.setPreference("permissions.default.image", 2);
			
			firefoxOptions.addArguments("--headless");
			WebDriver driver = new FirefoxDriver(firefoxOptions);
			int currentLife = 0;
			
			
			
			boolean driverclosed = false;
			String stateCode = str1.toLowerCase();
			
			boolean isInterrupt = false;
			Vector<String> visitedCities = new Vector<String>();
			File file = new File("visited/"+stateCode+".txt");
			if(file.exists()){
				isInterrupt = true;
				try {
					Reader reader = new FileReader(file);
					BufferedReader bufferedReader = new BufferedReader(reader);
					String line = "";
					line = bufferedReader.readLine();
					visitedCities.add(line);
					while (line!=null && !line.equals("")) {
						line = bufferedReader.readLine();
						visitedCities.add(line);
					}
					bufferedReader.close();
					reader.close();
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				}
			}
			
			Vector<String> alllHomeLinks = new Vector<String>();
			for (String city : cities) {
				System.out.println(threadid+": "+city);
				// this try to catch firefox crash error
				try {
					if (driverclosed) {
						driver = new FirefoxDriver(firefoxOptions);
						driverclosed = false;
					}
					
					if(isInterrupt && visitedCities.contains(city))
						continue;
					
					
					// get total number of pages
					int totalPages = 1;
					String page = null;
					
					try {
						if(++currentLife == life){
							driver.quit();
							driver = new FirefoxDriver(firefoxOptions);
							currentLife = 0;
						}
						driver.get("https://www.homes.com/" + city + "-" + stateCode + "/homes-for-" + recordType + "/");
						
						//Thread.sleep((int)(500+Math.random()*500));
						(new WebDriverWait(driver, 15)).until(ExpectedConditions.presenceOfElementLocated(
								By.cssSelector("h1.title")));
						
						String[] element = driver.findElement(By.cssSelector(".pagination__page-buttons")).getText().split("\n");
						for (String s : element) {
							s = s.replaceAll("[^0-9]", "");
							int sint = 0;
							if(!s.equals(""))
								sint = Integer.parseInt(s);
							if( sint > totalPages)
								totalPages = sint; 
						}
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					
					System.out.println(threadid + ": total_pages=" + totalPages);
					
					
					boolean is_refreshed = false;
					boolean is_second_access = false;
					
		
					Vector<String> homeDetailLinks = new Vector<String>();
					for (int i = 1; i <= totalPages; i++) {
						try {
							if (is_second_access) {
								is_second_access = false;
								is_refreshed = false;
							}
							if(++currentLife == life){
								driver.quit();
								driver = new FirefoxDriver(firefoxOptions);
								currentLife = 0;
							}
							driver.get("https://www.homes.com/" + city + "-" + stateCode + "/homes-for-" + recordType + "/p" + i);
							
							System.out.println(threadid + 
									": https://www.homes.com/" + city + "-" + stateCode + "/homes-for-" + recordType + "/p" + i);
							//Thread.sleep((int)(500+Math.random()*500));
							(new WebDriverWait(driver, 15)).until(ExpectedConditions.presenceOfElementLocated(
									By.cssSelector("h1.title")));

							/*
							 *  to avoid get similar homes near this city (only occur in one page)
							 */
							List<WebElement> links = driver.findElement(By.cssSelector(".property-list__inner-width"))
									.findElements(By.tagName("a"));
							for(WebElement link : links){
								String linkhref = link.getAttribute("href");
								if (linkhref != null && linkhref.startsWith("https://www.homes.com/property/")
										&& ((recordType.equals("rent") && linkhref.endsWith("#listing_status=FOR_RENT"))
												|| recordType.equals("sale"))) {
//								if (linkhref != null && (recordType.equals("sale") && 
//										linkhref.startsWith("https://www.homes.com/property/")
//										|| (recordType.equals("rent") && linkhref.startsWith("/property/")
//												&& linkhref.endsWith("#listing_status=FOR_RENT")))) {

									// if current link is added into vector, check next
									if (alllHomeLinks.contains(linkhref)) {
										continue;
									}
									homeDetailLinks.add(linkhref);
									alllHomeLinks.add(linkhref);
								}
							}
		
							if (is_refreshed)
								is_refreshed = false;
							System.out.println(threadid + ": links #: " + homeDetailLinks.size());
		
						} catch (Exception e) {
							e.printStackTrace();
							if (!is_refreshed) {
								is_refreshed = true;
								//driver.navigate().refresh();
								i--;
							} else {
								is_second_access = true;
							}
						}
		
					}
		
					// to give not-found-page one more chance
					is_refreshed = false;
					is_second_access = false;
		
					for (int k = 0; k < homeDetailLinks.size(); k++) {
						try {
							if (is_second_access) {
								is_refreshed = false;
								is_second_access = false;
							}
							if (is_refreshed) {
								is_second_access = true;
							}
							String home_url = homeDetailLinks.get(k);
							System.out.println(threadid + ": " + home_url);
							if(++currentLife == life){
								driver.quit();
								driver = new FirefoxDriver(firefoxOptions);
								currentLife = 0;
							}
							driver.get(home_url);
		
							//Thread.sleep((int)(500+Math.random()*500));
							//WebElement element = 
							(new WebDriverWait(driver, 15)).until(ExpectedConditions.presenceOfElementLocated(
									By.cssSelector("h2.property-info__title:nth-child(2)")));
							//System.out.println(threadid+": "+element.getText());
							page = null;
							page = driver.getPageSource();
							
		
							// download page source codes			
							if (page != null) {
								if(recordType.equals("rent")){
									FileWriter fileWriter = new FileWriter("rent_pages/" + city + "_" + stateCode + "_"
											+ k +".txt", false);
									BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
									bufferedWriter.write("" + page);
									bufferedWriter.close();
									fileWriter.close();
								}
									
								else if(recordType.equals("sale")){
									FileWriter fileWriter = new FileWriter("source_codes/" + city + "_" + stateCode + "_"
											+ k +".txt", false);
									BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
									bufferedWriter.write("" + page);
									bufferedWriter.close();
									fileWriter.close();
								}
								
							}
		
						} 
						catch (Exception e) {
							// TODO: handle exception
								e.printStackTrace();
								if (!is_refreshed) {
									//driver.navigate().refresh();
									is_refreshed = true;
									k = (k != 0) ? --k : k;
								} else {
									try {
										FileWriter fileWriter = new FileWriter("check_url.txt", true);
										BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
										bufferedWriter.write(homeDetailLinks.get(k) + "\r\n");
										bufferedWriter.close();
										fileWriter.close();
									} catch (Exception e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									driver.quit();
									currentLife = 0;
									driver = new FirefoxDriver(firefoxOptions);
									currentLife++;
								}
						}
					}
					
					// we have crawled whole data for this city, hence, write it into file.
					try {
						FileWriter fileWriter = new FileWriter("visited/" + stateCode + ".txt", true);
						BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
						bufferedWriter.write(city + "\r\n");
						bufferedWriter.close();
						fileWriter.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
					driver.quit();
					currentLife = 0;
					driverclosed = true;
					System.out.println(threadid);
					
					try {
						FileWriter fileWriter = new FileWriter("city-error-log.txt", true);
						BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
						Date date = new Date(System.currentTimeMillis());
						SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
						bufferedWriter.write(city + " " + stateCode + df.format(date) + "\r\n" + e.getMessage() + "\r\n");
						bufferedWriter.close();
						fileWriter.close();
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
			
			driver.quit();
		}
		catch (Exception e) {
			e.printStackTrace();
			try {
				FileWriter fileWriter = new FileWriter("state-error-log.txt", true);
				BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
				Date date = new Date(System.currentTimeMillis());
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
				bufferedWriter.write(str1 + df.format(date) + "\r\n" + e.getMessage() + "\r\n");
				bufferedWriter.close();
				fileWriter.close();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
}