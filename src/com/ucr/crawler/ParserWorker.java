package com.ucr.crawler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Vector;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class ParserWorker implements Runnable {

	private Vector<File> files;
	private String recordType;

	public ParserWorker(Vector<File> files, String recordType) {
		// TODO Auto-generated constructor stub
		this.files = files;
		this.recordType = recordType;
	}

	public void run() {
		try {
			int total_num = 0;
			String threadid = Thread.currentThread().getName();
			for (File file : files) {
				String string = "";
				try {
					string = getContent(file);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Document document = Jsoup.parse(string);

				String city = null;
				String state = null;
				String home_url = null;
				String property_type = null;
				String streetaddr = null;
				String zipcode = null;
				String yearbuilt = null;
				String garage = null;
				String lotsize = null;
				String stories = null;
				String pool = null;
				String style = null;
				String stainlessAppliances = null;
				String fireplace = null;
				String description = null;
				String datelisted = null;
				String saleprice = null;

				String MLSNumber = null;
				String dogAllowed = null;
				String catAllowed = null;
				Timestamp crawl_time = null;
				
				

				/*
				 * if home is a apt with different floor plans we need to insert
				 * multiple rows into database
				 */
				Vector<String> floor_plan = new Vector<String>();
				Vector<String> numbed = new Vector<String>();
				Vector<String> num_bath_full = new Vector<String>();
				Vector<String> rentalprice_min = new Vector<String>();
				Vector<String> rentalprice_max = new Vector<String>();
				Vector<String> size = new Vector<String>();

				/*
				 * crawl basic info: crawl_time, website, rental_price, address,
				 * zipcode numbed, num_bath, size
				 */
				home_url = document.select("link[rel=canonical]").attr("href");

				/*
				 * check duplicates: whether this record appears in database
				 * using home_url and record_type
				 */
				boolean flag = false;
				Connection connection = newConnection();
				Statement statement = null;
				ResultSet resultSet = null;
				statement = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY);
				statement.setFetchSize(Integer.MIN_VALUE);
				resultSet = statement.executeQuery("SELECT count(*) as num from homes_2018 WHERE home_url='" + home_url
						+ "' and record_type='" + recordType + "'");
				while (resultSet.next()) {
					int count = resultSet.getInt("num");
					if (count != 0)
						flag = true;
				}
				connection.close();
				if (!flag) {
					++total_num;
					System.out.println(threadid + ": " + total_num + " " + file + " " + home_url);
					crawl_time = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

					/*
					 * check if the home has multiple floor plans If so, we need
					 * to crawl details for each floor plan (Vector) Otherwise,
					 * we just insert one row into database
					 */
					if (!document.select("div[class=floorplan__sub-heading]").isEmpty()) {
						for (int i = 0; i < document.select("div[class=floorplan__sub-heading]").size(); i++) {
							Element element = document.select("div[class=floorplan__sub-heading]").get(i);
							/*
							 * we need get next element to get contents since
							 * floor plan and details are not in one element
							 */
							Element nextElement = element.nextElementSibling();

							if (!nextElement.select("div[class=floorplan-property-card]").isEmpty()) {

								for (int j = 0; j < nextElement.select("div[class=floorplan-property-card]")
										.size(); j++) {

									floor_plan.add(element.text());
									Element detailCard = nextElement.select("div[class=floorplan-property-card]")
											.get(j);
									String priceinfo = detailCard.select("div[class=property-details__price]").text()
											.split("\\/")[0];
									String[] part = priceinfo.split("-");
									if (part.length == 1) {
										rentalprice_min.add(part[0].replaceAll("[^0-9]", ""));
										rentalprice_max.add("-1");
									} else if (part.length == 2) {
										rentalprice_min.add(part[0].replaceAll("[^0-9]", ""));
										rentalprice_max.add(part[1].replaceAll("[^0-9]", ""));
									} else {
										rentalprice_min.add("-1");
										rentalprice_max.add("-1");
									}
									if (floor_plan.get(i).equalsIgnoreCase("studio")) {
										numbed.add("1");
										num_bath_full.add(detailCard.select("div[class=property-details__beds] > span")
												.get(0).text().replaceAll("[^0-9]", ""));
										size.add(detailCard.select("div[class=property-details__beds] > span").get(1)
												.text().split("\\s")[0].replaceAll("[^0-9]", ""));
									} else {
										numbed.add(detailCard.select("div[class=property-details__beds] > span").get(0)
												.text().replaceAll("[^0-9]", ""));
										num_bath_full.add(detailCard.select("div[class=property-details__beds] > span")
												.get(1).text().replaceAll("[^0-9]", ""));
										size.add(detailCard.select("div[class=property-details__beds] > span").get(2)
												.text().split("\\s")[0].replaceAll("[^0-9]", ""));
									}
								}
							}

						}
					} else {
						floor_plan.add("N/A");
						if (!document
								.select("section[class=listing-details__property-info page__section page__inset property-info"
										+ " scroll-appear scroll-appear--animate-children scroll-appear-active] > aside")
								.isEmpty()) {
							
							/*
							 * we firstly find title information (since the accuracy of address info is higher).
							 * If not exist, we find another position which may store address info. 
							 * =====================================START=====================================================
							 */
							String streetaddr_full = "";
							if (!document.select("head > title").isEmpty()) {
								String title = document.select("head > title").text();
								streetaddr_full = title.split("\\|")[0];
							}else {
								streetaddr_full = document.select("section[class=listing-details__property-info"
										+ " page__section page__inset property-info"
										+ " scroll-appear scroll-appear--animate-children scroll-appear-active] > h1").text();
							}
							
							String address_parts[] = streetaddr_full.split(",");
							if (address_parts.length == 3) {
								/*
								 * sample style: 25 Arthur Drive 1, South Windsor,
								 * CT 06074 | Homes.com
								 */
								streetaddr = address_parts[0];
								city = address_parts[1].replaceAll("[^a-zA-Z\\s]", "").trim();
								state = address_parts[2].replaceAll("[^a-zA-Z]", "");
								zipcode = address_parts[2].replaceAll("[^0-9]", "");
							} else if (address_parts.length == 2) {
								/*
								 * sample style: House For Rent in
								 * Lauderdale-by-the-sea, FL 33308 - 200005731542 |
								 * Homes.com
								 */
								streetaddr = address_parts[0];
								city = streetaddr.split(" in ")[1].trim();
								state = address_parts[1].trim().split("\\s")[0].trim();
								zipcode = address_parts[1].split(" - ")[0].replaceAll("[^0-9]", "");
							} else if (address_parts.length == 4) {
								streetaddr = address_parts[0];
								city = (address_parts[1] + address_parts[2]).replaceAll("[^a-zA-Z\\s]", "").trim();
								state = address_parts[3].replaceAll("[^a-zA-Z]", "");
								zipcode = address_parts[3].replaceAll("[^0-9]", "");
							}
							
							//============================================END==============================================
							
							String parts[] = document
									.select("section[class=listing-details__property-info page__section page__inset"
											+ " property-info scroll-appear scroll-appear--animate-children scroll-appear-active]"
											+ " > aside"
											+ "> ul > li > p")
									.text().split(" ");
							if(recordType.equals("rent")){
								rentalprice_min.add(parts[0].replaceAll("[^0-9]", ""));
								if (parts.length > 2) {
									rentalprice_max.add(parts[2].replaceAll("[^0-9]", ""));
								} else {
									rentalprice_max.add("-1");
								}
							}else {
								rentalprice_min.add("-1");
								rentalprice_max.add("-1");
								saleprice = parts[0].replaceAll("[^0-9]", "");
							}
	
						} else {
							rentalprice_min.add("-1");
							rentalprice_max.add("-1");
							saleprice = "-1";
						}

						if (!document
								.select("section[class=listing-details__property-info page__section page__inset "
										+ "property-info scroll-appear scroll-appear--animate-children scroll-appear-active] "
										+ "> aside > ul > li[class=details__attribute details__bedrooms "
										+ "property-info__bedrooms]")
								.isEmpty()) {
							numbed.add(document
									.select("section[class=listing-details__property-info page__section "
											+ "page__inset property-info scroll-appear scroll-appear--animate-children "
											+ "scroll-appear-active] > aside > ul > li[class=details__attribute "
											+ "details__bedrooms property-info__bedrooms]")
									.text().replaceAll("[^0-9]", ""));
						} else {
							numbed.add("-1");
						}

						if (!document
								.select("section[class=listing-details__property-info page__section page__inset "
										+ "property-info scroll-appear scroll-appear--animate-children scroll-appear-active] "
										+ "> aside > ul > li[class=details__attribute "
										+ "details__bathrooms property-info__bathrooms]")
								.isEmpty()) {
							num_bath_full.add(document
									.select("section[class=listing-details__property-info page__section page__inset "
											+ "property-info scroll-appear scroll-appear--animate-children "
											+ "scroll-appear-active] > aside > ul > li[class=details__attribute "
											+ "details__bathrooms property-info__bathrooms]")
									.text().replaceAll("[^0-9]", ""));
						} else {
							num_bath_full.add("-1");
						}

						if (!document
								.select("section[class=page__content listing-details__content] > aside > ul > "
										+ "li[class=details__attribute "
										+ "details__square-footage property-info__square-footage]")
								.isEmpty()) {
							size.add(document
									.select("section[class=page__content listing-details__content] > aside > ul > "
											+ "li[class=details__attribute "
											+ "details__square-footage property-info__square-footage]")
									.text().replaceAll("[^0-9]", ""));
						} else {
							size.add("-1");
						}

					}
					
				

					// crawl overview/description
					if (!document.select("p[class=overview__description]").isEmpty()) {
						// get lower-case string
						description = document.select("p[class=overview__description]").text();
						String descriptionCopy = description.toLowerCase();

						if (descriptionCopy.contains("garage")) {
							garage = "1";
						} else {
							garage = "0";
						}

						if (descriptionCopy.contains(" story") || descriptionCopy.contains(" stories")) {
							stories = "1";
						} else {
							stories = "0";
						}

						if (descriptionCopy.contains("stainess")) {
							stainlessAppliances = "1";
						} else {
							stainlessAppliances = "0";
						}
					}

					// crawl rental details
					if (!document.select("section[class=home-details__list list] > ul > li > span").isEmpty()) {
						for (int i = 0; i < document
								.select("section[class=home-details__list list]" + " > ul > li > span")
								.size(); i = i + 2) {
							String temp = document
									.select("section[class=home-details__list list]" + " > ul > li > span").get(i)
									.text();
							if (14 == i && i + 1 < document
									.select("section[class=home-details__list list] > ul >" + " li > span").size()
									&& !document.select("section[class=home-details__list list]" + " > ul > li > span")
											.get(i + 1).text().replaceAll("[-]", "").equals("")) {
								i--;
								continue;
							}
							if (i <= 14) {
								if (temp.equalsIgnoreCase("property type")) {
									property_type = document
											.select("section[class=home-details__list list]" + " > ul > li > span")
											.get(i + 1).text().replaceAll("[-]", "");
								}
								if (temp.equalsIgnoreCase("exterior finish")) {
									style = document
											.select("section[class=home-details__list list]" + " > ul > li > span")
											.get(i + 1).text().replaceAll("[-]", "");
								}
								if (temp.equalsIgnoreCase("year built")) {
									yearbuilt = document
											.select("section[class=home-details__list list]" + " > ul > li > span")
											.get(i + 1).text().replaceAll("[-]", "");
								}
								if (temp.equalsIgnoreCase("lot size")) {
									lotsize = document
											.select("section[class=home-details__list list]" + " > ul > li > span")
											.get(i + 1).text().replaceAll("[-]", "");
								}
								if (temp.equalsIgnoreCase("fireplace")) {
									fireplace = document
											.select("section[class=home-details__list list]" + " > ul > li > span")
											.get(i + 1).text().replaceAll("[-]", "");
									fireplace = fireplace.equalsIgnoreCase("yes") ? "1" : "0";
								}
							} else {
								if (temp.equalsIgnoreCase("pool")) {
									if(i+1<document
											.select("section[class=home-details__list list]" + " > ul > li > span").size()){
										pool = document
												.select("section[class=home-details__list list]" + " > ul > li > span")
												.get(i + 1).text().replaceAll("[-]", "");
										pool = pool.equalsIgnoreCase("yes") ? "1" : "0";	
									}
								}
								if (temp.equalsIgnoreCase("garage")) {
									if(i+1<document
											.select("section[class=home-details__list list]" + " > ul > li > span").size()){
										garage = document
												.select("section[class=home-details__list list]" + " > ul > li > span")
												.get(i + 1).text().replaceAll("[-]", "");
										garage = garage.equalsIgnoreCase("yes") ? "1" : "0";	
									}
								}

								/*
								 * there are several types info to indicate the
								 * pets allowed info
								 */
								if (temp.equalsIgnoreCase("pets allowed") || temp.equalsIgnoreCase("pet friendly")) {
									String temp2 = document
											.select("section[class=home-details__list list]" + " > ul > li > span")
											.get(i + 1).text();
									if (temp2.equalsIgnoreCase("yes")) {
										dogAllowed = "1";
										catAllowed = "1";
									} else if (temp2.equalsIgnoreCase("no")) {
										dogAllowed = "0";
										catAllowed = "0";
									}
								}
								if (temp.equalsIgnoreCase("cats allowed")) {
									String temp2 = document
											.select("section[class=home-details__list list]" + " > ul > li > span")
											.get(i + 1).text();
									if (temp2.equalsIgnoreCase("yes")) {
										catAllowed = "1";
									} else if (temp2.equalsIgnoreCase("no")) {
										catAllowed = "0";
									}
								}
								if (temp.equalsIgnoreCase("dogs allowed")) {
									String temp2 = document
											.select("section[class=home-details__list list]" + " > ul > li > span")
											.get(i + 1).text();
									if (temp2.equalsIgnoreCase("yes")) {
										dogAllowed = "1";
									} else if (temp2.equalsIgnoreCase("no")) {
										dogAllowed = "0";
									}
								}
								if (temp.equalsIgnoreCase("pets") || temp.equalsIgnoreCase("pet")) {
									String temp2 = document
											.select("section[class=home-details__list list]" + " > ul > li > span")
											.get(i + 1).text();
									if (temp2.equalsIgnoreCase("no pets")) {
										catAllowed = "0";
										dogAllowed = "0";
									} else if (temp2.equalsIgnoreCase("cats allowed")) {
										catAllowed = "1";
									} else if (temp2.equalsIgnoreCase("dogs allowed")) {
										dogAllowed = "1";
									} else if (temp2.equalsIgnoreCase("no dogs allowed")) {
										dogAllowed = "0";
									} else if (temp2.equalsIgnoreCase("no cats allowed")) {
										catAllowed = "0";
									}
								}
							}

						}
					}

					// find MLS Number，date listed
					if (!document.select("section[class=mls__list sa-child sa-child--1 list] > ul > li > span")
							.isEmpty()) {
						for (int i = 0; i < document
								.select("section[class=mls__list " + "sa-child sa-child--1 list] > ul > li > span")
								.size(); i++) {
							if (document
									.select("section[class=mls__list " + "sa-child sa-child--1 list] > ul > li > span")
									.get(i).text().equalsIgnoreCase("MLS Number")) {
								MLSNumber = document.select(
										"section[class=mls__list " + "sa-child sa-child--1 list] > ul > li > span")
										.get(i + 1).text();
							}
							if (document
									.select("section[class=mls__list " + "sa-child sa-child--1 list] > ul > li > span")
									.get(i).text().equalsIgnoreCase("Listed")) {
								datelisted = document.select(
										"section[class=mls__list " + "sa-child sa-child--1 list] > ul > li > span")
										.get(i + 1).text();
								break;
							}
						}
					}

					// insert into database
					Connection connection2 = newConnection();
					for (int i = 0; i < floor_plan.size(); i++) {
						String sqlString = "insert ignore into homes_2018(website,home_url,property_type, record_type, "
								+ "streetaddr,city,state,zipcode,numbed,num_bath_full,rentalprice_min,rentalprice_max,"
								+ "yearbuilt,floor_plan,garage,lotsize,stories,size,pool,style,stainlessAppliances,"
								+ "fireplace,description,crawl_time,datelisted,dogAllowed,catAllowed,MLSNumber,saleprice) "
								+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
						PreparedStatement preparedStatement = connection2.prepareStatement(sqlString);
						preparedStatement.setString(1, "https://www.homes.com/");
						preparedStatement.setString(2, home_url.trim());
						if (property_type != null && !property_type.equals("")) {
							preparedStatement.setString(3, property_type.trim());
						} else {
							preparedStatement.setNull(3, Types.VARCHAR);
						}
						preparedStatement.setString(4, recordType.trim());
						if (streetaddr != null && !streetaddr.equals("")) {
							preparedStatement.setString(5, streetaddr.trim());
						} else {
							preparedStatement.setNull(5, Types.VARCHAR);
						}
						if (city != null && !city.equals("")) {
							preparedStatement.setString(6, city.trim());
						} else {
							preparedStatement.setNull(6, Types.VARCHAR);
						}
						if (state != null && !state.equals("")) {
							preparedStatement.setString(7, state.trim());
						} else {
							preparedStatement.setNull(7, Types.VARCHAR);
						}
						if (zipcode != null && !zipcode.equals("")) {
							preparedStatement.setInt(8, Integer.parseInt(zipcode.trim()));
						} else {
							preparedStatement.setNull(8, Types.INTEGER);
						}
						if (!numbed.get(i).equals("")) {
							preparedStatement.setInt(9, Integer.parseInt(numbed.get(i).trim()));
						} else {
							preparedStatement.setInt(9, -1);
						}
						if (!num_bath_full.get(i).equals("")) {
							preparedStatement.setInt(10, Integer.parseInt(num_bath_full.get(i).trim()));
						} else {
							preparedStatement.setInt(10, -1);
						}
						if (!rentalprice_min.get(i).equals("")) {
							preparedStatement.setInt(11, Integer.parseInt(rentalprice_min.get(i).trim()));
						} else {
							preparedStatement.setInt(11, -1);
						}
						if (!rentalprice_max.get(i).equals("")) {
							preparedStatement.setInt(12, Integer.parseInt(rentalprice_max.get(i).trim()));
						} else {
							preparedStatement.setInt(12, -1);
						}
						if (yearbuilt != null && !yearbuilt.equals("")) {
							preparedStatement.setInt(13, Integer.parseInt(yearbuilt.trim()));
						} else {
							preparedStatement.setNull(13, Types.INTEGER);
						}
						if (!floor_plan.equals("")) {
							preparedStatement.setString(14, floor_plan.get(i).trim());
						} else {
							preparedStatement.setString(14, "N/A");
						}
						if (garage != null && !garage.equals("")) {
							preparedStatement.setInt(15, Integer.parseInt(garage.trim()));
						} else {
							preparedStatement.setNull(15, Types.INTEGER);
						}
						if (lotsize != null && !lotsize.equals("")) {
							preparedStatement.setInt(16, (int) Double.parseDouble(lotsize.trim()));
						} else {
							preparedStatement.setNull(16, Types.INTEGER);
						}
						if (stories != null && !stories.equals("")) {
							preparedStatement.setInt(17, Integer.parseInt(stories.trim()));
						} else {
							preparedStatement.setNull(17, Types.INTEGER);
						}
						preparedStatement.setInt(18,
								!size.get(i).equals("") ? Integer.parseInt(size.get(i).trim()) : -1);
						if (pool != null) {
							preparedStatement.setInt(19, Integer.parseInt(pool.trim()));
						} else {
							preparedStatement.setNull(19, Types.TINYINT);
						}
						if (style != null && !style.equals("")) {
							preparedStatement.setString(20, style.trim());
						} else {
							preparedStatement.setNull(20, Types.VARCHAR);
						}
						if (stainlessAppliances != null) {
							preparedStatement.setInt(21, Integer.parseInt(stainlessAppliances.trim()));
						} else {
							preparedStatement.setNull(21, Types.TINYINT);
						}
						if (fireplace != null) {
							preparedStatement.setInt(22, Integer.parseInt(fireplace.trim()));
						} else {
							preparedStatement.setNull(22, Types.TINYINT);
						}
						preparedStatement.setString(23, description != null ? description.trim() : null);
						preparedStatement.setTimestamp(24, crawl_time);
						if (datelisted != null) {
							SimpleDateFormat sdf = new SimpleDateFormat("mm/dd/yyyy");
							preparedStatement.setDate(25, new java.sql.Date(sdf.parse(datelisted).getTime()));
						} else {
							preparedStatement.setNull(25, Types.DATE);
						}
						if (dogAllowed != null) {
							preparedStatement.setInt(26, Integer.parseInt(dogAllowed.trim()));
						} else {
							preparedStatement.setNull(26, Types.TINYINT);
						}
						if (catAllowed != null) {
							preparedStatement.setInt(27, Integer.parseInt(catAllowed.trim()));
						} else {
							preparedStatement.setNull(27, Types.TINYINT);
						}
						preparedStatement.setString(28, MLSNumber);
						if(saleprice != null && !saleprice.equals("")){
							preparedStatement.setInt(29, Integer.parseInt(saleprice.trim()));
						} else {
							preparedStatement.setInt(29, -1);
						}
						preparedStatement.executeUpdate();
						preparedStatement.close();
					}
					connection2.close();

				} else {
					FileWriter fileWriter = new FileWriter("multiple_url.txt", true);
					BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
					bufferedWriter.write(home_url + "\r\n");
					bufferedWriter.close();
					fileWriter.close();
				}

			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}

	}

	private String getContent(File file) throws IOException {
		// TODO Auto-generated method stub
		String encoding = "UTF-8";
		byte[] filecontent = {};
		if (file.isFile() && file.exists()) {
			Long filelength = file.length();
			filecontent = new byte[filelength.intValue()];
			FileInputStream inputStream = new FileInputStream(file);
			inputStream.read(filecontent);
			inputStream.close();
		} else {
			System.err.println("cannot find!");
		}
		return new String(filecontent, encoding);
	}

	// for local window10 use only
//	private Connection newConnection() throws ClassNotFoundException, SQLException {
//		String driver = "com.mysql.cj.jdbc.Driver";
//		String url = "jdbc:mysql://localhost:3306/home_data?&useSSL=false&serverTimezone=UTC";
//		String username = "root";
//		String password = "123456";
//		Connection conn = null;
//
//		Class.forName(driver);
//		conn = DriverManager.getConnection(url, username, password);
//		return conn;
//	}
	
	// for sever:v75 use only
	private static Connection newConnection() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		String urldb = "jdbc:mysql://localhost/homeDB";
		String user = "mxu054";
		String password = "123456";
		Connection conn = DriverManager.getConnection(urldb, user, password);
		return conn;
	}
}
