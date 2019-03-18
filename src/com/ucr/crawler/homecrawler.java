package com.ucr.crawler;

import java.util.Vector;

public class homecrawler {

	/*
	 * author: Minghao Xu
	 * parameters:
	 * 1. type: "crawler" or "parser"
	 * 2. record type: "rent" or "sale"
	 * 3. number of threads
	 * 	  if first parameter is "crawler": the total number of threads
	 * 	  if first parameter is "sale": the # of threads for each API key
	 * 4. if first parameter is "crawler":
	 * 		group id: range from 1 to 5. Otherwise, only one group
	 * 	  if first parameter is "sale":
	 * 		file path and name which contains Here Geocoding API keys
	 * 5. life of browser: range from 10 to 1000 
	 *    (destroy current browser and create a new one after visit assigned number of pages)
	 *    larger # threads => smaller life
	 *   smaller # threads => larger  life
	 */
	private static final String[] GROUP_ONE = { "DC", "AR", "MS", "ND", "SC", "MI", "IN", "KS", "MD", "NY" };
	private static final String[] GROUP_TWO = { "CO", "NC", "MT", "FL", "LA", "ME", "NJ", "TN", "NM", "AZ" };
	private static final String[] GROUP_THREE = { "VA", "SD", "VT", "PR", "UT", "WY", "CT", "HI", "WI", "NH" };
	private static final String[] GROUP_FOUR = { "DE", "RI", "TX", "MO", "KY", "WV", "PA", "OK", "AL", "NE" };
	private static final String[] GROUP_FIVE = { "OR", "AK", "CA", "WA", "MA", "IL", "MN", "GA", "IA", "OH", "NV", "ID"};

	public static void main(String args[]) {

		String type = args[0];
		String recordType = args[1];
		int threadNum = Integer.parseInt(args[2]);
		if (type.equals("crawler")) {
			int n = Integer.parseInt(args[3]);
			int life = Integer.parseInt(args[4]);
			String[] states = {};
			if (n == 1) {
				states = GROUP_ONE;
			} else if (n == 2){
				states = GROUP_TWO;
			} else if (n == 3) {
				states = GROUP_THREE;
			} else if (n == 4) {
				states = GROUP_FOUR;
			} else if (n == 5) {
				states = GROUP_FIVE;
			} else {
				/*
				 * crawl all states
				 */
				states = new String[GROUP_ONE.length + GROUP_TWO.length + GROUP_THREE.length + GROUP_FOUR.length
						+ GROUP_FIVE.length];
				System.arraycopy(GROUP_ONE, 0, states, 0, GROUP_ONE.length);
				System.arraycopy(GROUP_TWO, 0, states, GROUP_ONE.length, GROUP_TWO.length);
				System.arraycopy(GROUP_THREE, 0, states, GROUP_ONE.length + GROUP_TWO.length, GROUP_THREE.length);
				System.arraycopy(GROUP_FOUR, 0, states, GROUP_ONE.length + GROUP_TWO.length + GROUP_THREE.length,
						GROUP_FOUR.length);
				System.arraycopy(GROUP_FIVE, 0, states,
						GROUP_ONE.length + GROUP_TWO.length + GROUP_THREE.length + GROUP_FOUR.length,
						GROUP_FIVE.length);
			}
			
			if(threadNum == 1){
				// do not do multi-threads
				for(String str1 : states){
					Worker.crawlhomes(str1, recordType,life);
				}
			}else{
				// do multi-threads
				for (int i = 0; i < threadNum; i++) {
					Vector<String> currentStates = new Vector<String>();
					int bucket = states.length / threadNum;
					if(i != threadNum - 1){
						for (int j = i * bucket; j < (i+1)*bucket; j++) {
							currentStates.add(states[j]);
						}
					}else {
						for (int j = i * bucket; j < states.length; j++) {
							currentStates.add(states[j]);
						}
					}
					Worker worker = new Worker(currentStates, recordType,life);
					Thread thread = new Thread(worker);
					thread.start();
				}
			}
		}
		else if (type.equals("parser")) {
			
			String filePath = args[3].trim();
			Parser parser = new Parser(recordType,threadNum,filePath);
			parser.runParser();
		
			
		}
	}
}
