package com.ucr.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.Vector;

public class Parser {

	private String recordType;
	private int threadNum;
	private String filePath;
	
	public Parser(String recordType, int threadNum, String filePath){
		this.recordType = recordType;
		this.threadNum = threadNum;
		this.filePath = filePath;
	}
	
	public void runParser(){
		
		Vector<String> ids = new Vector<String>();
		Vector<String> codes = new Vector<String>();
		File keysfile = new File(filePath);
		if(keysfile.exists()){
			try {
				Reader reader = new FileReader(keysfile);
				BufferedReader bufferedReader = new BufferedReader(reader);
				String line = "";
				line = bufferedReader.readLine();
				while (line!=null && !line.equals("")) {
					if(line.trim().equalsIgnoreCase("id:")){
						line = bufferedReader.readLine().trim();
						if(line!=null && !line.equals(""))
							ids.add(line);
					}else if (line.trim().equalsIgnoreCase("code:")) {
						line = bufferedReader.readLine().trim();
						if(line!=null && !line.equals(""))
							codes.add(line);
					}
					line = bufferedReader.readLine();
				}
				bufferedReader.close();
				reader.close();
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}
		
		int threadsTotal = threadNum * ids.size();
		
		
		String path = "";
		if(recordType.equals("sale"))
			path = "source_codes";
		else if(recordType.equals("rent"))
			path = "rent_pages";
		File fileObj = new File(path);
		File[] files = fileObj.listFiles();
		Vector<Vector<File>> lists = new Vector<Vector<File>>();
		for (int i = 0; i < threadsTotal; i++) {
			lists.add(new Vector<File>());
		}
		int index = 0;
		for(File file : files){
			lists.get(index % threadsTotal).add(file);
			index++;
		}
		
		for(int i = 0; i < threadsTotal; i++){
			ParserWorker worker = new ParserWorker(lists.get(i), recordType, ids.get(i % ids.size()), codes.get(i % codes.size()));
			System.out.println(lists.get(i).size());
			Thread thread = new Thread(worker);
			thread.start();
		}
		
	}
}
