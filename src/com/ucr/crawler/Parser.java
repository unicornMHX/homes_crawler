package com.ucr.crawler;

import java.io.File;
import java.util.Vector;

public class Parser {

	private String recordType;
	private int threadNum;
	
	public Parser(String recordType, int threadNum){
		this.recordType = recordType;
		this.threadNum = threadNum;
	}
	
	public void runParser(){
		String path = "";
		if(recordType.equals("sale"))
			path = "source_codes";
		else if(recordType.equals("rent"))
			path = "rent_pages";
		File fileObj = new File(path);
		File[] files = fileObj.listFiles();
		Vector<Vector<File>> lists = new Vector<Vector<File>>();
		for (int i = 0; i < threadNum; i++) {
			lists.add(new Vector<File>());
		}
		int index = 0;
		for(File file : files){
			lists.get(index % threadNum).add(file);
			index++;
		}
		
		for(int i = 0; i < threadNum; i++){
			ParserWorker worker = new ParserWorker(lists.get(i), recordType);
			Thread thread = new Thread(worker);
			thread.start();
		}
		
	}
}
