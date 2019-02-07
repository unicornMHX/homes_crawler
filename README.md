# homes_crawler

Author: Minghao Xu
E-mail: mhao.xu@gmail.com

Project Description:
This is a java-based crawler which crawls rent homes and sale homes from HOMES.COM.

Environment:
Operationg System: Window 7+, Ubuntu 14+, Ubuntu 14+
selenium: 3.5.0
FireFox: 60.0+
jsoup: 1.9.1
geckodriver: 19.0/21.0

Parameters:
	* function type: "crawler" or "parser"
		crawler: downloading pages
		parser: parse pages and save useful data on database (MySQL)
	* record type: "rent" or "sale"
		rent: crawl/parse rent homes
		sale: crawl/parse sale homes
	* number of threads
	* group id (only for crawler)
		range from 1 to 5
		Otherwise, only one group
	* life of browser (only for crawler)
		range from 10 to 1000
		this number means destroy current browser and create a new one after visit assigned number of pages
	    tips: larger # threads => shorter life && smaller # threads => longer life