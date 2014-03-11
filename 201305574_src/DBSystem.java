import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.TreeMap;

public class DBSystem {
		
	
	TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
	
	public static TreeMap<String, TreeMap<Integer, PageInfo>> tablemapping = new TreeMap<String, TreeMap<Integer, PageInfo>>();	
		
	public void readConfig(String configFilePath) {
	//	System.out.println("Read config gile "+configFilePath);
		DBConfig.loadConfig(configFilePath);
		DBTools.getAllEntries(configFilePath);
	}

	public static void populateDBInfo() {
		tablemapping = DBInit.createTablesMapping(DBConfig.listtablename);
	}
	


	public static String getRecord(String tableName, int recordId) {
		PageInfo pi = DBTools.retrievePageInfo(tableName, recordId);
		String record = DBTools.getRecord(tableName, pi, recordId);
		return record;
	}
	
	public static void insertRecord(String tableName, String record) {
		DBTools.insertRecord(tableName, record);
	}

	/*
	Deteremine the type of the query (select/create) and
	invoke appropriate method for it.
	*/
	
	void queryType(String query) {
		sqlparser.sqltext = query;
		//System.out.println("Input: "+query);
		int ret = sqlparser.parse();
		if (ret == 0){
		for(int i=0;i<sqlparser.sqlstatements.size();i++){
		//	System.out.println("Output:");
		DBQueryParser.analyzeStmt(sqlparser.sqlstatements.get(i));
	//	System.out.println("");
		}
		}else{
		//	System.out.println("Testing: Exception!!!");
		//	System.out.println("Output:");
		System.out.println("Query Invalid");
		}	
	}

	void createCommand(String query) {
		sqlparser.sqltext = query;
		System.out.println("Input: "+query);
		int ret = sqlparser.parse();
		if (ret == 0){
		for(int i=0;i<sqlparser.sqlstatements.size();i++){
			System.out.println("Output:");
		DBQueryParser.analyzeCreateStmt((TCreateTableSqlStatement)sqlparser.sqlstatements.get(i));
	}
		} else {
			System.out.println("Output:");
			System.out.println("Query Invalid");
		
		}
	}

	/*
	Use any SQL parser to parse the input query. Perform all validations (table
	name, attributes, datatypes, operations). Print the query tokens as specified
	below.
	*/

	void selectCommand(String query) {
		sqlparser.sqltext = query;
		System.out.println("Input: "+query);
		int ret = sqlparser.parse();
		if (ret == 0){
		for(int i=0;i<sqlparser.sqlstatements.size();i++){
			//System.out.println("Output:");
		SelectQuery sq = DBQueryParser.selectCommand((TSelectSqlStatement)sqlparser.sqlstatements.get(i));
		DBTools.executeSelectQuery(sq);
	}
		} else {
			//System.out.println("Output:");
			System.out.println("Query Invalid");
		}
		}

	public static void main(String[] args) {
	String systemconfig = args[0];
		//String systemconfig = "/media/Local Disk/Sem2/DB/config.txt";
		DBConfig.pathforConfig = systemconfig;
		//String inputfile = args[1];
		//String inputfile = "data/input.txt";
		//File f = new File(inputfile);
	//	System.out.println(""+f.getAbsolutePath());
	//	System.out.println("input--"+inputfile);
		DBSystem db = new DBSystem();
		db.readConfig(systemconfig);
		populateDBInfo();
	    try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String line = "";
			int nooflines = Integer.parseInt(br.readLine());
			for(int i=0;i<nooflines;i++) {
				line = br.readLine();
				//System.out.println("executing "+line);
				try {
				db.queryType(line);
				}catch (Exception e) {
					continue;
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		
	}
}
