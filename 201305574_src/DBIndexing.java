import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class DBIndexing {

	HashMap<String, String> tablecolumnindex = new HashMap<String, String>();
	
	public static HashMap<String, ArrayList<Integer>> createIndex(String table,
			String column) {
		File tablefile = DBTools.getTableFile(table, DBOutput.CSV);
		HashMap<String, ArrayList<Integer>> colrecords = new HashMap<String, ArrayList<Integer>>();
		int columno = DBTools.getColumnno(table, column);
		String datatype = DBTools.allentries.get(table).get(column);
		int currentrecord = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(tablefile));
			String record = null;
			while ((record = br.readLine()) != null) {
				String values[] = record.split(",");
				String colvalue = values[columno];
				ArrayList<Integer> records = colrecords.get(colvalue);
				if (records != null) {
					records.add(currentrecord);
					colrecords.put(colvalue, records);
				} else {
					records = new ArrayList<Integer>();
					records.add(currentrecord);
					colrecords.put(colvalue, records);
				}
				currentrecord++;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		TreeMap finalcolrecords = null;
		if (datatype.equalsIgnoreCase("integer")) {
		//	System.out.println("Final Intger.............."+finalcolrecords);
			Set<String> colvalues = colrecords.keySet();
			finalcolrecords = new TreeMap<Integer, List<Integer>>();
			for (String colvalue : colvalues) {
				String colvaluekey = colvalue.substring(1, colvalue.length()-1);
		//		System.out.println("colvalue "+colvalue);
				finalcolrecords.put(Integer.valueOf(colvaluekey),
						colrecords.get(colvalue));
			}
		} else if (datatype.equalsIgnoreCase("float")) {
		//	System.out.println("Final "+finalcolrecords);
			Set<String> colvalues = colrecords.keySet();
			finalcolrecords = new TreeMap<Float, List<Integer>>();
			for (String colvalue : colvalues) {
				String colvaluekey = colvalue.substring(1, colvalue.length()-1);
					finalcolrecords.put(Float.valueOf(colvaluekey),
						colrecords.get(colvalue));
			}
		} else {
		//	System.out.println("Final "+finalcolrecords);
			finalcolrecords = new TreeMap<String, List<Integer>>();
			finalcolrecords.putAll(colrecords);
		}
		StringBuffer filepath = new StringBuffer();
		filepath.append(DBConfig.pathforData);
		filepath.append(File.separator);
		filepath.append(table);
		filepath.append(column);
		filepath.append(".idx");
		try {
			
			FileOutputStream fout = new FileOutputStream(new File(
					filepath.toString()));
			ObjectOutputStream oos = new ObjectOutputStream(fout);
			//System.out.println("Final "+finalcolrecords);
			
			oos.writeObject(finalcolrecords);
			oos.close();
			fout.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return colrecords;
	}

	/**
	 * 0 --> = 1 --> > 2 --> >= 3 --> < 4 --> <= 5 --> !=
	 **/
	public static List<Integer> searchIntegerindex(TreeMap<Integer, List<Integer>> colrecords,
			Integer value, int op) {
		NavigableMap<Integer, List<Integer>> selectedmap = null;
		List<Integer> records = null;
		if (op == 0) {
			records =  colrecords.get(value);
		} else if (op == 1) {
			selectedmap = colrecords.tailMap(value, false);
		//	System.out.println("selected "+selectedmap);
		} else if (op == 2) {
			selectedmap = colrecords.tailMap(value, true);
		} else if (op == 3) {
			selectedmap = colrecords.headMap(value, false);
		} else if (op == 4) {
			selectedmap = colrecords.headMap(value, true);
		} else if (op == 5) {
			colrecords.remove(value);
			selectedmap = colrecords;
		}
		if (selectedmap != null) {
			records = new ArrayList<Integer>();
			Set<Integer> keys1 = selectedmap.keySet();
			for (Integer key : keys1) {
				records.addAll(selectedmap.get(key));
			}
		}
		
		return records;
	}
	
	/**
	 * 0 --> = 1 --> > 2 --> >= 3 --> < 4 --> <=
	 **/
	public static List<Integer> searchFloatindex(TreeMap<Float, List<Integer>> colrecords,
			Float value, int op) {
		NavigableMap<Float, List<Integer>> selectedmap = null;
		List<Integer> records = null;
		if (op == 0) {
			records =  colrecords.get(value);
		} else if (op == 1) {
			selectedmap = colrecords.tailMap(value, false);
		} else if (op == 2) {
			selectedmap = colrecords.tailMap(value, true);
		} else if (op == 3) {
			selectedmap = colrecords.headMap(value, false);
		} else if (op == 4) {
			selectedmap = colrecords.headMap(value, true);
		} else if (op == 5) {
			colrecords.remove(value);
			selectedmap = colrecords;
		}
		if (selectedmap != null) {
			records = new ArrayList<Integer>();
			Set<Float> keys = selectedmap.keySet();
			for (Object key : keys) {
				records.addAll(selectedmap.get(key));
			}
		}
		
		return records;
	}

	/**
	 * 0 -- Case sensitive 1 --Case insesitive
	 **/

	public static List<Integer> searchStringindex(
			TreeMap<String, List<Integer>> colrecords, String value, int op) {
	//System.out.println("value "+value);
		List<Integer> records = null;
		List<Integer> records1 = null;
		if(value.indexOf('"')==-1) {
			StringBuffer sb = new StringBuffer("\"");
			if(value.charAt(0)=='\'') {
				sb.append(value.substring(1, value.length()-1));
			}
			else {
			sb.append(value);
			}
			sb.append("\"");
			value = sb.toString();
		}
		if (op == 0) {
		//	System.out.println("0");
			records = colrecords.get(value);
		//	records1 = colrecords.get("\""+value+"\"");
		} else if (op == 1) {
		//	System.out.println("1");
			Set<String> keys = colrecords.keySet();
			for (String key : keys) {
				if (key.equalsIgnoreCase(value)) {
					if (records == null) {
						records = new ArrayList<Integer>();
					}
					records.addAll(colrecords.get(key));
				//	System.out.println("equal");
					
				}
			}
		}
	//	System.out.println("rec"+records);
	//	System.out.println("rec1"+records1);
		return records;
	}

	public static void main(String[] args) {
		DBConfig.loadConfig("/media/Local Disk/Sem2/DB/config.txt");
		DBTools.getAllEntries("/media/Local Disk/Sem2/DB/config.txt");
		System.out.println(DBIndexing.createIndex("student", "sname"));
		System.out.println(DBIndexing.createIndex("student", "sid"));
		System.out.println(DBIndexing.createIndex("student", "sbatch"));
	//	TreeMap map = loadindexintoMemory("student", "sname");
	//	System.out.println("Map "+map);
	
		//System.out.println(searchStringindex(map, "gaurav agrawal", 1));
		TreeMap map = loadindexintoMemory("student", "sid");
		System.out.println(searchIntegerindex(map, 5580, 0));
		System.out.println(searchIntegerindex(map, 5576, 1));
		System.out.println(searchIntegerindex(map, 5576, 2));
		System.out.println(searchIntegerindex(map, 5576, 3));
		System.out.println(searchIntegerindex(map, 5576, 4));
		System.out.println(searchIntegerindex(map, 5576, 5));
	}
	
	public static TreeMap loadindexintoMemory(String table, String column) {
		StringBuffer filepath = new StringBuffer();
		filepath.append(DBConfig.pathforData);
		filepath.append(File.separator);
		filepath.append(table);
		filepath.append(column);
		filepath.append(".idx");
		String datatype = DBTools.allentries.get(table).get(column);
	//	System.out.println("All entires "+DBTools.allentries);
	//	System.out.println("data type"+datatype);
		try {
			File indexfile = new File(
					filepath.toString());
			if(!indexfile.exists()) {
				createIndex(table, column);
			}
			FileInputStream fin = new FileInputStream(indexfile);
			ObjectInputStream oos = new ObjectInputStream(fin);
			Object obj = oos.readObject();
			if(datatype.equalsIgnoreCase("integer")) {
			//	System.out.println("Object "+obj);
				
				return (TreeMap<Integer, List<Integer>>)obj;
			} else if(datatype.equalsIgnoreCase("float")) {
				return (TreeMap<Float, List<Integer>>)obj;
			} else if(datatype.startsWith("varchar")) {
				return (TreeMap<String, List<Integer>>)obj;
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}
}
