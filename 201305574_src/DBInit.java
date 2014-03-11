import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.TreeMap;


public class DBInit {

	public static TreeMap<String, TreeMap<Integer, PageInfo>> tablemapping = new TreeMap<String, TreeMap<Integer, PageInfo>>();

	public static TreeMap<String, TreeMap<Integer, PageInfo>> createTablesMapping(List<String> listtablename) {
		for(String tablename : listtablename) {
			////System.out.println("Argument "+listtablename);
			TreeMap<Integer, PageInfo> mappageinfo = createMappingPageinfo(tablename);
		tablemapping.put(tablename, mappageinfo);
		////System.out.println("tablemapping" + tablemapping);
		}
		return tablemapping;
	}
	
	
	public static TreeMap<Integer, PageInfo> createMappingPageinfo(String tablename) {
		File tablefile = DBTools.getTableFile(tablename, DBOutput.CSV);
		TreeMap<Integer, PageInfo> mapPageInfo = new TreeMap<Integer, PageInfo>();
		////System.out.println("path"+tablefile.getAbsolutePath());
		FileInputStream fis = null;
		try {
			 fis = new FileInputStream(tablefile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			PageInfo pi = new PageInfo();
			int recordid = -1;
			int currentpagesize = 0;
			int recordlength = 0;
			int pageno = 0;
			int seekpoint = 0;
			pi.setPageno(pageno);
			pi.setStartrecordid(0);
			pi.setBeiginpoint(seekpoint);
				RandomAccessFile raf = new RandomAccessFile(tablefile, "r");
				String recordcontent = "";
				while((recordcontent=raf.readLine())!=null) {
				recordlength = recordcontent.length();
					if(currentpagesize+recordlength < DBConfig.pagesize) {
						
							currentpagesize = currentpagesize + recordlength + 1;
							recordid++;
							//System.out.println("consider newline "+currentpagesize+"-1-"+recordid);
						} else if(currentpagesize+recordlength == DBConfig.pagesize) {
							currentpagesize = currentpagesize + recordlength;
							recordid++;
							//System.out.println(currentpagesize+"-2-"+recordid);
						} else {
							pi.setEndrecordid(recordid);
							pi.setPagesize(currentpagesize);
							pi.setEndpoint(seekpoint);
							mapPageInfo.put(pageno, pi);
							//System.out.println(""+pi);
							pi = new PageInfo();
							pageno++;
							if(recordlength < DBConfig.pagesize) {
							currentpagesize = recordlength + 1;
							//System.out.println(currentpagesize+"-1--"+(recordid+1));
							} else {
								currentpagesize = recordlength;
								//System.out.println(currentpagesize+"---"+(recordid+1));
							}
							pi.setPageno(pageno);
							recordid++;
							pi.setStartrecordid(recordid);
							pi.setBeiginpoint(seekpoint);	
							
				}
					seekpoint = seekpoint+recordlength+1;
					//System.out.println("seekpoint="+seekpoint);
			}
		//	//System.out.println("Record id"+recordid);
			pi.setEndrecordid(recordid);
			pi.setPagesize(currentpagesize);
			pi.setEndpoint(seekpoint);
			mapPageInfo.put(pageno, pi);
		//System.out.println(pageno+" "+pi);
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		////System.out.println("Map info" + mapPageInfo);
	return mapPageInfo;
	}
	
}
	
