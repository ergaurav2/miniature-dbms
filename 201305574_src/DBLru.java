import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.TreeMap;

public class DBLru {

	LinkedList<String> listmemorypages = new LinkedList<String>();
	TreeMap<String, Integer> mapmemorypageTablepage = new TreeMap<String, Integer>();
	public TreeMap<String, TreeMap<Integer, String>> maptablerecords = new TreeMap<String, TreeMap<Integer, String>>();

	public TreeMap<String, TreeMap<Integer, String>> getMaptablerecords() {
		return maptablerecords;
	}

	public void setMaptablerecords(
			TreeMap<String, TreeMap<Integer, String>> maptablerecords) {
		this.maptablerecords = maptablerecords;
	}

	int pagesinuse = 0;

	void addPageintoMemory(String tablename, PageInfo pi, boolean newpage, boolean toprint) {
		//System.out.println("Page to be added into memory " + pi);
		//System.out.println("No of pages in use "+pagesinuse);
		int pageid = pi.getPageno();
		String tablepage = tablename + "_" + pageid;
		if (pagesinuse == DBConfig.numofPages) {
			//System.out.println("Main memory is full");
			//System.out.println("LRU Replacement started -->");
			//System.out.println("Current LRU "+listmemorypages);
			String pagereplace = listmemorypages.remove();
			//System.out.println("Replacing.."+pagereplace);
			maptablerecords.remove(pagereplace);
			String[] split = pagereplace.split("_");
			String oldtable = split[0];
			Integer oldpageid = Integer.parseInt(split[1]);
			Integer memoryid = mapmemorypageTablepage.get(pagereplace);
			mapmemorypageTablepage.remove(pagereplace);
			listmemorypages.add(tablepage);
			mapmemorypageTablepage.put(tablepage, memoryid);
			if (toprint) {
				//System.out.println("MISS " + memoryid);
			}
			DBInit.tablemapping.get(tablename).get(pi.getPageno())
					.setInmemory(true);
			DBInit.tablemapping.get(oldtable).get(oldpageid).setInmemory(false);
			//System.out.println("New LRU "+listmemorypages);
		} else {
			//System.out.println("Free main meory available with page in use " + pagesinuse);
			//System.out.println("Current LRU "+listmemorypages);
			listmemorypages.add(tablepage);
			mapmemorypageTablepage.put(tablepage, pagesinuse);
			if (toprint) {
			//	System.out.println("MISS " + pagesinuse);
			}
			pagesinuse++;
			DBInit.tablemapping.get(tablename).get(pi.getPageno())
					.setInmemory(true);
			//System.out.println("New LRU "+listmemorypages);
		}
		if (!newpage) {
			loadPageintoMemory(tablename, pi);
		}
	}

	void updatePageintoMemory(String tablename, int pageid, boolean toprint) {
		if (toprint) {
			//System.out.println("HIT");
			}
	
		String tablepage = tablename + "_" + pageid;
		//System.out.println("Page is available in the memory "+tablepage);
		//System.out.println("Updating LRU-->");
		//System.out.println("Before-->"+listmemorypages);
		listmemorypages.remove(tablepage);
		listmemorypages.add(tablepage);
		//System.out.println("After-->"+listmemorypages);

	}

	String getRecordFromPageInMemory(String tablepage, int recordid) {
		//System.out.println("Retrieving record for "+tablepage);
		return maptablerecords.get(tablepage).get(recordid);
	}

	/**
	 * To load a Page into memory
	 * 
	 * @param pageinfo
	 */
	public void loadPageintoMemory(String tablename,
			PageInfo pageinfo) {
		TreeMap<Integer, String> mapidrecord = new TreeMap<Integer, String>();
		//System.out.println("Loading Page into memory.... "+tablename+"_"+pageinfo.getPageno());
		int startrecord, endrecord;
		startrecord = pageinfo.getStartrecordid();
		endrecord = pageinfo.getEndrecordid();
		int pageno = pageinfo.getPageno();
		int seekpoint=0;
		try {
			
			 RandomAccessFile raf = new RandomAccessFile(DBTools.getTableFile(tablename, DBOutput.CSV), "r");
			seekpoint=pageinfo.getBeiginpoint();
			raf.seek(seekpoint);
		//	i=endrecord-startrecord;
		//	recordid=startrecord;
			for (int i=startrecord; i<=endrecord;i++) {
				//System.out.println("recordid="+recordid);
				String line = raf.readLine();
				mapidrecord.put(i, line.toString());
				//recordid++;
			}
			//System.out.println(mapidrecord);
			/*
			FileInputStream fis = new FileInputStream(DBTools.getTableFile(tablename));
			int r;
			char c;
			StringBuffer line = new StringBuffer();
			int recordid = -1;
			while ((r = fis.read()) != -1) {
				c = (char) r;
				while (c != '\n') {
					line.append(c);
					r = fis.read();
					c = (char) r;

				}
				recordid++;
				////System.out.println("Record id"+recordid);
				if (recordid >= startrecord && recordid <= endrecord) {
					////System.out.println("Adding record");
					mapidrecord.put(recordid, line.toString());
					line.setLength(0);
				} else if (recordid > endrecord) {
					break;
				}
				else
					line.setLength(0);
					*/
			
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println(""+mapidrecord);
		maptablerecords.put(tablename+"_"+pageno, mapidrecord);
	}
	
	
	
	public boolean addRecordtoMemPage(String tablename, PageInfo pi, String record, boolean newPage) {
	     TreeMap<Integer, PageInfo> map = DBInit.tablemapping.get(tablename);
	     TreeMap<Integer, String> mapidrecord = null;
	     int newrecordid  = 0;
	     if(newPage) {
	    	 newrecordid = pi.getEndrecordid();
	    	 mapidrecord = new  TreeMap<Integer, String>();
	    	 //System.out.println("Inserting record into new Page");
	    	  //System.out.println("New Page update "+pi);
	     } else {
	    	 mapidrecord = maptablerecords.get(tablename+"_"+pi.getPageno());
	     newrecordid = pi.getEndrecordid()+1;
	     //System.out.println("Inserting New Record into Last page "+newrecordid);
         pi.setEndrecordid(newrecordid);
	     }
	     if(pi.getPagesize()+record.length() < DBConfig.pagesize) {
	    	    pi.setPagesize(pi.getPagesize()+record.length()+1);
	     } else {
	    	   pi.setPagesize(pi.getPagesize()+record.length());
	     }
	     pi.setEndpoint(pi.getBeiginpoint()+record.length()+1);
         map.put(pi.getPageno(),pi);
   	 // System.out.println("Last Page update "+pi);
      mapidrecord.put(newrecordid, record);
   	  //System.out.println("Map id record"+mapidrecord);
         maptablerecords.put(tablename+"_"+pi.getPageno(), mapidrecord);
        return false;
    }
   
public PageInfo addNewPage(String tablename) {
    TreeMap<Integer, PageInfo> map = DBInit.tablemapping.get(tablename);
    PageInfo newpi = new PageInfo();
    PageInfo lastpage = DBTools.getLastPage(tablename);
    int newpageno = lastpage.getPageno() + 1;
    newpi.setPageno(newpageno);
    int endrecordid = lastpage.getEndrecordid();
    int beginpoint = lastpage.getEndpoint();
	  int newrecordid = endrecordid+1;
    newpi.setEndrecordid(newrecordid);
    newpi.setStartrecordid(newrecordid);	
    newpi.setPagesize(0);
    newpi.setBeiginpoint(beginpoint);
    map.put(newpageno, newpi);
    
   //System.out.println("Adding new Page "+newpi+" in "+tablename);
        return newpi;
    }

public boolean isSpaceinPage(PageInfo pi, String record) {
    int currentpageSize = pi.getPagesize();
    //System.out.println("Checking for space in "+pi);
    //System.out.println("Record Size "+record.length());
    if(currentpageSize + record.length() <= DBConfig.pagesize) {
    	//System.out.println("Space available");
    	return true;
    }
    return false;
}


}
