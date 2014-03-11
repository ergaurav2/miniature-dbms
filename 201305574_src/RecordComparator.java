import java.util.Comparator;
import java.util.HashMap;
import java.util.List;


public class RecordComparator implements Comparator<String>{

	public List<Integer> ordercolumns;
	
	public HashMap<Integer, String> coltypes;
	
	public RecordComparator(List<Integer> ordercolumns, HashMap<Integer, String> coltypes) {
		this.ordercolumns = ordercolumns;
		this.coltypes = coltypes;
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public int compare(String arg0, String arg1) {
		int comp = compareRecord(arg0, arg1, ordercolumns, coltypes);
		return comp;
	}

	public static int compareRecord(String record1, String record2, List<Integer> ordercolumns, HashMap<Integer, String> coltypes) {
		String [] colvalues1 = record1.split(",");
		String [] colvalues2 = record2.split(",");
		int noofcolumns = ordercolumns.size();
		for(int i=0; i<noofcolumns; i++) {
			int colindex = ordercolumns.get(i);
			String val1 = colvalues1[colindex].substring(1,colvalues1[colindex].length()-1);
			String val2 = colvalues2[colindex].substring(1,colvalues2[colindex].length()-1);
			if(coltypes.get(colindex).equalsIgnoreCase("integer")) {
				int comp = (Integer.valueOf(val1)).compareTo(Integer.valueOf(val2));
				if(comp!=0 || i==noofcolumns-1) {
					return comp;
				}
			} else if(coltypes.get(colindex).equalsIgnoreCase("float")) {
				int comp = (Float.valueOf(val1)).compareTo(Float.valueOf(val2));
				if(comp!=0 || i==noofcolumns-1) {
					return comp;
				}
			} else {
				int comp = (colvalues1[colindex]).compareTo(colvalues2[colindex]);
				if(comp!=0 || i==noofcolumns-1) {
					return comp;
				}
			}
		}
		return 0;
	}

	
}
