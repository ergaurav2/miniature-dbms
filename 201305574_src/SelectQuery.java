import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

public class SelectQuery {

	public List<String> tables;

	public List<String> columns;

	public String where;

	public String distinct;

	public String having;

	public List<String> groupby;

	public List<String> orderby;

	HashMap<String, String> wherecolvalues;
	List<String> wherecols;
	List<String> wherevalues;
	List<String> whereops;
	public List<String> getWherecols() {
		return wherecols;
	}

	public void setWherecols(List<String> wherecols) {
		this.wherecols = wherecols;
	}

	public List<String> getWherevalues() {
		return wherevalues;
	}

	public void setWherevalues(List<String> wherevalues) {
		this.wherevalues = wherevalues;
	}

	public List<String> getWhereops() {
		return whereops;
	}

	public void setWhereops(List<String> whereops) {
		this.whereops = whereops;
	}

	HashMap<String, String> havingcolvalues;
	
	HashMap<String, String> wherecolops;
	
	HashMap<String, String> havingcolops;

	public Integer querytype = 0;
	
	public Integer getQuerytype() {
		return querytype;
	}

	public void setQuerytype(Integer querytype) {
		this.querytype = querytype;
	}

	public HashMap<String, String> getWherecolvalues() {
		return wherecolvalues;
	}

	public void setWherecolvalues(HashMap<String, String> wherecolvalues) {
		this.wherecolvalues = wherecolvalues;
	}

	public HashMap<String, String> getHavingcolvalues() {
		return havingcolvalues;
	}

	public void setHavingcolvalues(HashMap<String, String> havingcolvalues) {
		this.havingcolvalues = havingcolvalues;
	}

	public List<String> getTables() {
		return tables;
	}

	public void setTables(List<String> tables) {
		this.tables = tables;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public String getWhere() {
		return where;
	}

	public void setWhere(String where) {
		this.where = where;
	}

	public String getDistinct() {
		return distinct;
	}

	public void setDistinct(String distinct) {
		this.distinct = distinct;
	}

	public String getHaving() {
		return having;
	}

	public void setHaving(String having) {
		this.having = having;
	}

	public List<String> getGroupby() {
		return groupby;
	}

	public void setGroupby(List<String> groupby) {
		this.groupby = groupby;
	}

	public List<String> getOrderby() {
		return orderby;
	}

	public void setOrderby(List<String> orderby) {
		this.orderby = orderby;
	}

	@Override
	public String toString() {
		try {
		if(!isValidQuery()) {
			return "Query Invalid";
		}
		} catch(Exception e) {
			return "Query Invalid";
		}
		StringBuffer sbout = new StringBuffer();
		sbout.append(DBOutput.QUERTTYPE);
		sbout.append(DBOutput.COLON);
		sbout.append(DBOutput.SELECT);
		sbout.append(DBOutput.NEWLINE);
		sbout.append(DBOutput.TABLENAME);
		sbout.append(DBOutput.COLON);
		int nooftables = tables.size();
		for (int i = 0; i < nooftables; i++) {
			sbout.append(tables.get(i));
			if (i != nooftables - 1) {
				sbout.append(DBOutput.COMMA);
			}
		}
		sbout.append(DBOutput.NEWLINE);
		sbout.append(DBOutput.COLUMNS);
		sbout.append(DBOutput.COLON);
		if (columns != null) {
			if(columns.get(0).equals("*")) {
				columns = DBTools.getAllColumns(tables);
			}
			int noofcolumns = columns.size();
			for (int i = 0; i < noofcolumns; i++) {
				sbout.append(columns.get(i));
				if (i != noofcolumns - 1) {
					sbout.append(DBOutput.COMMA);
				}
			}
		} else {
			sbout.append(DBOutput.NA);
		}
		sbout.append(DBOutput.NEWLINE);
		sbout.append(DBOutput.DISTINCT);
		sbout.append(DBOutput.COLON);
		if (distinct != null) {
			sbout.append(distinct);
		} else {
			sbout.append(DBOutput.NA);
		}
		sbout.append(DBOutput.NEWLINE);

		sbout.append(DBOutput.CONDITION);
		sbout.append(DBOutput.COLON);
		if (where != null) {
			sbout.append(where);
		} else {
			sbout.append(DBOutput.NA);
		}
		sbout.append(DBOutput.NEWLINE);
		sbout.append(DBOutput.ORDERBY);
		sbout.append(DBOutput.COLON);
		if (orderby != null) {
			int noofcolumns = orderby.size();
			for (int i = 0; i < noofcolumns; i++) {
				sbout.append(orderby.get(i));
				if (i != noofcolumns - 1) {
					sbout.append(DBOutput.COMMA);
				}
			}
		} else {
			sbout.append(DBOutput.NA);
		}
		sbout.append(DBOutput.NEWLINE);
		sbout.append(DBOutput.GROUPBY);
		sbout.append(DBOutput.COLON);
		if (groupby != null) {
			int noofcolumns = groupby.size();
			for (int i = 0; i < noofcolumns; i++) {
				sbout.append(groupby.get(i));
				if (i != noofcolumns - 1) {
					sbout.append(DBOutput.COMMA);
				}
			}
		} else {
			sbout.append(DBOutput.NA);
		}
		sbout.append(DBOutput.NEWLINE);
		sbout.append(DBOutput.HAVING);
		sbout.append(DBOutput.COLON);
		if (having != null) {
			sbout.append(having);
		} else {
			sbout.append(DBOutput.NA);
		}
		//System.out.println("All 24"+DBTools.allentries);
		return sbout.toString();
	}

	public boolean isValidQuery() {
	//	System.out.println("All 22"+DBTools.allentries);
		if (tables != null) {
			if (!DBTools.isTablesExist(tables)) {
			//	System.out.println("Query Invalid");
				return false;
			}
		}
		if (columns != null) {
			if (!DBTools.isValidColumns(tables, columns)) {
			//	System.out.println("Query Invalid");
				return false;
			}
			if(DBTools.isAmbiguous(tables,columns)) {
				return false;
			}
		}
		if(groupby!=null) {
			if (!DBTools.isValidColumns(tables, groupby)) {
			//	System.out.println("Query Invalid");
				return false;
			}
			if(DBTools.isAmbiguous(tables,columns)) {
				return false;
			}
		}
		if(orderby!=null) {
			if (!DBTools.isValidColumns(tables, orderby)) {
			//	System.out.println("Query Invalid");
				return false;
			}
			if(DBTools.isAmbiguous(tables,columns)) {
				return false;
			}
		}
		if(where!=null) {
			Set<String> columns = wherecolvalues.keySet();
			List<String> listcolumns = new ArrayList<String>();
			listcolumns.addAll(columns);
			if(!DBTools.isValidColumns(tables, listcolumns)) {
			//	System.out.println("Query Invalid");
				return false;
			}
			if(DBTools.isAmbiguous(tables,new ArrayList<String>(columns))) {
			
				return false;
			}
			if(!DBTools.isValidColumnsType(tables, wherecolvalues, wherecolops)) {
			//	System.out.println("Query Invalid");
				return false;
			}
		}
			if(having!=null) {
				Set<String> columns = havingcolvalues.keySet();
				List<String> listcolumns = new ArrayList<String>();
				listcolumns.addAll(columns);
				if(!DBTools.isValidColumns(tables, listcolumns)) {
				//	System.out.println("Query Invalid");
					return false;
				}
				if(DBTools.isAmbiguous(tables,new ArrayList<String>(columns))) {
					return false;
				}
				if(!DBTools.isValidColumnsType(tables, havingcolvalues, havingcolops)) {
				//	System.out.println("Query Invalid");
					return false;
				}
			}
			
		//	System.out.println("Valid Query");
		return true;
	}

	public HashMap<String, String> getWherecolops() {
		return wherecolops;
	}

	public void setWherecolops(HashMap<String, String> wherecolops) {
		this.wherecolops = wherecolops;
	}

	public HashMap<String, String> getHavingcolops() {
		return havingcolops;
	}

	public void setHavingcolops(HashMap<String, String> havingcolops) {
		this.havingcolops = havingcolops;
	}
	
}