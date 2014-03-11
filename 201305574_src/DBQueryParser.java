import gudusoft.gsqlparser.*;
import gudusoft.gsqlparser.nodes.*;
import gudusoft.gsqlparser.stmt.TAlterTableStatement;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TCreateViewSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import gudusoft.gsqlparser.stmt.TUpdateSqlStatement;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import javax.print.attribute.HashAttributeSet;

public class DBQueryParser {
	
	public static SelectQuery selectCommand(TCustomSqlStatement sqlstmt) {
		//TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
		//sqlparser.setSqltext(query);
		//int ret = sqlparser.parse();
	//	System.out.println("All 2"+DBTools.allentries);
		SelectQuery sq = new SelectQuery();
		try {
		//TCustomSqlStatement sqlstmt = sqlparser.getSqlstatements().get(0);
		if (sqlstmt instanceof TSelectSqlStatement) {
			TSelectSqlStatement sqlselectstmt = (TSelectSqlStatement) sqlstmt;
			int joinsize = sqlselectstmt.joins.size();
			StringBuffer sbout = new StringBuffer();
			// Table Name
			List<String> tables = new ArrayList<String>();
			for (int i = 0; i < joinsize; i++) {
				TJoin join = sqlselectstmt.joins.getJoin(i);
				TTable table = join.getTable();
				tables.add(table.toString());
				sq.setTables(tables);
			}
			
			//HashMap<String, HashMap<String, String>> tablecolumns = DBTools.getTablesColumn(tables);
			TResultColumnList columns = sqlselectstmt.getResultColumnList();
			String distinctcol = null;
			if (columns != null) {
				List<String> listcolumns = new ArrayList<String>();
				int noofcolumns = columns.size();
				for (int i = 0; i < noofcolumns; i++) {
					String column = columns.getResultColumn(i).toString();
					if(column.startsWith("(")) {
						column = column.substring(1, column.length()-1);
					}
					listcolumns.add(column);
				}
				sq.setColumns(listcolumns);
				if(listcolumns.size()>0 && !listcolumns.get(0).equals("*")) {
				distinctcol = listcolumns.get(0);
				}
			} 
			// Distinct
			TSelectDistinct distinct = sqlselectstmt.getSelectDistinct();
			if (distinct != null) {
				sq.setDistinct(distinctcol);
			}			// Where Condition
			TWhereClause where = sqlselectstmt.getWhereClause();
			TExpression condition = null;
			Integer querytype = null;
			if (where != null) {
				condition = where.getCondition();
				sq.setWhere(condition.toString());
				
		//	for(int i=0;i<exp.size();i++) {
		//		System.out.println("exp "+i+" "+exp.getElement(i));
		//	}
				condition = where.getCondition();
				sq.setWhere(condition.toString());
				String query = sqlstmt.toString();
				query = query.replace('(', ' ');
				query=query.replace(')', ' ');
				TGSqlParser sqlparser1 = new TGSqlParser(EDbVendor.dbvoracle);
				sqlparser1.sqltext = query;
				sqlparser1.parse();
				TCustomSqlStatement sql = sqlparser1.sqlstatements.get(0);
				condition = sql.getWhereClause().getCondition();
			//	System.out.println("condtion "+condition);
			//	System.out.println(condition.toString());
				if (condition != null) {
					HashMap<String, String> colvalues = new HashMap<String, String>();
					HashMap<String, String> coloperator = new HashMap<String, String>();
					List<String> wherecols = new ArrayList<String>();
					List<String> wherevalues = new ArrayList<String>();
					List<String> whereops = new ArrayList<String>();
					
					while (condition != null) {
						TExpression left = condition.getLeftOperand();
						TExpression right = condition.getRightOperand();
					//	System.out.println("Left "+left);
					// System.out.println("Right "+right);
					//	 System.out.println("##"+left.getLeftOperand().toString());
						String op = condition.getOperatorToken().toString();
				//	System.out.println("Operator "+op);
						TExpression leftcol = null;
						TExpression leftvalue = null;
						String operator = null;
						if (querytype == null) {
							if(op.equalsIgnoreCase("and")) {
							querytype = 1;
						} else if(op.equalsIgnoreCase("or")) {
							querytype = 2;
						}
						}
						if (op.equalsIgnoreCase("and")
								|| op.equalsIgnoreCase("or")) {
							leftcol = right.getLeftOperand();
							leftvalue = right.getRightOperand();
							if(leftvalue == null) {
								leftcol = right;
							}
							 else if (leftvalue!=null){
								operator = right.getOperatorToken().toString();
					//			System.out.println("op "+operator);
								wherecols.add(leftcol.toString());
								whereops.add(operator);
								coloperator.put(leftcol.toString(), operator);
							}
						if(leftvalue!=null) {
							colvalues.put(leftcol.toString(),
									leftvalue.toString());
							wherevalues.add(leftvalue.toString());
						} else {
							colvalues.put(leftcol.toString(),
									null);
							wherevalues.add(null);
						}
							condition = condition.getLeftOperand();
				//			System.out.println(leftcol+" "+operator+" "+leftvalue);
						} else {
							leftcol = left;
							leftvalue = right;
							colvalues.put(leftcol.toString(),
									leftvalue.toString());
							coloperator.put(leftcol.toString(), op);
							wherevalues.add(leftvalue.toString());
							wherecols.add(leftcol.toString());
							whereops.add(op);
							condition = null;
						}
					}
					sq.setQuerytype(querytype);
					sq.setWherecolvalues(colvalues);
					sq.setWherecolops(coloperator);
					sq.setWherecols(wherecols);
					sq.setWherevalues(wherevalues);
					sq.setWhereops(whereops);
				} 
			}
			// Group By
			TGroupBy groupby = sqlselectstmt.getGroupByClause();
			// TExpression having = groupby.getHavingClause();
			StringBuffer sbhaving = new StringBuffer();
			if (groupby != null) {
				TGroupByItemList groupbylist = groupby.getItems();
				if (groupbylist != null) {
					List<String> listgroupby = new ArrayList<String>();
					int noofgroup = groupbylist.size();
					for (int i = 0; i < noofgroup; i++) {
						listgroupby.add(groupbylist.getElement(i).toString());
						}
					sq.setGroupby(listgroupby);
					

				}
			}
			if (groupby != null) {
				TExpression having = groupby.getHavingClause();
				if (having != null) {
					sq.setHaving(having.toString());
					String query = sqlstmt.toString();
					query = query.replace('(', ' ');
					query=query.replace(')', ' ');
					TGSqlParser sqlparser1 = new TGSqlParser(EDbVendor.dbvoracle);
					sqlparser1.sqltext = query;
					sqlparser1.parse();
					TCustomSqlStatement sql = sqlparser1.sqlstatements.get(0);
					TSelectSqlStatement sqlselect = (TSelectSqlStatement)sql;
					having = sqlselect.getGroupByClause().getHavingClause();
					HashMap<String, String> colvalues = new HashMap<String, String>();
					HashMap<String, String> coloperator = new HashMap<String, String>();
					while (having != null) {
						TExpression left = having.getLeftOperand();
						TExpression right = having.getRightOperand();
						// System.out.println("Left "+left);
						// System.out.println("Right "+right);
						String op = having.getOperatorToken().toString();
					//	System.out.println("Operator "+op);
						TExpression leftcol = null;
						TExpression leftvalue = null;
						String operator = null;
						if (op.equalsIgnoreCase("and")
								|| op.equalsIgnoreCase("or")) {
							leftcol = right.getLeftOperand();
							leftvalue = right.getRightOperand();
							if(leftvalue == null) {
								leftcol = right;
							}
							 else if (leftvalue!=null){
								operator = right.getOperatorToken().toString();
							//	System.out.println("op "+operator);
								coloperator.put(leftcol.toString(), operator);
							}
						if(leftvalue!=null) {
							colvalues.put(leftcol.toString(),
									leftvalue.toString());
						} else {
							colvalues.put(leftcol.toString(),
									null);
						}
							having = having.getLeftOperand();
	//						System.out.println(leftcol+" "+operator+" "+leftvalue);
						} else {
							leftcol = left;
							leftvalue = right;
							colvalues.put(leftcol.toString(),
									leftvalue.toString());
							coloperator.put(leftcol.toString(), op);
							having = null;
						}
					}
					sq.setHavingcolvalues(colvalues);
					sq.setHavingcolops(coloperator);
				} 
			}
			TOrderBy orderby = sqlselectstmt.getOrderbyClause();
			if (orderby != null) {
				List<String> listorderby = new ArrayList<String>();
				TOrderByItemList orderbylist = orderby.getItems();
				int noofgroup = orderbylist.size();
				for (int i = 0; i < noofgroup; i++) {
					listorderby.add(orderbylist.getElement(i).toString());
				}
				sq.setOrderby(listorderby);
				}
		} 
		}catch(Exception e) {
			System.out.println("Query Invalid");
			return sq;
		}
		//System.out.println("All 3"+DBTools.allentries);
		//System.out.println(sq);
		try {
			if(!sq.isValidQuery()) {
				System.out.println("Query Invalid");
				return sq;
			}
			} catch(Exception e) {
				System.out.println("Query Invalid");
				return sq;
			}
		//SelectQuery sq = DBQueryParser.selectCommand((TSelectSqlStatement)sqlparser.sqlstatements.get(i));
		DBTools.executeSelectQuery(sq);
		
		
		return sq;		
}
	
	
	
	protected static void analyzeCreateStmt(TCreateTableSqlStatement stmt){
		try{
								
				String csvfile=stmt.tables.toString();
				if(!DBTools.isTableExist(csvfile))
				{				
					System.out.println("Querytype:create");
					System.out.println("Tablename:"+stmt.tables.toString());
					
					DBTools.createFile(stmt);
				}
				else
				{
					System.out.println("Query Invalid");
				}
		}
		catch(Exception e)
		{
			System.out.println("Query Invalid");
		}
		
	}
	
		
	protected static void analyzeStmt(TCustomSqlStatement stmt){
		switch(stmt.sqlstatementtype){
		case sstselect:
		//	System.out.println("Select statement");
			selectCommand(stmt);
							break;
		case sstupdate:
							break;
		case sstcreatetable:
							//System.out.println("create statement");
							analyzeCreateStmt((TCreateTableSqlStatement)stmt);
							
							break;
		case sstaltertable:
							break;
		case sstcreateview:
							break;
		default:
							System.out.println(stmt.sqlstatementtype.toString());
		}
		}
	
	public static void main(String args[]){
		
		DBTools.getAllEntries("./config.txt");
		TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
		/*String sqlquery1 = "create table emp(empid integer, empname varchar(20), deptid integer, promotion float)";
		String sqlquery2 = "create table employee_2(empid    integer  , empname   varchar(20)  , deptid integer , promotion float)";
		String sqlquery3 = "create table employee_1(empid integer)";
		String sqlquery4 = "create table employee_4(empid integer empname varchar(20))";
		String sqlquery5 = "create table employee_5(empid integer primary key not null, empname varchar(20))";
		String sqlquery6 = "create table dept(deptid integer, deptname varchar(20), level float)";
		String sqlquery7 = "select emp from manish";
		String sqlquery8 = "select empid from manish";
		String sqlquery9 = "select empid from emp,dep";
		String sqlquery10 = "select empid from emp,dept";
		String sqlquery11 = "select * from emp,dept group by em";
		String sqlquery12 = "select emp from emp,dept";
		String sqlquery13 = "select empid from emp,dept where empid=2 group by emp";
		String sqlquery14 = "select empid from emp,dept where empid='hi' group by empid";
		String sqlquery15 = "select empid from emp,dept where empid=2 and empname='gaurav' group by empid having empid=2 order by deptid";
		String sqlquery16 = "select empid from emp,dept where empid=2 and empname='gaurav' group by empid having empid='2' order by deptid";
		String sqlquery17 = "select empid from emp,dept where emp=2 and empname='gaurav' group by empid having empid='2' order by deptid";
		String sqlquery18 = "select empid from emp,dept where empid=2 and empname='gaurav' group by empid having empid=2 order by deptid";
		String sqlquery19 = "select empid from emp,dept where empid=2 and empname='gaurav' group by empid having empid=2 order by deptid";
		String sqlquery20 = "select empid from emp,dept where   empid=2   and   empname='gaurav'  or promotion=2  group by  empid  having  empid=2  order by   deptid";
		String sqlquery21 = "select empid from emp,dept where empid=2.0 and empname='gaurav' group by empid having empid=2 order by deptid";
		String sqlquery22 = "select dept.empid from emp,dept where empid=2 and empname='gaurav' or promotion";
		String sqlquery23 = "select deptid from emp,dept";
		*/String sqlquery24 = "select * from emp,dept where (empid=1 and empname='manish') and (dept.deptid=1)  (emp.empid=emp.deptid) group by empid";
		
		//String sqlquery16 = "select empid from dept";
		List<String> myQuery = new ArrayList<String>();
		/*myQuery.add(sqlquery1);
		myQuery.add(sqlquery2);
		myQuery.add(sqlquery3);
		myQuery.add(sqlquery4);
		myQuery.add(sqlquery5);
		myQuery.add(sqlquery6);
		//myQuery.add(sqlquery5);
		myQuery.add(sqlquery7);
		myQuery.add(sqlquery8);
		myQuery.add(sqlquery9);
		myQuery.add(sqlquery10);
		myQuery.add(sqlquery11);
		myQuery.add(sqlquery12);
		myQuery.add(sqlquery13);
		myQuery.add(sqlquery14);
		myQuery.add(sqlquery15);
		myQuery.add(sqlquery16);
		myQuery.add(sqlquery17);
		myQuery.add(sqlquery18);
		myQuery.add(sqlquery19);
		myQuery.add(sqlquery20);
		myQuery.add(sqlquery21);
		myQuery.add(sqlquery22);
		myQuery.add(sqlquery23);
		*/myQuery.add(sqlquery24);
		
		for(String sqlquery: myQuery) {
			//System.out.println("Query number");
			testQuery(sqlquery);
		}
		}
	
	public static void testQuery(String sqlquery) {
		TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
		sqlparser.sqltext = sqlquery;
		System.out.println("Input: "+sqlquery);
		//sqlparser.sqltext ="select jindal from tablename where col1='2' and col2='3' and col3=4 or col4<=6.6";
		int ret = sqlparser.parse();
		if (ret == 0){
		for(int i=0;i<sqlparser.sqlstatements.size();i++){
			System.out.println("Output:");
		analyzeStmt(sqlparser.sqlstatements.get(i));
		System.out.println("");
		}
		}else{
		//	System.out.println("Testing: Exception!!!");
			System.out.println("Output:");
		System.out.println("--Query Invalid");
		}
	}
	
}