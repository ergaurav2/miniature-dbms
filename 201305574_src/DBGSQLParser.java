import java.util.ArrayList;
import java.util.List;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.TSourceToken;
import gudusoft.gsqlparser.nodes.TColumnDefinitionList;
import gudusoft.gsqlparser.nodes.TColumnReferenceList;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TExpressionList;
import gudusoft.gsqlparser.nodes.TGroupBy;
import gudusoft.gsqlparser.nodes.TGroupByItemList;
import gudusoft.gsqlparser.nodes.TJoin;
import gudusoft.gsqlparser.nodes.TOrderBy;
import gudusoft.gsqlparser.nodes.TOrderByItemList;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.nodes.TSelectDistinct;
import gudusoft.gsqlparser.nodes.TTable;
import gudusoft.gsqlparser.nodes.TWhereClause;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import gudusoft.gsqlparser.stmt.postgresql.TGrantSqlStatement;

public class DBGSQLParser {


	/**
	 * Tablename:<table1>,<table2> Columns:<col1>,<col2>,.. Distinct:NA
	 * Condition:<col1>=<value> Orderby:<col2> Groupby:<col1> Having:<col1>=50
	 * 
	 * @param query
	 * @return 
	 */

	public static SelectQuery selectCommand(TCustomSqlStatement sqlstmt) {
		//TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
		//sqlparser.setSqltext(query);
		//int ret = sqlparser.parse();
		SelectQuery sq = new SelectQuery();
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
			// Column
			TResultColumnList columns = sqlselectstmt.getResultColumnList();
			if (columns != null) {
				List<String> listcolumns = new ArrayList<String>();
				int noofcolumns = columns.size();
				for (int i = 0; i < noofcolumns; i++) {
					listcolumns.add(columns.getResultColumn(i).toString());
				}
				sq.setColumns(listcolumns);
				
			} 
			// Distinct
			TSelectDistinct distinct = sqlselectstmt.getSelectDistinct();
			//System.out.println("Distinct "+distinct.);
			if (distinct != null) {
				int distinctlist = distinct.getDistinctType();
			
			} 
			// Where Condition
			TWhereClause where = sqlselectstmt.getWhereClause();
			TExpression condition = null;
			if (where != null) {
				condition = where.getCondition();
		//	for(int i=0;i<exp.size();i++) {
		//		System.out.println("exp "+i+" "+exp.getElement(i));
		//	}
				System.out.println();
				if (condition != null) {
					while(condition!=null) {
					TExpression left = condition.getLeftOperand();
					TExpression right = condition.getRightOperand();
					TSourceToken op = condition.getOperatorToken();
					if(op.equals("and") || op.equals("or")) {
						TExpression	leftcol = left.getLeftOperand();
						TExpression leftvalue = left.getRightOperand();
					}
					System.out.println("OP "+op);
					System.out.println("Left "+left+"Right "+right);
					condition = condition.getRightOperand();
					}
					sq.setWhere(where.toString());
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
				}
		}
		System.out.println(sq);
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
					System.out.println("Invalid Query");
				}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
		
	protected static void analyzeStmt(TCustomSqlStatement stmt){
		switch(stmt.sqlstatementtype){
		case sstselect:
						//analyzeSelectStmt((TSelectSqlStatement)stmt);
							System.out.println("Select statement");
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
		TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
		sqlparser.sqltext ="select * from jindal where a=manish";
		int ret = sqlparser.parse();
		if (ret == 0){
		for(int i=0;i<sqlparser.sqlstatements.size();i++){
		analyzeStmt(sqlparser.sqlstatements.get(i));
		System.out.println("");
		}
		}else{
		System.out.println("Invalid Query");
		}
		}
	
}