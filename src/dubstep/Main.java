package dubstep;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.Stack;
import java.util.TreeSet;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class Main {
	public static String inputQuery;
	public static String tableName;
	public static String line;
	public static BufferedReader br = null;
	public static Scanner scan;
	public static int columnIndex;
	public static String[] readBuffer;
	public static boolean bufferFromExternalClass = false;
	public static boolean selectAll = false;
	public static boolean columnValidation = false;
	public static HashSet<Integer> countPosition= new HashSet<>();
	public static String currentColumn;
	public static PlainSelect plainselect;
	public static Expression columnExpression;
	public static Expression whereclauseExpression;
	public static boolean endofAggregate = false;
	public static HashMap<String, HashMap> tableDetails = new HashMap<String, HashMap>();//Contains table schema(Table name and Column Names)
	public static HashMap<String, HashMap> tableDetails1 = new HashMap<String, HashMap>();//Contains table Index (tableIndex)
	public static LinkedHashMap<String, String> tableSchema;
	public static HashMap<String, Integer> tableIndex;
	public static LinkedHashMap<String, String> getSchemaDetails;
	public static HashMap<String,Integer> getSchemaIndex;
	public static List<SelectItem> selectedColumns;
	public static SelectItem[] selectedColumsArray;
	public static ArrayList<Integer> columnPositions;
	public static PrimitiveValue aggregateResultsList[];
	public static Statement query;
	public static PrimitiveValue count;
	public static int avgCount;
	public static boolean aggreateFunctionsOnly=false;
	public static Stack<List<SelectItem>> sel= new Stack<List<SelectItem>>();
	public static int totalSelectCount=0;
	public static int presentSelectCount=0;


	public static void main(String[] args) throws ParseException, SQLException, IOException {
		while(true){	
			System.out.print("$>");
			scan = new Scanner(System.in);
			inputQuery = scan.nextLine();
			StringReader input=new StringReader(inputQuery);
			CCJSqlParser parser= new CCJSqlParser(input);
			query = parser.Statement();

			if(query instanceof Select){
				Select select=(Select) query;
				plainselect=(PlainSelect) select.getSelectBody();
				FromItem temp= plainselect.getFromItem();
				while(!(temp instanceof Table)){
					totalSelectCount++;
					System.out.println(totalSelectCount);
					sel.push(plainselect.getSelectItems());
					SubSelect ts=(SubSelect) temp;
					plainselect=(PlainSelect) ts.getSelectBody();
					temp=plainselect.getFromItem();
				}
				presentSelectCount=totalSelectCount;
				while(presentSelectCount>=0){
					System.out.println(presentSelectCount);
					//Table table = (Table)plainselect.getFromItem();
					Table table = (Table)temp;
					tableName = table.getWholeTableName();
					if(tableDetails.containsKey(tableName)){
						if(totalSelectCount==0 || presentSelectCount==totalSelectCount) selectedColumns=plainselect.getSelectItems();
						else selectedColumns= sel.pop();
						//selectedColumns = plainselect.getSelectItems();
						selectedColumsArray=new SelectItem[selectedColumns.size()];
						int tempi=0;
						for(SelectItem tempo : selectedColumns){
							selectedColumsArray[tempi++]=tempo;
						}
						whereclauseExpression = plainselect.getWhere();
						avgCount=0;
						if(whereclauseExpression != null){
							SelectWhere selectwhereInvoked = new SelectWhere(); 
						}
						else if (whereclauseExpression == null){
							SelectQuery sq = new SelectQuery();
						}	
					}
					else{
						System.out.println("The Table does not exist!!");
					}
					presentSelectCount--;
				}
				presentSelectCount=0;
				totalSelectCount=0;
			}	
			else if(query instanceof CreateTable){
				CreateQuery cq = new CreateQuery();
			}
			else{
				throw new java.sql.SQLException("The following query doesn't start with CREATE OR SELECT "+query);
			}
		}
	}
}

