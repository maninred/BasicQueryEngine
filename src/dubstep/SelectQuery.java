package dubstep;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.statement.select.*;

public class SelectQuery extends Main{
	public static PrimitiveValue value;
	public static LinkedHashMap<String, String> SchemaDetails;
	public static Function function;
	public static int pos = 0;
	public static boolean aggregateEvaluation = false;
	public static customEval evalobj;
	public static customEval expressionEvaluator = new customEval();
	public static customEval customeval = new customEval();
	//public static PrimitiveValue[] aggregateResultsList;
	SelectQuery() throws SQLException, IOException
	{	
		HandleExpressionandColumns(tableName, selectedColumsArray);
	}
	public static void HandleExpressionandColumns(String tableName ,SelectItem[] selectItems) throws SQLException, IOException{
		SelectItem selectedItem;
		SelectExpressionItem expressionItemformatted;
		Expression finalformattedExpression;
		getSchemaDetails = (LinkedHashMap<String, String>) tableDetails.get(tableName);
		getSchemaIndex=tableDetails1.get(tableName);
		//If No where clause is present, All the executions on line by line basis can occur here.
		if(!bufferFromExternalClass){
	    	br = new BufferedReader(new FileReader("data/"+tableName+".csv"));
	    	aggregateResultsList = new PrimitiveValue[selectedColumsArray.length];
	    	line = br.readLine();
	    }
		bufferReaderOuterLoop:
		while(line != null){
			aggreateFunctionsOnly=false;
			pos = 0;
			SelectExpressionItem expression = (SelectExpressionItem) selectItems[0];
			Expression finalexpression = (Expression) expression.getExpression();
			//Iterator<SelectItem> selectColumnsIterator = selectItems.iterator();
			//Aggregate Queries
			if(finalexpression instanceof Function){
				if (whereclauseExpression == null) ++avgCount;
				aggreateFunctionsOnly=true;
				//System.out.println(finalexpression.toString());
				while(pos<selectedColumsArray.length){
					selectedItem = selectedColumsArray[pos];
					expressionItemformatted = (SelectExpressionItem) selectedItem;
					finalformattedExpression = (Expression) expressionItemformatted.getExpression();
					aggregateEvaluation = true;
					function = (Function) finalformattedExpression;
					//System.out.println(function.getParameters().toString());
					if(function.getParameters() != null)
						ExpressionHandler((Expression)function.getParameters().getExpressions().get(0));
					evaluateAggregateFunction(function.getName().toString());
					pos++;		
				}
			}
			//Queries with Expression and Select statements
			else if(finalexpression instanceof Expression){
					for(int t=0;t<selectedColumsArray.length;t++){
						selectedItem = selectedColumsArray[t];
						expressionItemformatted = (SelectExpressionItem) selectedItem;
						finalformattedExpression = (Expression) expressionItemformatted.getExpression();
						ExpressionHandler(finalformattedExpression);
						if(t!=selectedColumsArray.length-1) System.out.print("|");
						else System.out.println();
					}
		    }
		if(!bufferFromExternalClass) {
			//System.out.println();
			line = br.readLine();
			if(line == null) {
				aggregatePrint();
				aggregateEvaluation = false;
				if(pos >= 1) System.out.println();
			}
		}
		else break bufferReaderOuterLoop;
		}
	}
	
	public static void ExpressionHandler(Expression finalformattedExpression) throws SQLException, IOException{
			readBuffer = line.split("\\|");
			value = expressionEvaluator.eval(finalformattedExpression);
			if(aggregateEvaluation == false) printPrimitiveValue();
	}
	
	public static PrimitiveValue getPrimitiveValue() throws InvalidPrimitive{
		return value;
	}
	
	public static void printPrimitiveValue() throws InvalidPrimitive{
			System.out.print(value.toString());
	}
	
	public static void ColumnsHandler(SelectItem selectItem){
		//System.out.println("Inside columns Handler");
		getSchemaDetails = (LinkedHashMap<String, String>) tableDetails.get(tableName);
		getSchemaIndex=tableDetails1.get(tableName);
		columnPositions = new ArrayList<Integer>();
		int pos = 0;
			currentColumn = selectItem.toString();
			if(getSchemaDetails.containsKey(currentColumn)){
				pos = getSchemaIndex.get(currentColumn);
				columnPositions.add(pos);
				columnValidation = true;
			}
			else{
				System.out.println("The Column " + currentColumn + " does not exist! ");
				columnValidation = false;
				}
	}
	
	
	public static void executeSelectAll(){
		/*
		 * The BufferedReader Pointer can be set at other place.
		 * Example - Consider evaluating a where condition - 
		 * The pointer of the file read would be set by the SelectWhere Class.
		 */	
			String[] readBuffer = line.split("\\|");
			for(String data: readBuffer){
				if (data != readBuffer[0]){
					System.out.print("|");
				}
				System.out.print(data);				
			}
	}
	
	public static void evaluateAggregateFunction(String functionName) throws SQLException{
		
		functionName=functionName.toUpperCase();
		
		switch (functionName) {
		
		case "COUNT":
			if(countPosition.isEmpty() || !countPosition.contains(pos))countPosition.add(pos);
			break;
		case "MIN":
			executeMin();
			break;
		case "MAX":
			executeMax();
			break;
		case "SUM":
			executeSum();
			break;
		default:
			System.out.println("Un-handled aggregate function!!");
		}
	}
	
	public static void executeCount() throws SQLException{
		if(aggregateResultsList[pos] == null)
			aggregateResultsList[pos] = customeval.eval(new LongValue(1));
		else
			aggregateResultsList[pos] = customeval.eval(new Addition(aggregateResultsList[pos], new LongValue(1)));	
	}
	
	public static void executeMin() throws SQLException{
		if(aggregateResultsList[pos] == null)
			aggregateResultsList[pos] = value;
		else if(customeval.eval(new MinorThan(value, aggregateResultsList[pos])).toBool())
			aggregateResultsList[pos] = value;
	}
	
	public static void executeMax() throws SQLException{
		if(aggregateResultsList[pos] == null)
			aggregateResultsList[pos] = value;
		else if(customeval.eval(new GreaterThan(value, aggregateResultsList[pos])).toBool())
			aggregateResultsList[pos] = value;
	}
	
	public static void executeSum() throws SQLException{
		if(aggregateResultsList[pos] == null)
			aggregateResultsList[pos] = value;
		else
			aggregateResultsList[pos] = customeval.eval(new Addition(value, aggregateResultsList[pos]));
	}
	
	public static void aggregatePrint() throws SQLException{
		if(aggreateFunctionsOnly){
		if(aggregateResultsList[0] != null || countPosition.contains(0)){
		for(int j=0;j<aggregateResultsList.length;j++){
			if(!countPosition.isEmpty() && countPosition.contains(j)) System.out.print(avgCount);
			else System.out.print(aggregateResultsList[j].toString());
			if(j!=aggregateResultsList.length-1){
				System.out.print("|");
			}
			else 
				countPosition.clear();
		}
	   }
	 }
	}
}
