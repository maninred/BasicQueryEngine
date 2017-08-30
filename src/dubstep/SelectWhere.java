package dubstep;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;

public class SelectWhere extends Main {
	public static SelectQuery selectQueryexecute;
	SelectWhere() throws SQLException, IOException{
		/*
		 * Evaluate the Where Condition of select query
		 * Determine whether it's or true or False.
		 * If the result is true then 
		 */
		whereClauseEvaluator();
	}
	public static void whereClauseEvaluator() throws SQLException, IOException{
		PrimitiveValue result;
		customEval expressionEvaluator = new customEval();
		br = new BufferedReader(new FileReader("data/"+tableName+".csv"));
		getSchemaDetails = (LinkedHashMap<String, String>) tableDetails.get(tableName);
		getSchemaIndex=tableDetails1.get(tableName);
		aggregateResultsList = new PrimitiveValue[selectedColumsArray.length];
		while((line = br.readLine()) != null){
			readBuffer = line.split("\\|");
			result = expressionEvaluator.eval(whereclauseExpression);
			if(result.toBool() == true){
				++avgCount;
				bufferFromExternalClass = true;
				selectQueryexecute = new SelectQuery();
			}
		}
		SelectQuery.aggregatePrint();
		System.out.println();
		bufferFromExternalClass = false;
	}
}
