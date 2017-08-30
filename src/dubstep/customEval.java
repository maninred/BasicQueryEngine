package dubstep;
import java.sql.SQLException;
import java.util.ArrayList;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

public class customEval extends Eval {
	int pos;
	String columnValue;
	String dataType;
	@Override
	public PrimitiveValue eval(Column column) throws SQLException {
		PrimitiveValue currentColumnValue = null;
		if(Main.getSchemaDetails.containsKey(column.toString())){
			pos = Main.getSchemaIndex.get(column.toString());// Gets the Index of the column
			dataType = Main.getSchemaDetails.get(column.toString());// Whether it's int/decimal/date/string ie. Gives the datatype of column.
			if(dataType.equals("int")){
				Long longvalue = Long.parseLong(Main.readBuffer[pos]);
				return new LongValue(longvalue);
			}
			else if(dataType.equals("decimal")){
				Double doublevalue = Double.parseDouble(Main.readBuffer[pos]);
				return new DoubleValue(doublevalue);
			}
			else if(dataType.equals("date")){
				return new DateValue(Main.readBuffer[pos]);
			}
			else if(dataType.equals("string") || dataType.equals("varchar") || dataType.equals("char")){
				return new StringValue(Main.readBuffer[pos]);
			}
		}
		return currentColumnValue;
	}
	
}
