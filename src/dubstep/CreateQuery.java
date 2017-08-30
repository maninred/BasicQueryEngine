package dubstep;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;


public class CreateQuery extends Main{

	CreateQuery()
	{
		CreateTable create=(CreateTable) query;
		Table table =(Table) create.getTable();
		tableName = table.getWholeTableName();
		List<ColumnDefinition> createColumnsList = new ArrayList<ColumnDefinition>();
		createColumnsList = create.getColumnDefinitions();
		Iterator<ColumnDefinition> createColumnsIterator = createColumnsList.iterator();
		String columnsIdentifier;
		String[] columnsInfo;
		tableSchema = new LinkedHashMap<String, String>();
		tableIndex = new HashMap<String, Integer>();
		int index=0;
		while(createColumnsIterator.hasNext()){
			columnsIdentifier = (String) createColumnsIterator.next().toString();
			columnsInfo = columnsIdentifier.split(" ");	
			tableIndex.put(columnsInfo[0], index++);
			tableSchema.put(columnsInfo[0], columnsInfo[1]);
		}
		tableDetails.put(tableName, tableSchema);
		tableDetails1.put(tableName, tableIndex);
	}
}
