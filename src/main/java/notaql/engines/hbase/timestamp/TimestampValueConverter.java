package notaql.engines.hbase.timestamp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import notaql.datamodel.AtomValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.Step;
import notaql.datamodel.Value;
import notaql.datamodel.ValueUtils;
import notaql.datamodel.delta.DeltaMode;
import notaql.datamodel.delta.DeltaStringValue;
import notaql.datamodel.delta.DeltaValue;
import notaql.engines.EngineEvaluator;
import notaql.engines.hbase.datamodel.ValueConverter;
import notaql.engines.incremental.DeltaValueConverter;

/**
 * Used for generating TimestampNumberValues with the correct falgs (deleted, inserted, preserved).
 * 
 * See the paper "Incremental Data Transformations on Wide-Column Stores with NotaQL" for details.
 */
public class TimestampValueConverter extends ValueConverter {
	// Class variables
	private static final long serialVersionUID = -4447503716834300450L;
	
	
	// Object variables
	private final long timestampStart;
	
	
    public TimestampValueConverter(long timestampStart) {
		this.timestampStart = timestampStart;
	}
    

    /* (non-Javadoc)
     * @see notaql.engines.hbase.datamodel.ValueConverter#convertToNotaQL(org.apache.hadoop.hbase.client.Result)
     */
    @Override
    @Deprecated
    public Value convertToNotaQL(Result result) {
    	throw new IllegalStateException("For timestamped values use the method which returns a list");
    }
    

    /**
     * Generates ObjectValues from a row. The rows are translated as follows:
     *
     *          colfamily1  colfamily2
     *          col1  col2  col3
     * rowid    val1  val2  val3
     *
     * to
     *
     * {
     *     "_id": "rowid",
     *     "colfamily1": {
     *         "col1": ["val1"],
     *         ...
     *     }
     *     ...
     * }
     *
     * @param result
     * @return Multiple ObjectValues containing all values from the row in an internal representation 
     */
    public List<Value> convertToNotaQLList(Result result) {
    	List<Value> resultList = new ArrayList<Value>(2);
        final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap = result.getMap();
    	
    	
        // Instanciate and add the primary result-object
        ObjectValue rowObject1 = new ObjectValue();
        resultList.add(rowObject1);
        
        
        // For one cell there may be multiple values => we need a backup-object if this is the case
        ObjectValue rowObject2 = null;

        
        // Add the column families containing the columns containing the cells to the result-object
        for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamily : resultMap.entrySet()) {
            // The column family        	
			String columnFamilyId = Bytes.toString(columnFamily.getKey()); // e.g. "colfamily1" (see jDoc)
			ObjectValue columnFamilyObject1 = new ObjectValue();	
            
			
			// An additional column family object used by rowObject2
			// If it is null no additional values had to be stored.
			ObjectValue columnFamilyObject2 = null;
			
			
            // Collect the columns and the cell-values
            for (Entry<byte[], NavigableMap<Long, byte[]>> column : columnFamily.getValue().entrySet()) {
                String columnId = Bytes.toString(column.getKey()); // e.g. "col1"
                List<AtomValue<?>> values = columnToCellValue(column.getValue());
                
                columnFamilyObject1.put(new Step<String>(columnId), values.get(0));
                
                
                // If there is more than one value in this cell we add the additional values to the additional row
                if (values.size() == 2) {
                	if (columnFamilyObject2 == null)
                		columnFamilyObject2 = new ObjectValue();
                	
                	columnFamilyObject2.put(new Step<String>(columnId), values.get(1));
                }
                
                // If this value is a preserved value it has to be present in all rows.
                // Otherwise grouping the results by a preserved value will corrupt the result.
                // Aggregating functions have to make sure that these double values are handled (e.g. SUM() ignores all preserved
                // values)
                else if (values.get(0) instanceof DeltaValue && ((DeltaValue) values.get(0)).getMode() == DeltaMode.PRESERVED) {
                	if (columnFamilyObject2 == null)
                		columnFamilyObject2 = new ObjectValue();
                	
                	columnFamilyObject2.put(new Step<String>(columnId), values.get(0).deepCopy());                	
                }
            }
            
            rowObject1.put(new Step<String>(columnFamilyId), columnFamilyObject1);
            
            
            // If there were additional cells/columns (contained by a column family) we add them to the additional row
            if (columnFamilyObject2 != null) {
            	if (rowObject2 == null) {
            		rowObject2 = new ObjectValue();
                    resultList.add(rowObject2);
            	}
            	
                rowObject2.put(new Step<String>(columnFamilyId), columnFamilyObject2);
            }
                
        }
        
    
        // Set the delta-status of the rowid.
        // If there were preserved values or deleted values the size is 2 and the row is not new.
        String rowId = Bytes.toString(result.getRow());
        
    	if (resultList.size() == 1)
    		rowObject1.put(EngineEvaluator.getRowIdentifierStep(), new DeltaStringValue(rowId, DeltaMode.INSERTED));
		else {
			for (Value value : resultList) {
				if (value instanceof ObjectValue)
					((ObjectValue) value).put(EngineEvaluator.getRowIdentifierStep(), new DeltaStringValue(rowId, DeltaMode.PRESERVED));
			}
		}

        
        return resultList;
    }

    
	/**
     * Transforms a column into values which are flagged with delta-flags.
     * 
     * @param column the column from the Result-map containing the key and all values (= all timestamps). The first value is the most recent value (= biggest timestamp). 
     * @return the values of the cell (e.g. one delete and one insert or just one preserved value)
     */
    protected List<AtomValue<?>> columnToCellValue(NavigableMap<Long, byte[]> column) {
		final long mostRecentCellTimestamp = column.firstKey();
    	final AtomValue<?> mostRecentCellValue = ValueUtils.parse(Bytes.toString(column.get(column.firstKey())));
    	
    	
    	// Set the correct flags
    	
		// If the most recent value was created before the timestampStart we don't have to look at the other values. Nothing has changed
		// since the last execution of the transformation.
		List<AtomValue<?>> returnList = new ArrayList<AtomValue<?>>(2);
		
		if (mostRecentCellTimestamp <= this.timestampStart)
			returnList.add((AtomValue<?>) DeltaValueConverter.createDeltaValue(mostRecentCellValue, DeltaMode.PRESERVED));
		
		else {
			// There were changes since the last execution of the transformation
			returnList.add((AtomValue<?>) DeltaValueConverter.createDeltaValue(mostRecentCellValue, DeltaMode.INSERTED));
			
			
			// We also have to add the most recent value right before this.timestampStart
			// mostRecentCellValue will be ignoried because its timestamp is > than this.timestampStart
        	for (Entry<Long, byte[]> cellEntry : column.entrySet()) {
        		long cellTimestamp = cellEntry.getKey();
        		
        		if (cellTimestamp <= this.timestampStart) {
        	    	AtomValue<?> cellValue = ValueUtils.parse(Bytes.toString(cellEntry.getValue()));
        			returnList.add((AtomValue<?>) DeltaValueConverter.createDeltaValue(cellValue, DeltaMode.DELETED));
        			break;
        		}
        	}
		}
		
		
    	return returnList;
	}
}
