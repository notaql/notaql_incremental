package notaql.engines.hbase.timestamp;

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
 * Used for generating TimestampNumberValues with the correct falgs (inserted).
 */
public class TimestampValueConverterInsertsOnly extends ValueConverter {
	// Class variables
	private static final long serialVersionUID = 1L;
	
	
	// Object variables
	private final long timestampStart;
	
	
    public TimestampValueConverterInsertsOnly(long timestampStart) {
		this.timestampStart = timestampStart;
	}
    

    /* (non-Javadoc)
     * @see notaql.engines.hbase.datamodel.ValueConverter#convertToNotaQL(org.apache.hadoop.hbase.client.Result)
     */
    @Override
    public Value convertToNotaQL(Result result) {
        final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap = result.getMap();
    	
    	
        // Instanciate and add the primary result-object
        ObjectValue rowObject1 = new ObjectValue();
        boolean rowIsInserted = true; // >= 1 preserved -> this is false

        
        // Add the column families containing the columns containing the cells to the result-object
        for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamily : resultMap.entrySet()) {
            // The column family        	
			String columnFamilyId = Bytes.toString(columnFamily.getKey()); // e.g. "colfamily1" (see jDoc)
			ObjectValue columnFamilyObject1 = new ObjectValue();
			
			
            // Collect the columns and the cell-values
            for (Entry<byte[], NavigableMap<Long, byte[]>> column : columnFamily.getValue().entrySet()) {
                String columnId = Bytes.toString(column.getKey()); // e.g. "col1"
                AtomValue<?> value = columnToCellValue(column.getValue());
                
                columnFamilyObject1.put(new Step<String>(columnId), value);
                
                if (rowIsInserted && ((DeltaValue) value).getMode() == DeltaMode.PRESERVED)
                	rowIsInserted = false;
            }
            
            rowObject1.put(new Step<String>(columnFamilyId), columnFamilyObject1);
        }
        
    
        // Set the delta-status of the rowid.
        // If there were preserved values the row is not new.
        String rowId = Bytes.toString(result.getRow());
        rowObject1.put(EngineEvaluator.getRowIdentifierStep(), new DeltaStringValue(rowId, rowIsInserted ? DeltaMode.INSERTED : DeltaMode.PRESERVED));

        
        return rowObject1;
    }

    
	/**
     * Transforms a column into values which are flagged with delta-flags.
     * 
     * @param column the column from the Result-map containing the key and all values (= all timestamps). The first value is the most recent value (= biggest timestamp). 
     * @return the value of the cell
     */
    protected AtomValue<?> columnToCellValue(NavigableMap<Long, byte[]> column) {
		final long mostRecentCellTimestamp = column.firstKey();
    	final AtomValue<?> mostRecentCellValue = ValueUtils.parse(Bytes.toString(column.get(column.firstKey())));
    	
    	
    	// Set the correct flags
    	
		// If the most recent value was created before the timestampStart we don't have to look at the other values. Nothing has changed
		// since the last execution of the transformation.
		if (mostRecentCellTimestamp <= this.timestampStart)
			return (AtomValue<?>) DeltaValueConverter.createDeltaValue(mostRecentCellValue, DeltaMode.PRESERVED);
		
		else
			// There were changes since the last execution of the transformation
			return (AtomValue<?>) DeltaValueConverter.createDeltaValue(mostRecentCellValue, DeltaMode.INSERTED);
	}
}
