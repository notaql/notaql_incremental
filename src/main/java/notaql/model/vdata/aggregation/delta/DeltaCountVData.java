package notaql.model.vdata.aggregation.delta;

import notaql.datamodel.NumberValue;
import notaql.datamodel.Value;
import notaql.datamodel.delta.DeltaMode;
import notaql.datamodel.delta.DeltaValue;
import notaql.model.vdata.aggregation.CountVData;


/**
 * Used for (count-)aggregating.
 * 
 * See the paper "Incremental Data Transformations on Wide-Column Stores with NotaQL" for details.
 */
public class DeltaCountVData extends CountVData {
	// Class variables
	private static final long serialVersionUID = -1920351845838442468L;
    protected static final NumberValue NUMBER_VALUE_MINUS_ONE = new NumberValue(-1); 
    protected static final NumberValue NUMBER_VALUE_ZERO = new NumberValue(0);

    
    public DeltaCountVData(CountVData countVData) {
        super(countVData.getExpression());
    }
    
    
    /* (non-Javadoc)
     * @see notaql.model.vdata.aggregation.CountVData#valuePipe(notaql.datamodel.Value)
     */
    @Override
    protected NumberValue valuePipe(Value value) {
    	return handleFlags(value);
    }
    
    
    /**
     * Returns different numbers to fit the flag of the Value.
     * 
     * @param value
     * @return
     */
    public static NumberValue handleFlags(Value value) {
    	if (!(value instanceof DeltaValue))
    		return NUMBER_VALUE_ONE;
    	
    	else if (((DeltaValue) value).getMode() == DeltaMode.DELETED)
    		return NUMBER_VALUE_MINUS_ONE;

    	else if (((DeltaValue) value).getMode() == DeltaMode.PRESERVED)
    		return NUMBER_VALUE_ZERO;
    	
    	else
    		return NUMBER_VALUE_ONE;
    }
}
