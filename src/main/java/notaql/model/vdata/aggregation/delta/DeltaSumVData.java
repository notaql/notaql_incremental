package notaql.model.vdata.aggregation.delta;

import notaql.datamodel.NumberValue;
import notaql.datamodel.delta.DeltaMode;
import notaql.datamodel.delta.DeltaValue;
import notaql.model.vdata.aggregation.SumVData;


/**
 * Used for (sum-)aggregating DeltaNumberValues.
 * 
 * See the paper "Incremental Data Transformations on Wide-Column Stores with NotaQL" for details.
 */
public class DeltaSumVData extends SumVData {
	// Class variables
    private static final long serialVersionUID = 6574585465493817532L;
    protected static final NumberValue NUMBER_VALUE_ZERO = new NumberValue(0);

    
    public DeltaSumVData(SumVData sumVData) {
        super(sumVData.getExpression());
    }
    
    
    /* (non-Javadoc)
     * @see notaql.model.vdata.aggregation.SumVData#numberValuePipe(notaql.datamodel.NumberValue)
     */
    @Override
    protected NumberValue numberValuePipe(NumberValue numberValue) {
    	return handleFlags(numberValue);
    }
    
    
    /**
     * Returns different numbers to fit the flag of the number.
     * 
     * @param numberValue
     * @return
     */
    public static NumberValue handleFlags(NumberValue numberValue) {
    	if (!(numberValue instanceof DeltaValue))
    		return numberValue;
    	
    	else if (((DeltaValue) numberValue).getMode() == DeltaMode.DELETED)
    		return new NumberValue(numberValue.getValue().doubleValue()*-1);

    	else if (((DeltaValue) numberValue).getMode() == DeltaMode.PRESERVED)
    		return NUMBER_VALUE_ZERO;
    	
    	else
    		return numberValue;
    }
}
