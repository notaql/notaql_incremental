package notaql.datamodel.delta.aggregations;

import notaql.datamodel.NumberValue;
import notaql.datamodel.Value;

/**
 * Used for combining delta-values with the result from the previous computation.
 * 
 * Used by SUM() and COUNT()
 */
public class NumberValueSum extends NumberValue implements CombinableUnresolvedValue {
	// Class variables
	private static final long serialVersionUID = -8561187999459527513L;

	
	public NumberValueSum(Number value) {
		super(value);
	}


	/* (non-Javadoc)
	 * @see notaql.datamodel.timestamp.aggregations.MergeableUnresolvedValue#merge(notaql.datamodel.Value)
	 */
	@Override
	public Value combine(Value otherValue) throws IllegalArgumentException {		
		if (otherValue == null)
			return this;
		else if (!(otherValue instanceof NumberValue))
			throw new IllegalArgumentException("For merging the other Value has to be a NumberValue");
		else
			return new NumberValue(this.getValue().floatValue() + ((NumberValue) otherValue).getValue().floatValue());
	}
}
