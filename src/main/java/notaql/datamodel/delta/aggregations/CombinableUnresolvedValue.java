package notaql.datamodel.delta.aggregations;

import notaql.datamodel.Value;
import notaql.evaluation.values.UnresolvedValue;

/**
 * Flags a class as unresolved but also indicates the presence of methods for combiningg (and by this resolving) the value with other values.
 * 
 * This is needed by the incremental timestamp-based transformations. 
 */
public interface CombinableUnresolvedValue extends UnresolvedValue {
	/**
	 * Merges this value with another Value and returns a resolved (= normal) Value.
	 * 
	 * @param otherValue (may be null)
	 * @return
	 * @throws IllegalArgumentException if the argument is not from the correct type (class)
	 */
	public Value combine(Value otherValue) throws IllegalArgumentException;
}
