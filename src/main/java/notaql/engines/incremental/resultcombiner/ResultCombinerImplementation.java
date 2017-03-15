package notaql.engines.incremental.resultcombiner;

import java.io.Serializable;
import java.util.Map.Entry;

import com.google.common.base.Optional;

import notaql.datamodel.ObjectValue;
import notaql.datamodel.Step;
import notaql.datamodel.Value;
import notaql.datamodel.delta.aggregations.CombinableUnresolvedValue;
import scala.Tuple2;

/**
 * The default combiner used by the default implementation in the interface CombiningEngineEvaluator.
 */
public class ResultCombinerImplementation implements Serializable, ResultCombiner {
	// Configuration
	private static final long serialVersionUID = 3L;
	
	
	// Class variables
	private static final ObjectValue EMPTY_OBJECT_VALUE = new ObjectValue();


	/**
	 * Executed for each result-value.
	 * 
	 * Checks if the result-value has to be aggregated with previous values (if not we are done very fast because we just return it)
	 * and -if this is the case- aggregates in the specific way of the aggregation function.
	 * 
	 * @param elements - the _id and 2 values with this _id (previous result and new result) 
	 * @return
	 */
	@Override
	public ObjectValue combine(Tuple2<Optional<ObjectValue>, Optional<ObjectValue>> tuple) {
		// Get the snapshot- and the current value
		ObjectValue elementNew = tuple._1.or(EMPTY_OBJECT_VALUE);
		ObjectValue elementOld = tuple._2.or(EMPTY_OBJECT_VALUE);
		
		
		// Merge the old values into the new element
		return combineRecursive(elementNew, elementOld);
	}
	
	
	/**
	 * Recursivly loops over the values of both given ObjectValues and merges the Values if possible.
	 * 
	 * @param elementNew
	 * @param elementOld
	 * @return
	 */
	public static ObjectValue combineRecursive(ObjectValue elementNew, ObjectValue elementOld) {
		for (Entry<Step<String>, Value> entry : elementNew.toMap().entrySet()) {
			// Get the matching value from the previous result
			Value valueOld = elementOld.get(entry.getKey());
			
			
			// If the previous result has no matching value this entry does not matter
			if (valueOld != null) {
				
				// If this Entry is an ObjectValue the recursion will be called again (if the previous result was also an ObjectValue)
				if (entry.getValue() instanceof ObjectValue) {
					if ((valueOld instanceof ObjectValue)) {
						elementNew.put(entry.getKey(), combineRecursive((ObjectValue) entry.getValue(), (ObjectValue) valueOld));
					}
				}
				
				// If this Entry is not an ObjectValue but a combineable Value => combine the values
				else if (entry.getValue() instanceof CombinableUnresolvedValue) {
					try {
						Value valueNew = ((CombinableUnresolvedValue) entry.getValue()).combine(valueOld);						
						elementNew.put(entry.getKey(), valueNew);
					} catch (IllegalArgumentException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		return elementNew;
	}
}
