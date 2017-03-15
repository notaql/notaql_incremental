package notaql.engines.incremental.snapshot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.Optional;

import notaql.datamodel.ComplexValue;
import notaql.datamodel.ListValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.Step;
import notaql.datamodel.Value;
import notaql.datamodel.delta.DeltaMode;
import notaql.engines.incremental.DeltaValueConverter;
import scala.Tuple2;

/**
 * The default combiner used by the default implementation in the interface SnapshotEngineEvaluator.
 */
public class SnapshotCombinerImplementation implements Serializable, SnapshotCombiner {
	// Configuration
	private static final long serialVersionUID = 2L;
	
	
	// Class variables
	private static final ObjectValue EMPTY_OBJECT_VALUE = new ObjectValue();
	private static final ListValue EMPTY_LIST_VALUE = new ListValue();


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.snapshot.SnapshotCombiner#combine(scala.Tuple2)
	 */
	@Override
	public List<ObjectValue> combine(Tuple2<Optional<ObjectValue>, Optional<ObjectValue>> tuple) {
		// Get the snapshot- and the current value
		ObjectValue elementCurrent = tuple._1.or(EMPTY_OBJECT_VALUE);
		ObjectValue elementSnapshot = tuple._2.or(EMPTY_OBJECT_VALUE);
		
		
		// Merge the old values into the new element
		List<ObjectValue> returnList = new ArrayList<ObjectValue>(2);
		
		for (ComplexValue<?> objectValue : combineRecursive(elementCurrent, elementSnapshot)) {
			if (objectValue != null)
				returnList.add((ObjectValue) objectValue);
		}		
		
		return returnList;
	}
	
	
	/**
	 * Recursivly loops over the values of both given ComplexValue (which are BOTH either a ListValue or an ObjectValue)
	 * and generates flagged (preserve, inserted, deleted) values.
	 *  
	 * @param elementCurrent
	 * @param elementSnapshot
	 * @return Array with the flagged values (may contain nulls)
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <T> ComplexValue<T>[] combineRecursive(ComplexValue<T> elementCurrent, ComplexValue<T> elementSnapshot) {
		// Two new ComplexValues have to be generated (because there may be a combination of a delete and an inserted value (= updated value))
		ComplexValue<T> returnObjectInserts;
		ComplexValue<T> returnObjectDeletes;
		if (elementCurrent instanceof ObjectValue && elementSnapshot instanceof ObjectValue) {
			returnObjectInserts = (ComplexValue<T>) new ObjectValue();
			returnObjectDeletes = (ComplexValue<T>) new ObjectValue();
		}
		
		else if (elementCurrent instanceof ListValue && elementSnapshot instanceof ListValue) {
			returnObjectInserts = (ComplexValue<T>) new ListValue();
			returnObjectDeletes = (ComplexValue<T>) new ListValue();
		}
		
		else {
			throw new IllegalArgumentException("Incompatible types: Current = " + elementCurrent.getClass().getSimpleName() + ", Snapshot = " + elementSnapshot.getClass().getSimpleName());
		}
		
		
		ComplexValue<T>[] returnList = new ComplexValue[]{returnObjectInserts, returnObjectDeletes};
		
		
		// Combine the elements
		for (Entry<Step<T>, Value> entry : elementCurrent.toMap().entrySet()) {
			Step<T> key = entry.getKey();
			Value valueCurrent = entry.getValue();
			
			Value valueSnapshot;
			try {
				valueSnapshot = elementSnapshot.get(key);
			} catch (IndexOutOfBoundsException e) {
				valueSnapshot = null;
			}
			
			
			// Check if the snapshot contains a value with the same key
			if (valueSnapshot == null) {
				// If this Entry is an ComplexValue the recursion will be called again
				// Because there is no counterpart the recursion will be called with an empty Object as elementSnapshot
				if (valueCurrent instanceof ComplexValue) {
					ComplexValue<?> emptyValue = valueCurrent instanceof ObjectValue ? EMPTY_OBJECT_VALUE : EMPTY_LIST_VALUE;
					ComplexValue<?>[] recursiveReturn = combineRecursive((ComplexValue) valueCurrent, emptyValue);
					
					if (!recursiveReturn[0].toMap().isEmpty())
						returnObjectInserts.set(key, recursiveReturn[0]);
				}
				
				// Otherwise the Value has to be flagged as inserted (because they were not present in the snapshot-version)
				else
					returnObjectInserts.set(key, DeltaValueConverter.createDeltaValue(valueCurrent, DeltaMode.INSERTED));
			}
			
			else {
				// If this Entry is an ComplexValue and the value from the snapshot is one from the same class the recursion will be called again on both
				if (valueCurrent instanceof ComplexValue && valueCurrent.getClass() == valueSnapshot.getClass()) {
					ComplexValue<?>[] recursiveReturn = combineRecursive((ComplexValue) valueCurrent, (ComplexValue) valueSnapshot);
					
					if (!recursiveReturn[0].toMap().isEmpty())
						returnObjectInserts.set(key, recursiveReturn[0]);
					
					if (!recursiveReturn[1].toMap().isEmpty())
						returnObjectDeletes.set(key, recursiveReturn[1]);
				}
				
				// If the snapshot was a ComplexValue and the current version is not (or vice versa) or they don't belong to the same class
				// all values from the snapshot have to be deleted and all current values have to be inserted
				else if ((valueCurrent instanceof ComplexValue != valueSnapshot instanceof ComplexValue) || (valueCurrent instanceof ComplexValue && valueSnapshot instanceof ComplexValue && valueCurrent.getClass() != valueSnapshot.getClass())) {
					// Delete flags for the snapshot
					if (valueSnapshot instanceof ComplexValue) {
						ComplexValue<?> emptyValue = valueSnapshot instanceof ObjectValue ? EMPTY_OBJECT_VALUE : EMPTY_LIST_VALUE;
						ComplexValue<?>[] recursiveReturn = combineRecursive(emptyValue, (ComplexValue) valueSnapshot);
						
						if (!recursiveReturn[1].toMap().isEmpty())
							returnObjectDeletes.set(key, recursiveReturn[1]);
					}
					else 
						returnObjectDeletes.set(key, DeltaValueConverter.createDeltaValue(valueSnapshot, DeltaMode.DELETED));

					// Insert flags for the current version
					if (valueCurrent instanceof ComplexValue) {
						ComplexValue<?> emptyValue = valueCurrent instanceof ObjectValue ? EMPTY_OBJECT_VALUE : EMPTY_LIST_VALUE;
						ComplexValue<?>[] recursiveReturn = combineRecursive((ComplexValue) valueCurrent, emptyValue);
						
						if (!recursiveReturn[0].toMap().isEmpty())
							returnObjectInserts.set(key, recursiveReturn[0]);
					}
					else 
						returnObjectInserts.set(key, DeltaValueConverter.createDeltaValue(valueCurrent, DeltaMode.INSERTED));
				}
				
				// Otherwise the Value has to be flagged as preserved (no change) or inserted + deleted (change was made)
				else {
					if (valueCurrent.equals(valueSnapshot)) {
						// Nothing has changed
						returnObjectInserts.set(key, DeltaValueConverter.createDeltaValue(valueCurrent, DeltaMode.PRESERVED));
						returnObjectDeletes.set(key, DeltaValueConverter.createDeltaValue(valueCurrent, DeltaMode.PRESERVED));
					}
					
					else {
						// There were changes
						returnObjectInserts.set(key, DeltaValueConverter.createDeltaValue(valueCurrent, DeltaMode.INSERTED));
						returnObjectDeletes.set(key, DeltaValueConverter.createDeltaValue(valueSnapshot, DeltaMode.DELETED));
					}
				}
			}
		}		
		
		
		
		// Add the elements from the snapshot which have no counterpart in the "current"-element as deleted
		for (Entry<Step<T>, Value> entry : elementSnapshot.toMap().entrySet()) {
			Step<T> key = entry.getKey();
			
			if (!elementCurrent.toMap().containsKey(key)) {
				Value valueSnapshot = entry.getValue();
				
				// If this Entry is an ComplexValue the recursion will be called again
				// Because there is no counterpart the recursion will be called with an empty Object as elementCurrent
				if (valueSnapshot instanceof ComplexValue) {
					ComplexValue<?> emptyValue = valueSnapshot instanceof ObjectValue ? EMPTY_OBJECT_VALUE : EMPTY_LIST_VALUE;
					ComplexValue<?>[] recursiveReturn = combineRecursive(emptyValue, (ComplexValue) valueSnapshot);
					
					if (!recursiveReturn[1].toMap().isEmpty())
						returnObjectDeletes.set(key, recursiveReturn[1]);
				}
				
				// Otherwise the Value has to be flagged as deleted (because they were not present in the current-version)
				else 
					returnObjectDeletes.set(key, DeltaValueConverter.createDeltaValue(valueSnapshot, DeltaMode.DELETED));
			}
		}
		
		return returnList;
	}
}
