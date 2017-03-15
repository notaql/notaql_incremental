package notaql.engines.incremental;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;

import notaql.datamodel.ComplexValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.Step;
import notaql.datamodel.Value;
import notaql.datamodel.delta.DeltaMode;
import notaql.datamodel.delta.DeltaValue;
import notaql.engines.EngineEvaluator;
import notaql.model.Transformation;

/**
 * Handles the IN-Filter for Values with delta-flags (preserved, inserted, deleted)
 */
public class DeltaFilter implements Serializable {
	// Class variables
	private static final long serialVersionUID = 4201428272046865124L;
	
	
	/**
	 * Filters the data for engines which support only inserts 
	 * 
	 * @param transformation contains the in-filter
	 * @param dataConverted
	 * @return the filtered data
	 */
	public static JavaRDD<Value> filterDataOnlyInserts(Transformation transformation, JavaRDD<Value> dataConverted) {
		return dataConverted
			.filter(value -> containsChangedValues((ObjectValue) value) && transformation.satisfiesInPredicate((ObjectValue) value)); // This will match all elements with changes (deleted / inserted columns) which also match the IN-FILTER
	}
	
	
	/**
	 * Filters the data. 
	 * 
	 * @param transformation contains the in-filter
	 * @param dataConverted
	 * @return the filtered data
	 */
	public static JavaRDD<Value> filterData(Transformation transformation, JavaRDD<Value> dataConverted) {
		return dataConverted
			.groupBy(value -> ((ObjectValue) value).get(EngineEvaluator.getRowIdentifierStep())) // => Tuple2<RowID, List<Values>> (all elements with the same _id)
			.filter(tuple -> containsChangedValues(tuple._2)) // This will match all groups with changes (deleted / inserted columns in any element)
			.flatMap(tuple -> getCorrectElements(transformation, tuple._2));
	}
	
	
	/**
	 * Checks for the deleted elements (if any) and the inserted row the IN-FILTER and determines based on
	 * the result which elements to pass on.
	 * 
	 * deleted was ok, inserted is ok	 								=> return both elements
	 * deleted was filtered (or non-existent), inserted is ok			=> return inserted
	 * deleted was ok, inserted is filtered								=> return deleted and flag preserved values as deleted
	 * deleted was filtered (or non-existent), inserted is filtered		=> return none
	 * 
	 * @param elements
	 * @return
	 */
	private static Iterable<Value> getCorrectElements(Transformation transformation, Iterable<Value> elements) {
		// Get the correct elements
		ObjectValue elementInserted = null;
		ObjectValue elementDeleted = null;
		ObjectValue elementPreserved = null;

		Iterator<Value> iterator = elements.iterator();
		
		while (iterator.hasNext()) {
			ObjectValue element = (ObjectValue) iterator.next();
			
			if (elementInserted == null && containsValuesWithMode(element, DeltaMode.INSERTED))
				elementInserted = element;
			
			else if (elementDeleted == null && containsValuesWithMode(element, DeltaMode.DELETED))
				elementDeleted = element;
			
			else if (elementPreserved == null && containsValuesWithMode(element, DeltaMode.PRESERVED))
				elementPreserved = element;
			
			if (elementInserted != null && elementDeleted != null && elementPreserved != null)
				break;
		}
		
		
		// In case of inserts the deleted element has no values flagged as deleted
		if (elementDeleted == null && elementPreserved != null)
			elementDeleted = elementPreserved;

		
		// Check their filter-status
		boolean isElementDeletedOk = elementDeleted != null && transformation.satisfiesInPredicate(elementDeleted);
		boolean isElementInsertedOk = elementInserted != null && transformation.satisfiesInPredicate(elementInserted);
		
		
		// Return the correct rows
		if (isElementDeletedOk && isElementInsertedOk)
			return Arrays.asList(generateElementInserted(elementInserted), generateElementDeleted(elementDeleted, false));
		
		else if (!isElementDeletedOk && isElementInsertedOk)
			return Arrays.asList(generateElementInserted(elementInserted));
		
		else if (isElementDeletedOk && !isElementInsertedOk)
			return Arrays.asList(generateElementDeleted(elementDeleted, true));
		
		else
			return Collections.emptyList();
	}
	
	
	/**
	 * If only a deleted element has to be returned some flags have to be changed: the preserved values are flagged
	 * as deleted (they shall be removed from the result) and the inserted values are flagged as preserved (they shall be ignored).
	 * 
	 * Changing inserts to preserved can be turned off, because if both rows are returned only the first change is
	 * needed. (In order to handle changes of the row-id correctly).
	 * 
	 * 
	 * @param element
	 * @param changeInsertedToPreserved set to true if this is the only row to return from the filter, false otherweis
	 * @return changed element
	 */
	private static <T> Value generateElementDeleted(ComplexValue<T> element, boolean changeInsertedToPreserved) {
		for (Entry<Step<T>, Value> entry : element.toMap().entrySet()) {
			Step<T> key = entry.getKey();
			Value value = entry.getValue();
			
			if (value instanceof ComplexValue)
				element.set(key, generateElementDeleted((ComplexValue<?>) value, changeInsertedToPreserved));

			else if (value instanceof DeltaValue) {
				// Change the flags
				DeltaValue deltaValue = (DeltaValue) value;
				
				if (deltaValue.getMode() == DeltaMode.PRESERVED)
					deltaValue.setMode(DeltaMode.DELETED);
				
				else if (deltaValue.getMode() == DeltaMode.INSERTED && changeInsertedToPreserved)
					deltaValue.setMode(DeltaMode.PRESERVED);
			}
		}
		
		
		return element;
	}
	
	
	/**
	 * If only an inserted row is returned some flags have to be changed: The preserved values are flagged as inserted
	 * (they shall be added to result).
	 * 
	 * @param element
	 * @return changed element
	 */
	private static <T> Value generateElementInserted(ComplexValue<T> element) {
		for (Entry<Step<T>, Value> entry : element.toMap().entrySet()) {
			Step<T> key = entry.getKey();
			Value value = entry.getValue();
			
			if (value instanceof ComplexValue)
				element.set(key, generateElementInserted((ComplexValue<?>) value));

			else if (value instanceof DeltaValue) {
				// Change the flags
				DeltaValue deltaValue = (DeltaValue) value;
				
				if (deltaValue.getMode() == DeltaMode.PRESERVED)
					deltaValue.setMode(DeltaMode.INSERTED);
			}
		}
		
		
		return element;
	}
	
	
	/**
	 * Recursivly checks if the Value contains any element with the given mode.
	 * 
	 * @param complexValue
	 * @return
	 */
	private static boolean containsValuesWithMode(ComplexValue<?> complexValue, DeltaMode mode) {
		for (Value value : complexValue.toMap().values()) {
			if (value instanceof ComplexValue) {
				if (containsValuesWithMode((ComplexValue<?>) value, mode))
					return true;
			}
			else if (value instanceof DeltaValue && (((DeltaValue) value)).getMode() == mode)
				return true;		
		}
		
		return false;
	}
	
		
	/**
	 * Recursivly checks if the Iterable contains any Values with relevant data (deleted or insert).
	 * 
	 * @param complexValue
	 * @return
	 */
	private static boolean containsChangedValues(Iterable<Value> complexValues) {
		for (Value value : complexValues) {
			if (value instanceof ComplexValue && containsChangedValues((ComplexValue<?> ) value))
				return true;
		}
		
		return false;
	}
	
		
	/**
	 * Recursivly checks if the Value contains any relevant data (delete or insert).
	 * 
	 * @param complexValue
	 * @return
	 */
	private static boolean containsChangedValues(ComplexValue<?> complexValue) {
		for (Value value : complexValue.toMap().values()) {
			if (value instanceof ComplexValue) {
				if (containsChangedValues((ComplexValue<?>) value))
					return true;
			}
			else if (value instanceof DeltaValue && (((DeltaValue) value)).isChange())
				return true;		
		}
		
		return false;
	}
}
