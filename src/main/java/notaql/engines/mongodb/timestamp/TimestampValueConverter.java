package notaql.engines.mongodb.timestamp;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;
import org.bson.types.ObjectId;

import notaql.datamodel.AtomValue;
import notaql.datamodel.BooleanValue;
import notaql.datamodel.DateValue;
import notaql.datamodel.ListValue;
import notaql.datamodel.NullValue;
import notaql.datamodel.NumberValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.Step;
import notaql.datamodel.StringValue;
import notaql.datamodel.Value;
import notaql.datamodel.delta.DeltaMode;
import notaql.engines.incremental.DeltaValueConverter;
import notaql.engines.mongodb.datamodel.ObjectIdValue;
import notaql.engines.mongodb.datamodel.ValueConverter;
import notaql.model.EvaluationException;

/**
 * Used for generating TimestampNumberValues with the correct falgs (deleted, inserted, preserved).
 * 
 * See the paper "Incremental Data Transformations on Wide-Column Stores with NotaQL" for details.
 */
@SuppressWarnings({ "rawtypes", "unchecked" }) // We dont know the type stored in the database, so we can't set it
public class TimestampValueConverter extends ValueConverter {
	// Class variables
	private static final long serialVersionUID = 6881522968309134381L;
	
	
	// Object variables
	private final long timestampStart;
	
	
    public TimestampValueConverter(long timestampStart) {
		this.timestampStart = timestampStart;
	}
    
    
    /* (non-Javadoc)
     * @see notaql.engines.mongodb.datamodel.ValueConverter#convertToNotaQL(java.lang.Object)
     */
    @Override
	public Value convertToNotaQL(Object object) {
		return convertToNotaQL(object, null); 
	}
	
    
	private Value convertToNotaQL(Object object, Long timestampDocument) {
        // Atom values
		Value atomValue = convertObjectToAtomValue(object, timestampDocument);
		if (atomValue != null)
			return atomValue;
    
		
        // lists
        if (object instanceof List) {
            final List list = (List) object;
            final ListValue result = new ListValue();
            for (Object item : list) {
                result.add(convertToNotaQL(item, timestampDocument));
            }
            return result;
        }

        
        // object (including the main Object of the document)
		if (object instanceof BSONObject) {
            final BSONObject bsonObject = (BSONObject) object;
            final Map map = bsonObject.toMap();
            
            
            // Find the timestamp first (nested in the objectid) if it is not set yet
            if (timestampDocument == null) {
                for (Object value : map.values()) {
                    if (value instanceof ObjectId)
                    	timestampDocument = ((ObjectId) value).getDate().getTime();
                }
            }
            
            
            // Convert the object
            final ObjectValue result = new ObjectValue();
            for (Map.Entry<?, ?> entry : (Set<Map.Entry<?, ?>>) map.entrySet()) {
                final Step<String> step = new Step<>(entry.getKey().toString());
                final Value value = convertToNotaQL(entry.getValue(), timestampDocument);

                result.put(step, value);
            }

            return result;
		}

        
		throw new EvaluationException("Unsupported type read: " + object.getClass() + ": " + object.toString());
	}
	
	
	/**
	 * Converts the given object into an atom value. 
	 * 
	 * @param object
	 * @param timestampDocument
	 * @return the atom value or null if this is no atom value
	 */
	private Value convertObjectToAtomValue(Object object, Long timestampDocument) {
		Value returnValue = null;
		
        if(object == null)
        	returnValue = new NullValue();
        if(object instanceof String)
        	returnValue = new StringValue((String) object);
        if(object instanceof Date)
        	returnValue = new DateValue((Date) object);
        if(object instanceof Number)
        	returnValue = new NumberValue((Number) object);
        if(object instanceof Boolean)
        	returnValue = new BooleanValue((Boolean) object);
        if(object instanceof ObjectId)
        	returnValue = new ObjectIdValue((ObjectId) object);
        
        
        // Check if this value can be converted into a timestamped value
        if (returnValue != null && timestampDocument != null && returnValue instanceof AtomValue) {
        	// If the most recent value was created before the timestampStart nothing has changed since the last execution of the transformation.
    		if (timestampDocument <= this.timestampStart)
        		returnValue = DeltaValueConverter.createDeltaValue((AtomValue<?>) returnValue, DeltaMode.PRESERVED);
    		
    		// Otherwise this is an insert (deletes can not be detected because mongo db does not provide a history)
    		else
    			returnValue = DeltaValueConverter.createDeltaValue((AtomValue<?>) returnValue, DeltaMode.INSERTED);
        }
        
        
        
        return returnValue;
	}
	
}
