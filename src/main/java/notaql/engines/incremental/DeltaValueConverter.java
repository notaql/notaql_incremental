package notaql.engines.incremental;

import java.util.Date;

import org.bson.types.ObjectId;

import notaql.datamodel.AtomValue;
import notaql.datamodel.BooleanValue;
import notaql.datamodel.DateValue;
import notaql.datamodel.NullValue;
import notaql.datamodel.NumberValue;
import notaql.datamodel.StringValue;
import notaql.datamodel.Value;
import notaql.datamodel.delta.DeltaBooleanValue;
import notaql.datamodel.delta.DeltaDateValue;
import notaql.datamodel.delta.DeltaMode;
import notaql.datamodel.delta.DeltaNullValue;
import notaql.datamodel.delta.DeltaNumberValue;
import notaql.datamodel.delta.DeltaObjectIdValue;
import notaql.datamodel.delta.DeltaStringValue;
import notaql.engines.mongodb.datamodel.ObjectIdValue;

public class DeltaValueConverter {
    /**
     * Creates a new value with the given change-mode.
     * 
     * Note that lists are not supported at the moment.
     * 
     * @param value
     * @param timestamp
     * @param mode
     * @return
     */
    public static Value createDeltaValue(Value value, DeltaMode mode) {
    	if (value instanceof NumberValue) // MongoDB + HBase
    		return new DeltaNumberValue((Number) ((AtomValue<?>) value).getValue(), mode);
    	
    	else if (value instanceof StringValue) // MongoDB + HBase
    		return new DeltaStringValue((String) ((AtomValue<?>) value).getValue(), mode);
    	
    	else if (value instanceof BooleanValue) // MongoDB + HBase
    		return new DeltaBooleanValue((Boolean) ((AtomValue<?>) value).getValue(), mode);
    	
    	else if (value instanceof DateValue) // MongoDB
    		return new DeltaDateValue((Date) ((AtomValue<?>) value).getValue(), mode);
    	
    	else if (value instanceof ObjectIdValue) // MongoDB
    		return new DeltaObjectIdValue((ObjectId) ((AtomValue<?>) value).getValue(), mode);
    	
    	else if (value instanceof NullValue) // MongoDB
    		return new DeltaNullValue(mode);
    	
    	else
    		throw new IllegalArgumentException("value does not match any implemented type (type is '" + value.getClass().getSimpleName() + "'");
    }
}
