/*
 * Copyright 2015 by Thomas Lottermann
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package notaql.engines.mongodb.datamodel;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.bson.BSONObject;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;

import notaql.datamodel.BooleanValue;
import notaql.datamodel.DateValue;
import notaql.datamodel.ListValue;
import notaql.datamodel.NullValue;
import notaql.datamodel.NumberValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.SplitAtomValue;
import notaql.datamodel.Step;
import notaql.datamodel.StringValue;
import notaql.datamodel.Value;
import notaql.model.EvaluationException;

/**
 * Converts MongoDB's format to our internal format and vice versa.
 *
 * Supported types are:
 * - Null
 * - String
 * - Date
 * - Number
 * - Boolean
 * - ObjectId
 * - List
 * - Object
 */
@SuppressWarnings({ "rawtypes", "unchecked" }) // We dont know the type stored in the database, so we can't set it
public class ValueConverter implements Serializable {
	// Class variables
	private static final long serialVersionUID = 9103110337008482989L;

	
	/**
	 * Converts an object into the internal representation.
	 * 
	 * @param object
	 * @return
	 */
	public Value convertToNotaQL(Object object) {
        // Atom values
        if(object == null)
            return new NullValue();
        if(object instanceof String)
            return new StringValue((String) object);
        if(object instanceof Date)
            return new DateValue((Date) object);
        if(object instanceof Number)
            return new NumberValue((Number) object);
        if(object instanceof Boolean)
            return new BooleanValue((Boolean) object);
        if(object instanceof ObjectId)
            return new ObjectIdValue((ObjectId) object);
        
        
        // complex values
        if(object instanceof List) {
            final List list = (List) object;
            final ListValue result = new ListValue();
            for (Object item : list) {
                result.add(convertToNotaQL(item));
            }
            return result;
        }
        

        // complex object
        if(object instanceof BSONObject) {
            final BSONObject bsonObject = (BSONObject) object;
            final ObjectValue result = new ObjectValue();

            final Map map = bsonObject.toMap();

            for (Map.Entry<?, ?> entry : (Set<Map.Entry<?, ?>>) map.entrySet()) {
                final Step<String> step = new Step<>(entry.getKey().toString());
                final Value value = convertToNotaQL(entry.getValue());

                result.put(step, value);
            }

            return result;
        }

        
        throw new EvaluationException("Unsupported type read: " + object.getClass() + ": " + object.toString());
    } 
    

    public static Object convertFromNotaQL(Value o) {
        // Atom values
        if(o instanceof NullValue)
            return null;
        if(o instanceof StringValue)
            return ((StringValue) o).getValue();
        if(o instanceof DateValue)
            return ((DateValue) o).getValue();
        if(o instanceof NumberValue)
            return ((NumberValue) o).getValue();
        if(o instanceof BooleanValue)
            return ((BooleanValue) o).getValue();
        if(o instanceof ObjectIdValue)
            return ((ObjectIdValue) o).getValue();
        if(o instanceof SplitAtomValue<?>)
            return ((SplitAtomValue<?>)o).getValue();

        // complex list
        if(o instanceof ListValue) {
            return ((ListValue)o).stream().map(ValueConverter::convertFromNotaQL).collect(Collectors.toList());
        }
        
        // complex object
        if(o instanceof ObjectValue) {
            final BasicDBObject dbObject = new BasicDBObject();
            for (Map.Entry<Step<String>, Value> entry : ((ObjectValue) o).toMap().entrySet()) {
                dbObject.put(entry.getKey().getStep(), convertFromNotaQL(entry.getValue()));
            }
            return dbObject;
        }
        

        throw new EvaluationException("Unsupported type written: " + o.getClass() + ": " + o.toString());
    }
}
