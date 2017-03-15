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

package notaql.model.vdata.aggregation;

import java.util.List;
import java.util.Optional;

import notaql.datamodel.NumberValue;
import notaql.datamodel.Value;
import notaql.datamodel.delta.DeltaValue;
import notaql.datamodel.delta.aggregations.NumberValueSum;
import notaql.model.vdata.VData;
import notaql.model.vdata.aggregation.delta.DeltaCountVData;

public class CountVData extends SimpleAggregatingVData<NumberValue> {
	// Class variables
    private static final long serialVersionUID = 2136275631345543123L;
    protected static final NumberValue NUMBER_VALUE_ONE = new NumberValue(1); 
    protected static final NumberValue NUMBER_VALUE_SUM_ZERO = new NumberValueSum(0); 

    
    public CountVData(VData expression) {
        super(expression);
    }

    
    /* (non-Javadoc)
     * @see notaql.model.vdata.aggregation.SimpleAggregatingVData#aggregate(java.util.List)
     */
    @Override
    public NumberValue aggregate(List<Value> values) {
    	// Check if there is a more specific function vor the given values
    	if (!(this instanceof DeltaCountVData) && values.stream().filter(v -> (v instanceof DeltaValue)).findAny().isPresent())
    		return new DeltaCountVData(this).aggregate(values);
        
        
    	// Aggregation
        final Optional<NumberValue> sum = values
                .stream()
                .map(value -> valuePipe(value)) // Each value is mapped to 1 (or something different if valuePipe is overwritten by a subclass)
                .reduce((a, b) -> new NumberValueSum(a.getValue().doubleValue() + b.getValue().doubleValue()));

        
        // If it worked we return the sum -- 0 otherwise
        if (sum.isPresent())
            return sum.get();
        else
        	return NUMBER_VALUE_SUM_ZERO;
    }
    
    
    /**
     * Will be called durring the aggregation. May be overwritten in subclasses in order to handle special cases.
     * 
     * @param value
     * @return
     */
    protected NumberValue valuePipe(Value value) {
    	return NUMBER_VALUE_ONE;
    }

    @Override
    public String toString() {
        return "COUNT()";
    }
}
