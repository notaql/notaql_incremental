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
import notaql.datamodel.delta.DeltaNumberValue;
import notaql.datamodel.delta.aggregations.NumberValueSum;
import notaql.model.EvaluationException;
import notaql.model.vdata.VData;
import notaql.model.vdata.aggregation.delta.DeltaSumVData;

/**
 * @author Thomas Lottermann
 */
public class SumVData extends SimpleAggregatingVData<NumberValue> {
	// Class variables
    private static final long serialVersionUID = 6574585465493817532L;
    protected static final NumberValue NUMBER_VALUE_SUM_ZERO = new NumberValueSum(0);

    
    public SumVData(VData expression) {
        super(expression);
    }

    
    /* (non-Javadoc)
     * @see notaql.model.vdata.aggregation.SimpleAggregatingVData#aggregate(java.util.List)
     */
    @Override
    public NumberValue aggregate(List<Value> values) {
        // Check if the values have the correct type
    	if (values.stream().filter(v -> !(v instanceof NumberValue)).findAny().isPresent())
            throw new EvaluationException("SUM aggregation function encountered values which are not numbers.");
        
        
    	// Check if there is a more specific function vor the given values
    	if (!(this instanceof DeltaSumVData) && values.stream().filter(v -> (v instanceof DeltaNumberValue)).findAny().isPresent())
    		return new DeltaSumVData(this).aggregate(values);
        
        
    	// Aggregation
        final Optional<NumberValue> sum = values
                .stream()
                .map(value -> numberValuePipe((NumberValue) value))
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
     * @param numberValue
     * @return
     */
    protected NumberValue numberValuePipe(NumberValue numberValue) {
    	return numberValue;
    }

    
    /* (non-Javadoc)
     * @see notaql.model.vdata.aggregation.SimpleAggregatingVData#toString()
     */
    @Override
    public String toString() {
        return "SUM(" + super.toString() + ")";
    }
}
