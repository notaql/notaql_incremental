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

package notaql.engines.hbase.datamodel;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import notaql.datamodel.AtomValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.Step;
import notaql.datamodel.StringValue;
import notaql.datamodel.Value;
import notaql.datamodel.ValueUtils;
import notaql.engines.EngineEvaluator;
import notaql.model.EvaluationException;

/**
 * Provides the tools to convert to and from HBase's internal format
 */
public class ValueConverter implements Serializable {
	// Class variables
	private static final long serialVersionUID = -2449531189689350724L;
	
	
    /**
     * Generates ObjectValues from a row. The rows are translated as follows:
     *
     *          colfamily1  colfamily2
     *          col1  col2  col3
     * rowid    val1  val2  val3
     *
     * to
     *
     * {
     *     "_id": "rowid",
     *     "colfamily1": {
     *         "col1": "val1",
     *         ...
     *     }
     *     ...
     * }
     *
     * @param result
     * @return  
     */
    public Value convertToNotaQL(Result result) {
        final NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultMap = result.getNoVersionMap();
    	
    	
        // Generate and add the default object
        String rowId = Bytes.toString(result.getRow());
        ObjectValue rowObject = new ObjectValue();
        rowObject.put(EngineEvaluator.getRowIdentifierStep(), new StringValue(rowId));

        
        // Add the column families containing the columns containing the cells to the result-object
        for (Entry<byte[], NavigableMap<byte[], byte[]>> columnFamily : resultMap.entrySet()) {
            // The column family        	
			String columnFamilyId = Bytes.toString(columnFamily.getKey()); // e.g. "colfamily1" (see jDoc)
			ObjectValue columnFamilyObject = new ObjectValue();
			
			
            // Collect the columns and the cell-values
            for (Entry<byte[], byte[]> column : columnFamily.getValue().entrySet()) {
                String columnId = Bytes.toString(column.getKey()); // e.g. "col1"
                AtomValue<?> columnValue = ValueUtils.parse(Bytes.toString(column.getValue()));
                
                columnFamilyObject.put(new Step<String>(columnId), columnValue);
            }
            
            rowObject.put(new Step<String>(columnFamilyId), columnFamilyObject);
        }

        
        return rowObject;
    }

    
    /**
     * Converts an ObjectValue (internal NotaQl representation) into a Put (HBase Operation for inserting a row).
     * 
     * @param rowObject @see this.convertToNotaQL()
     * @return hbasePut a Put which may be used for inserting the row
     * @throws IOException
     */
    public static Put convertFromNotaQL(ObjectValue rowObject) throws IOException {
    	// Get the row-id from the object.
    	// The row id has to be stored in a Step called '_id'.
        final Value rowIdValue = rowObject.get(new Step<>(EngineEvaluator.ROW_ID_IDENTIFIER));

        if (rowIdValue == null) {
            throw new EvaluationException("Row _id was null: " + rowObject);
        }

        if (!(rowIdValue instanceof AtomValue<?>))
            throw new EvaluationException("Row _id may only be of an atomic value");
        
        final String rowId = ((AtomValue<?>)rowIdValue).getValue().toString();

        
        // Instantiate the new Put to be returned
        final Put hbasePut = new Put(Bytes.toBytes(rowId));


        // Add the column families containing the columns containing the cells to the Put
        for (Map.Entry<Step<String>, Value> columnFamily : rowObject.toMap().entrySet()) {
            // Ignore '_id'
        	if(columnFamily.getKey().getStep().equals(EngineEvaluator.ROW_ID_IDENTIFIER))
                continue;

        	
        	// The column family
            final String columnFamilyId = columnFamily.getKey().getStep();

            if(!(columnFamily.getValue() instanceof ObjectValue))
                throw new EvaluationException("Column families must be of type ObjectValue");

            
            // Add the columns and the cell-values
            final ObjectValue columns = (ObjectValue) columnFamily.getValue();

            for (Map.Entry<Step<String>, Value> column : columns.toMap().entrySet()) {
                final String columnId = column.getKey().getStep();
                final String cellValue;
                
                if(column.getValue() instanceof AtomValue<?>) {
                    cellValue = ((AtomValue<?>)column.getValue()).getValue().toString();
                } else {
                    cellValue = column.getValue().toString();
                }

                hbasePut.add(Bytes.toBytes(columnFamilyId), Bytes.toBytes(columnId), Bytes.toBytes(cellValue));
            }
        }
        
        
        return hbasePut;
    }


	public Object convertToNotaQLList(Result _2) {
		// TODO Auto-generated method stub
		return null;
	}
}
