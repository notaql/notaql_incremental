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

package notaql.engines.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import notaql.NotaQL;
import notaql.SparkFactory;
import notaql.datamodel.AtomValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.Step;
import notaql.datamodel.Value;
import notaql.engines.DatabaseReference;
import notaql.engines.DatabaseSystem;
import notaql.engines.Engine;
import notaql.engines.EngineEvaluator;
import notaql.engines.hbase.datamodel.ValueConverter;
import notaql.engines.hbase.model.vdata.ColCountFunctionVData;
import notaql.engines.hbase.parser.path.HBaseInputPathParser;
import notaql.engines.hbase.parser.path.HBaseOutputPathParser;
import notaql.engines.incremental.resultcombiner.CombiningEngineEvaluator;
import notaql.model.Transformation;
import notaql.model.vdata.FunctionVData;
import notaql.parser.TransformationParser;
import notaql.parser.path.InputPathParser;
import notaql.parser.path.OutputPathParser;
import scala.Tuple2;

/**
 * This is the spark implementation of HBase evaluation.
 * It uses the Hadoop connector of HBase in order to read and write data.
 */
@SuppressWarnings("deprecation")
public class HBaseEngineEvaluator extends EngineEvaluator implements CombiningEngineEvaluator, DatabaseSystem {	
	// Configuration
	protected static final String PROPERTY_HBASE_HOST = "hbase_host";
	
	
    // Object variables
	private DatabaseReference databaseReference;
	protected final String table_id;
	private HBaseApi hbaseApi;

    
    public HBaseEngineEvaluator(Engine engine, TransformationParser parser, Map<String, AtomValue<?>> params) {
    	super(engine, parser, params);
    	
        this.table_id = params.get(HBaseEngine.PARAMETER_NAME_TABLE_ID).getValue().toString();
    }
    
    
    /**
     * @return the table id
     */
    public String getTableId() {
    	return this.table_id;
    }
    
    
    /**
     * @return the host for this engine evaluator
     */
    public String getHost() {
		String host = NotaQL.getProperties().getProperty(PROPERTY_HBASE_HOST);
	
		if (host != null)
			return host;
		else
			return HBaseApi.DEFAULT_HOST;
    }
    
    
    /**
     * @return the port for thins engine evaluator
     */
    public int getPort() {
		return HBaseApi.DEFAULT_PORT;
    }
    
    
    /**
     * @return a hbaseapi instance
     */
    public HBaseApi getHBaseApi() {
    	if (hbaseApi == null) {
    		hbaseApi = new HBaseApi(this.getHost(), this.getPort());
    	}
    	
    	return hbaseApi;
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getInputPathParser()
     */
    @Override
    public InputPathParser getInputPathParser() {
        return new HBaseInputPathParser(this.getParser());
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getOutputPathParser()
     */
    @Override
    public OutputPathParser getOutputPathParser() {
        return new HBaseOutputPathParser(this.getParser());
    }
	
	
	/**
	 * Retrieves the data from the given table-id
	 * 
	 * @param tableId
	 * @return
	 */
	protected JavaRDD<Value> getData(String tableId) {
    	// Configuration for the following steps
        final Configuration configuration = this.getHBaseApi().getConfiguration();
        configuration.set(TableInputFormat.INPUT_TABLE, tableId);
        configuration.set(TableInputFormat.SCAN_TIMERANGE_END, String.valueOf(this.getCurrentTimestamp()));
        configuration.set(TableInputFormat.SCAN_MAXVERSIONS, "1");
        
        final ValueConverter valueConverter = new ValueConverter();
    	
    	
    	// Use spark to retrieve the raw input rdds
        // These raw input rdds are then converted to the inner format used by this notaql-framework
        // After that the entries which do not fulfill the input filter will be filtered
        JavaSparkContext spark = SparkFactory.getSparkContext();
        final JavaPairRDD<ImmutableBytesWritable, Result> dataRaw = spark.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, org.apache.hadoop.hbase.client.Result.class);
        return dataRaw.map(tuple -> valueConverter.convertToNotaQL(tuple._2));	
	}
    
    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getDataFiltered(notaql.model.Transformation)
     */
    // No engine specific filtering (using the transformation) is currently applied
    public JavaRDD<Value> getDataFiltered(Transformation transformation) {
    	return this.getData(this.table_id);
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#store(org.apache.spark.api.java.JavaRDD)
     */
	@Override
    public void store(JavaRDD<ObjectValue> resultRaw) {
		super.store(resultRaw);

        // Create the table and the column families used in resultRaw
        try {
			this.createTableAndColumnFamilies(resultRaw);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
        // Generate the resulting puts (1 put for each row) to be executed (but filter the rows without columns)
        JavaPairRDD<ImmutableBytesWritable, Put> resultPuts = resultRaw
        		.filter(HBaseEngineEvaluator::hasCols)
        		.mapToPair(rowObject -> new Tuple2<>(new ImmutableBytesWritable(), ValueConverter.convertFromNotaQL(rowObject)));

		
    	// Configuration
        final Configuration hbaseConfiguration = this.getHBaseApi().getConfiguration();
        final JobConf jobConf = new JobConf(hbaseConfiguration, this.getClass());
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, this.table_id);
        
        
        // Store the result
        resultPuts.saveAsHadoopDataset(jobConf);
    }
    
    
    /**
     * Check if the table exists. Create it if this is not the case.
     * Check if the column families in the result exist. Create them if this is not the case.
     * 
     * @param hbaseConfiguration
     * @param resultRaw
     * @throws IOException 
     */
    private void createTableAndColumnFamilies(JavaRDD<ObjectValue> resultRaw) throws IOException {
    	HTable table = this.getHBaseApi().getOrCreateTable(this.table_id);

        // Collect the families from the result
        final List<String> columnFamilies = resultRaw
                .flatMap(ObjectValue::keySet)
                .map(Step::getStep)
                .filter(step -> !step.equals(EngineEvaluator.ROW_ID_IDENTIFIER) && !step.equals(HBaseApi.DEFAULT_COLUMN_FAMILY))
                .distinct()
                .collect();

        
        // Create the collected column families
        this.getHBaseApi().createColumnFamilies(table, columnFamilies);
    }

    
    /**
     * Checks if there is data in the object.
     * How: Checks if the objectValue (= row) contains an ObjectValue (= column family) which contains any Values (= rows).
     * 
     * @param objectValue
     * @return 
     */
    protected static boolean hasCols(ObjectValue objectValue) {
        return objectValue
        		.toMap()
        		.entrySet() // Get all contained Values
        		.stream()
                .filter(entry -> entry.getValue() instanceof ObjectValue) // Filter out all Values which are no ObjectValues 
                .anyMatch(entry -> ((ObjectValue) entry.getValue()).size() > 0);
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getFunction(java.lang.String)
     */
    @Override
    public FunctionVData getFunction(String name) {
        switch (name) {
	    	case "COL_COUNT":
	            return new ColCountFunctionVData();
			
			default:
				return null;
	    }
    }


	/* (non-Javadoc)
	 * @see notaql.engines.EngineEvaluator#isBaseType()
	 */
	@Override
	public boolean isBaseType() {
		return this.getClass().equals(HBaseEngineEvaluator.class);
	}
	
	
	public DatabaseReference getDatabaseReference() {
		if (databaseReference == null)
			databaseReference = new HBaseDatabaseReference(this.getHost(), this.getPort(), this.table_id);
		return databaseReference;
	}
}
