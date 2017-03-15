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

package notaql.engines.csv;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import notaql.SparkFactory;
import notaql.datamodel.AtomValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.Step;
import notaql.datamodel.StringValue;
import notaql.datamodel.Value;
import notaql.engines.Engine;
import notaql.engines.EngineEvaluator;
import notaql.engines.csv.datamodel.ValueConverter;
import notaql.engines.csv.model.vdata.ColCountFunctionVData;
import notaql.engines.csv.path.CSVInputPathParser;
import notaql.engines.csv.path.CSVOutputPathParser;
import notaql.model.Transformation;
import notaql.model.vdata.FunctionVData;
import notaql.parser.TransformationParser;
import notaql.parser.path.InputPathParser;
import notaql.parser.path.OutputPathParser;

/**
 * This is the simple spark implementation of CSV evaluation.
 * It reads the text file and uses the Apache CSV lib to do so.
 *
 * The expected format is "DEFAULT" with headers.
 *
 * TODO: more options could be provided to further specify the format.
 */
public class CSVEngineEvaluator extends EngineEvaluator {
	// Configuration
    private static final CSVFormat defaultCsvFormat = CSVFormat.DEFAULT;
	
	
    // Object variables
    private final String path;
    private final char csvDelimiter;
    

    public CSVEngineEvaluator(Engine engine, TransformationParser parser, Map<String, AtomValue<?>> params) {
        super(engine, parser, params);

        this.path = params.get(CSVEngine.PARAMETER_NAME_PATH).getValue().toString();
        this.csvDelimiter = params.getOrDefault("delimiter", new StringValue(Character.toString(defaultCsvFormat.getDelimiter()))).getValue().toString().charAt(0);
    }
    
    
    /**
     * @return the path
     */
    public String getPath() {
    	return this.path;
    }
    
    
    /**
     * Creates the CSVFormat which shall be used by the methods of this instance
     * 
     * @return the default CSVFormat for this instance
     */
    private CSVFormat getCSVFormat() {
    	return defaultCsvFormat.withDelimiter(this.csvDelimiter);
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getInputPathParser()
     */
    @Override
    public InputPathParser getInputPathParser() {
        return new CSVInputPathParser(this.getParser());
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getOutputPathParser()
     */
    @Override
    public OutputPathParser getOutputPathParser() {
        return new CSVOutputPathParser(this.getParser());
    }
    
    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getDataFiltered(notaql.model.Transformation)
     */
    // No engine specific filtering (using the transformation) is currently applied
    public JavaRDD<Value> getDataFiltered(Transformation transformation) {
    	// Configuration for the following steps
        final CSVFormat csvFormatWithoutHeader = this.getCSVFormat();
    	
    	
    	// Use spark to retreive the raw input rdds from the csv file
        final JavaSparkContext spark = SparkFactory.getSparkContext();
        final JavaRDD<String> dataRaw = spark.textFile(path);

        
        // Get the first line and the parse from this line the header of the csv-file
        // FIXME this assumes a header line. It might happen that it is not provided.
        final String csvFirstLine = dataRaw.first();
        final String[] csvHeaderColumns = getHeader(csvFormatWithoutHeader, csvFirstLine);
        final CSVFormat csvFormatWithHeader = csvFormatWithoutHeader.withHeader(csvHeaderColumns);

        
        // The raw input rdds are then converted to the inner format used by this notaql-framework
        // After that the entries which do not fulfill the input filter will be filtered
        final JavaRDD<CSVRecord> dataRawCSVRecords = dataRaw
                .filter(line -> !line.equals(csvFirstLine))
                .map(line -> csvFormatWithHeader.parse(new StringReader(line)).iterator().next());
        return dataRawCSVRecords.map(ValueConverter::convertToNotaQL);
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#store(org.apache.spark.api.java.JavaRDD)
     */
	@Override
    public void store(JavaRDD<ObjectValue> resultRaw) {
		super.store(resultRaw);
		
		
    	// Configuration for the following steps
        final CSVFormat csvFormatWithoutHeader = this.getCSVFormat();
        final String[] csvHeaderColumns = getHeader(resultRaw);


        // Generate the resulting csv-lines using spark
        final JavaRDD<String> resultConverted = resultRaw
                .map(object -> ValueConverter.convertFromNotaQL(object, csvHeaderColumns))
                .map(token -> csvFormatWithoutHeader.format(token.toArray()));

        
        // Store the data using spark
        final JavaSparkContext spark = SparkFactory.getSparkContext();
        final JavaRDD<String> resultHeader = spark.parallelize(Arrays.asList(csvFormatWithoutHeader.format((Object[]) csvHeaderColumns)));        
        spark.union(resultHeader, resultConverted).saveAsTextFile(this.path);
    }

    
    /**
     * Generates the header of the csv-file.
     * 
     * @param resultRaw the param which was passed to store()
     * @return the header-columns
     */
    private static String[] getHeader(JavaRDD<ObjectValue> resultRaw) {
        final List<String> strings = resultRaw.flatMap(ObjectValue::keySet).distinct().map(Step::getStep).collect();

        return strings.toArray(new String[strings.size()]);
    }

    
    /**
     * Generates the header of the csv-file.
     * 
     * @param csvFormatWithoutHeader the CSVFormat to be used for parsing the csvFirstLine
     * @param csvFirstLine the first line from the csv file
     * @return the header-columns
     * @throws AssertionError if the header could not be parsed
     */
    private static String[] getHeader(CSVFormat csvFormatWithoutHeader, String csvFirstLine) throws AssertionError {
    	final CSVRecord csvHeader;
        try {
            csvHeader = csvFormatWithoutHeader.parse(new StringReader(csvFirstLine)).iterator().next();
        } catch (IOException e) {
            e.printStackTrace();
            throw new AssertionError("CSV-Header could not be parsed");
        }
        
        
        String[] csvHeaderColumns = new String[csvHeader.size()];
        for (int i = 0; i < csvHeader.size(); i++) {
            csvHeaderColumns[i] = csvHeader.get(i);
        }
        
        
        return csvHeaderColumns; 
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
		return this.getClass().equals(CSVEngineEvaluator.class);
	}
}
