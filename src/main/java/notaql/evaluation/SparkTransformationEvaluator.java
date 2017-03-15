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

package notaql.evaluation;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import notaql.datamodel.ObjectValue;
import notaql.datamodel.Value;
import notaql.datamodel.fixation.Fixation;
import notaql.model.Transformation;
import scala.Tuple2;

/**
 * This is the generic transformation framework for NotaQL.
 *
 * It builds on Spark.
 */
public class SparkTransformationEvaluator implements Serializable {
	// Class variables
    private static final long serialVersionUID = 1118170317689014276L;
    
    
    // Object variables
    private Transformation transformation;

    
    public SparkTransformationEvaluator(Transformation transformation) {
        this.transformation = transformation;
    }

    
    /**
     * The starting point of the evaluation of a NotaQL transformation.
     *
     * @param values
     * @return
     */
    public JavaRDD<ObjectValue> process(JavaRDD<Value> values) {
        /* 
         * Map Phase
         * 
         * This step evaluates the transformation separately for each value without grouping and aggregation.
         * The evalutated result contains the partial results which then have to be aggregated (= reduced) afterwards. 
         */
        final JavaRDD<ValueEvaluationResult> evaluated = values.flatMap(value -> EvaluatorService.getInstance().evaluate(this.transformation.getExpression(), new Fixation((ObjectValue) value)));

    	
        /*
         * Aggregate (Reduce) Phase
         * 
         * Recursively go down the tree described by the transformation:
         * 1. First a JavaPairRDD is created (mapToPair()). Each Pair contains the group-key and one value of this group.
         * 	  The group key is: MD5(ClassName + All resolved values). If for example the row-id is fixed it will be part of the
         *    group-key. The most unspecific case would be that all ObjectValues are grouped together because nothing was fixed.
         * 2. After that these Pairs are aggregated by the groupkey. 
         */
        JavaPairRDD<String, Value> reduced = evaluated
        		.mapToPair(valueEvaluationResult -> new Tuple2<>(valueEvaluationResult.getValue().groupKey(), valueEvaluationResult.getValue()))
                .aggregateByKey(
                        EvaluatorService.getInstance().createIdentity(this.transformation.getExpression()), // Creates the neutral element to start with (e.g. 0 for summing up values)
                        (a, b) -> EvaluatorService.getInstance().reduce(this.transformation.getExpression(), a, b), // The "combining function" which merges the second parameter into the first parameter
                        (a, b) -> EvaluatorService.getInstance().reduce(this.transformation.getExpression(), a, b) // The "merging function" used for merging the output of partitions (Reducers)
                );

        
        /*
         * Finalize Phase
         * 
         * Transform the partial values into storable values (e.g. Lists into Strings)
         */
        return reduced.map(tuple -> (ObjectValue) EvaluatorService.getInstance().finalize(transformation.getExpression(), tuple._2));
    }


// FIXME Dead Code
//    /**
//     * Drops ignored values (i.e. OUT._)
//     * @param c
//     * @param <T>
//     * @return
//     */
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    private <T extends ComplexValue> T clearIgnored(T c) {
//        for (Iterator<Map.Entry<Step, Value>> iterator = c.toMap().entrySet().iterator(); iterator.hasNext(); ) {
//            final Map.Entry<Step, Value> next = iterator.next();
//            if (next.getKey() instanceof IgnoredIdStep.IgnoredStep)
//                iterator.remove();
//
//            if (next.getValue() instanceof ComplexValue<?>)
//                clearIgnored((ComplexValue<?>) next.getValue());
//        }
//        return c;
//    }
}
