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

package notaql.model.path;

import java.util.Arrays;
import java.util.List;

import notaql.datamodel.ComplexValue;
import notaql.datamodel.ListValue;
import notaql.datamodel.NumberValue;
import notaql.datamodel.Step;
import notaql.datamodel.Value;
import notaql.datamodel.fixation.Fixation;
import notaql.evaluation.ValueEvaluationResult;
import notaql.model.EvaluationException;

/**
 * This .index() step is used to get the index of an element in a list.
 */
public class IndexPathMethodStep implements PathMethodStep {
    private static final long serialVersionUID = 3232404026616419104L;

    @Override
    public List<ValueEvaluationResult> evaluate(ValueEvaluationResult step, Fixation contextFixation) {
        final Value value = step.getValue();
        final ComplexValue<?> parent = value.getParent();

        if(!(parent instanceof ListValue))
            throw new EvaluationException(".index() was called on a value which is not in an object.");

        final Step<Integer> indexStep = ((ListValue) parent).getStep(value);

        return Arrays.asList(
                new ValueEvaluationResult(
                        new NumberValue(indexStep.getStep()),
                        step.getFixation()
                )
        );
    }

    @Override
    public String toString() {
        return "index()";
    }
}
