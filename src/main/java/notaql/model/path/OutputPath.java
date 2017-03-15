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

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import notaql.datamodel.fixation.Fixation;

/**
 * This denotes where data ends up in (e.g. OUT.$(IN.a))
 *
 * TODO: what to do with nested output paths (in an object constructor)
 */
public class OutputPath implements Serializable {
    private static final long serialVersionUID = -1792131637590592751L;
    private List<OutputPathStep> pathSteps;

    public OutputPath(List<OutputPathStep> pathSteps) {
        this.pathSteps = pathSteps;
    }

    public OutputPath(OutputPathStep... pathSteps) {
        this(Arrays.asList(pathSteps));
    }

    public OutputPath() {
        pathSteps = new LinkedList<>();
    }

    public List<OutputPathStep> getPathSteps() {
        return pathSteps;
    }

    public List<StepNameEvaluationResult> evaluate(Fixation fixation) {
        List<StepNameEvaluationResult> nameSteps = Arrays.asList(new StepNameEvaluationResult(fixation));

        for (OutputPathStep pathStep : pathSteps) {
            nameSteps = nameSteps
                    .stream()
                    .map(nameStep -> pathStep.evaluateStepName(nameStep, fixation))
                    .flatMap(l -> l.stream())
                    .collect(Collectors.toList());
        }

        return nameSteps;
    }

    @Override
    public String toString() {
        return "OUT." + pathSteps.stream().map(Object::toString).collect(Collectors.joining("."));
    }
}
