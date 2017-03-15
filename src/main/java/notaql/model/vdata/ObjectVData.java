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

package notaql.model.vdata;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import notaql.model.AttributeSpecification;


public class ObjectVData implements ConstructorVData {
    private static final long serialVersionUID = 1326445906026136294L;
    private List<AttributeSpecification> specifications;

    public ObjectVData() {

    }

    public ObjectVData(List<AttributeSpecification> specifications) {
        this.specifications = specifications;
    }

    public ObjectVData(AttributeSpecification... specifications) {
        init(specifications);
    }

    @Override
    public void init(AttributeSpecification... specifications) {
        this.specifications = Arrays.asList(specifications);
    }

    @Override
    public String toString() {
        final String join = specifications
                .stream()
                .map(s -> s.getOutputPath().toString() + " <- " + s.getVData().toString())
                .collect(Collectors.joining(",\n"));
        return "OBJECT(\n" + join + "\n)";
    }

    public List<AttributeSpecification> getSpecifications() {
        return this.specifications;
    }
}
