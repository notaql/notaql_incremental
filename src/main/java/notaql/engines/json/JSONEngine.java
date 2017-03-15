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

package notaql.engines.json;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.auto.service.AutoService;

import notaql.datamodel.AtomValue;
import notaql.engines.Engine;
import notaql.engines.EngineEvaluator;
import notaql.engines.csv.CSVEngine;
import notaql.parser.TransformationParser;

/**
 * Created by Thomas Lottermann on 06.12.14.
 */
@AutoService(Engine.class)
public class JSONEngine extends Engine {
	// Configuration
    private static final long serialVersionUID = 8264140461162360975L;
	public static final String PARAMETER_NAME_PATH = CSVEngine.PARAMETER_NAME_PATH;

    
    /* (non-Javadoc)
     * @see notaql.engines.Engine#createEvaluator(notaql.parser.TransformationParser, java.util.Map)
     */
    @Override
    public EngineEvaluator createEvaluator(TransformationParser parser, Map<String, AtomValue<?>> params) {
        return new JSONEngineEvaluator(this, parser, params);
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.Engine#getEngineName()
     */
    @Override
    public String getEngineName() {
        return "json";
    }


    /* (non-Javadoc)
     * @see notaql.engines.Engine#getArguments()
     */
    @Override
    public List<String> getArguments() {
        return this.getRequiredArguments();
    }


    /* (non-Javadoc)
     * @see notaql.engines.Engine#getRequiredArguments()
     */
    @Override
    public List<String> getRequiredArguments() {
        return Arrays.asList(PARAMETER_NAME_PATH);
    }
}
