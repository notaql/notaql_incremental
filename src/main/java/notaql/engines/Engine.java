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

package notaql.engines;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import notaql.datamodel.AtomValue;
import notaql.parser.TransformationParser;

/**
 * This class provides all interfaces necessary in order to add support for new databases.
 *
 * @author Thomas Lottermann
 */
public abstract class Engine implements Serializable {
	// Configuration
	private static final long serialVersionUID = -3227079929433972328L;
	public static final String PARAMETER_NAME_USER_EXPECTS_UPDATES = "boolean_updates_expected";
	public static final String PARAMETER_NAME_USER_EXPECTS_DELETES = "boolean_deletes_expected";


	/**
     * Provides an evaluator for this engine
     * @return
     */
    public abstract EngineEvaluator createEvaluator(TransformationParser parser, Map<String, AtomValue<?>> params);

    
    /**
     * Provides the name by which this engines is identified in the notaql expression
     *
     * @return
     */
    public abstract String getEngineName();

    
    /**
     * Provides the ordered names of the arguments
     *
     * @return
     */
    public abstract List<String> getArguments();

    
    /**
     * Provides a list of all required arguments (have to be present)
     *
     * @return
     */
    public List<String> getRequiredArguments() {
    	return this.getArguments();
    }

    
    /**
     * Provides a list of all arguments which shall stay active when the user gives the control to the advisor.
     *
     * @return
     */
    public List<String> getAdvisorArguments() {
    	return Arrays.asList(PARAMETER_NAME_USER_EXPECTS_UPDATES, PARAMETER_NAME_USER_EXPECTS_DELETES);
    }

    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return this.getEngineName();
    }
}
