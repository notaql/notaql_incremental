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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import notaql.engines.csv.CSVEngine;
import notaql.engines.hbase.HBaseEngine;
import notaql.engines.json.JSONEngine;
import notaql.engines.mongodb.MongoDBEngine;
import notaql.engines.redis.RedisEngine;
import notaql.model.EvaluationException;

/**
 * A utility function, which provides the tools to resolve engines based on their name
 */
public class EngineService {
	// Class variables
    private static EngineService engineServiceInstance = null;
	
    
    // Object variables
    private Map<String, Engine> engines = new HashMap<>();

    
    private EngineService() {
        final ServiceLoader<Engine> engineLoader = ServiceLoader.load(Engine.class);

        for (Engine engine : engineLoader)
	    	engines.put(engine.getEngineName(), engine);
    }
    
    
    /**
     * Normally the ServiceLoader should add the engines himself. However his does
     * not work always and this is a fallback method.
     */
    private static void addAllEngines() {
		EngineService.getInstance().addService(CSVEngine.class);
		EngineService.getInstance().addService(HBaseEngine.class);
		EngineService.getInstance().addService(JSONEngine.class);
		EngineService.getInstance().addService(MongoDBEngine.class);
		EngineService.getInstance().addService(RedisEngine.class);
    }
    

    public static EngineService getInstance() {
        if(engineServiceInstance == null)
            engineServiceInstance = new EngineService();
        
        return engineServiceInstance;
    }

    
    /**
     * Provides all engines
     * @return
     */
    public List<Engine> getEngines() {
    	if (engines.isEmpty()) {
    		// The ServiceLoader didn't work...
    		addAllEngines();
    		
    		return getEngines();
    	}
    	
    	
        return new ArrayList<Engine>(engines.values());
    }

    
    public void addService(Class<? extends Engine> serviceClass) {
    	Engine instance;
		try {
			instance = (Engine) serviceClass.newInstance();
	    	engines.put(instance.getEngineName(), instance);
		} catch (InstantiationException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    

    /**
     * Provides the engine with the given name
     * @param name
     * @return
     */
    public Engine getEngine(String name) {
        final Engine engine = engines.get(name);

        if (engine == null) {
        	if (engines.isEmpty()) {
        		// The ServiceLoader didn't work...
        		addAllEngines();
        		
        		return getEngine(name);
        	}
        	else
        		throw new EvaluationException("Unsupported engine (there are " + engines.size() + " engines available): " + name);
        }        
        
        return engine;
    }
}
