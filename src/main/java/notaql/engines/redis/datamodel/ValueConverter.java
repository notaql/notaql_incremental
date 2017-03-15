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

package notaql.engines.redis.datamodel;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import notaql.datamodel.AtomValue;
import notaql.datamodel.ListValue;
import notaql.datamodel.NullValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.Step;
import notaql.datamodel.Value;
import notaql.datamodel.ValueUtils;
import notaql.model.EvaluationException;
import redis.clients.jedis.Jedis;

/**
 * Created by Thomas Lottermann on 06.12.14.
 */
public class ValueConverter implements Serializable {
	private static final long serialVersionUID = 3642797936874638440L;
	
	private final String host;
    private final int port;
    private final int databaseId;
    

    public ValueConverter(String host, int port, int databaseId) {
        this.host = host;
        this.port = port;
        this.databaseId = databaseId;
        
    }
    
    
    /**
     * @return a jedis instance with the database pre-selected.
     */
    public Jedis getJedis() {
    	Jedis jedis = new Jedis(host, port);
    	jedis.select(this.databaseId);
    	return jedis;
    }
    

    /**
     * @param jedis
     * @param key
     * @return
     */
    public static Value readFromRedis(Jedis jedis, String key) {
        final String type = jedis.type(key);

        if (type.equals("string"))
            return ValueUtils.parse(jedis.get(key));
        
        else if (type.equals("list")) {
            final List<String> list = jedis.lrange(key, 0, jedis.llen(key));
            final ListValue listValue = new ListValue();

            list.forEach(element -> listValue.add(ValueUtils.parse(element)));

            return listValue;
        }
        
        else if (type.equals("set")) {
            // TODO: we might want to support a real Set type instead of List?
            final Set<String> set = jedis.smembers(key);
            final ListValue listValue = new ListValue();

            set.forEach(element -> listValue.add(ValueUtils.parse(element)));

            return listValue;
        }
        
        else if (type.equals("hash")) {
            final Map<String, String> map = jedis.hgetAll(key);
            final ObjectValue object = new ObjectValue();

            map.forEach((k,v) -> object.put(new Step<>(k), ValueUtils.parse(v)));

            return object;
        }
        
        else
        	throw new EvaluationException("Unsupported type read: " + type);
    }

    
    /**
     * @param object
     */
    public void writeToRedis(ObjectValue object) {
        final Value id = object.get(new Step<>("_id"));
        final Value value = object.get(new Step<>("_v"));

        if(value != null) {
	        assert id != null && id instanceof AtomValue<?> && value != null;
	
	        final String key = ((AtomValue<?>)id).getValue().toString();
	
	        try (Jedis jedis = this.getJedis()) {
		        // Atom values
		        if (value instanceof NullValue) {
		        	// Do nothing
		        }
		        
		        else if (value instanceof AtomValue<?>) {
		            final String string = ((AtomValue<?>) value).getValue().toString();
		            jedis.set(key, string);
		        }
		
		        // Complex values
		        else if (value instanceof ListValue) {
		            final List<String> strings = ((ListValue) value)
		                    .stream()
		                    .filter(m -> m instanceof AtomValue<?> && !(m instanceof NullValue))
		                    .map(m -> ((AtomValue<?>) m).getValue().toString()).collect(Collectors.toList());
		            jedis.lpush(key, strings.toArray(new String[strings.size()]));
		        }
		        
		        else if (value instanceof ObjectValue) {
		            ((ObjectValue) value)
		                    .toMap()
		                    .entrySet()
		                    .stream()
		                    .filter(e -> (e.getValue() instanceof AtomValue<?> && !(e.getValue() instanceof NullValue)))
		                    .forEach(
		                            e -> jedis.hset(
		                                    key,
		                                    e.getKey().getStep(),
		                                    ((AtomValue<?>) e.getValue()).getValue().toString()
		                            )
		                    );
		        }
		        
		        else
		        	throw new EvaluationException("Unsupported type written: " + value.getClass() + ": " + value.toString());
	        }
        }
    }

    
    /**
     * Needed by Spark.
     * 
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
    }
}
