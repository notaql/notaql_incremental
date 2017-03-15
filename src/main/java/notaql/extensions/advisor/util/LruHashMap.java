package notaql.extensions.advisor.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Removes the eldest entry if new elements are added and the Map is already full.
 *
 * @param <K>
 * @param <V>
 */
public class LruHashMap<K, V> extends LinkedHashMap<K, V> {
	private static final long serialVersionUID = 1L;
	private int maxElements;
	
	
	public LruHashMap(int maxElements) {
		super(maxElements, (float)0.75, true);
		this.maxElements = maxElements;
	}
	
	
    /**
     * Forces a removal of the oldest entry.
     */
    public void removeEldestEntry() {
    	if (!this.isEmpty()) {
    		K headKey = entrySet().iterator().next().getKey();
    		this.remove(headKey);
    	}
    }
	
	
	/* (non-Javadoc)
	 * @see java.util.LinkedHashMap#removeEldestEntry(java.util.Map.Entry)
	 */
	@Override
	protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
		return this.size() > this.maxElements; 
	}
}