package de.hpi.fgis.dbda.textmining.MainTask_flink;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class HashMapWithSemicolons<K, V> extends HashMap<K, V> {
	
	public HashMapWithSemicolons() {
		super();
	}
	
	public HashMapWithSemicolons(Map<K, V> m) {
		super(m);
	}

	@Override
	public String toString() {
        Iterator<Entry<K,V>> i = entrySet().iterator();
        if (! i.hasNext())
            return "{}";

        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (;;) {
            Entry<K,V> e = i.next();
            K key = e.getKey();
            V value = e.getValue();
            if (key.toString().equals(",")) {
            	sb.append(key   == this ? "(this Map)" : "(comma)");
            }
            else {
            	sb.append(key   == this ? "(this Map)" : key);
            }
            sb.append('=');
            sb.append(value == this ? "(this Map)" : value);
            if (! i.hasNext())
                return sb.append('}').toString();
            sb.append(';').append(' ');
        }
	}

	
	
}
