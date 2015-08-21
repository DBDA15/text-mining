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
            String keyString = key.toString();
            if (keyString.contains(",")) {
            	keyString.replace(",", "(comma)");
            }
            sb.append(key   == this ? "(this Map)" : keyString);
            sb.append('=');
            String valueString = value.toString();
            if (valueString.contains(",")) {
            	valueString.replace(",", "(comma)");
            }
            sb.append(value == this ? "(this Map)" : valueString);
            if (! i.hasNext())
                return sb.append('}').toString();
            sb.append(';').append(' ');
        }
	}

	
	
}
