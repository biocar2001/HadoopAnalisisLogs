package org.palomares;

import java.util.Comparator;
import java.util.Map;

 /**
 * Description: Comparador de valores en una map para ordenarlos de mayor a menor o al reves
 * @author cloudera
 *
 */
public class ValueComparator implements Comparator<String> {
	
	static String ASC = "ASC";
	static String DES = "DES";

    Map<String, Integer> base;
    String order;
    
    public ValueComparator(Map<String, Integer> base, String order) {
        this.base = base;
        this.order = order;
    }

    public int compare(String a, String b) {
    	int order = (this.order.equals("ASC") ? 1 : -1);
        if (base.get(a) >= base.get(b))
            return (1 * order);
        else
            return (-1 * order);
    }
}