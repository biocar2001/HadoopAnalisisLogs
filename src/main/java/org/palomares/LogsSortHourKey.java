package org.palomares;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Clase de ordenacion de las hora de mayor a menor
 * @author Carlos Palomares Campos
 *
 */
public class LogsSortHourKey extends WritableComparator {
	protected LogsSortHourKey() {
		super(LogServiceWritable.class, true);
	}
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		LogServiceWritable cin1 = (LogServiceWritable) w1;
		LogServiceWritable cin2 = (LogServiceWritable) w2;
		System.out.println("Comparando :"+ cin1.toString()+ " and " + cin2.toString());
		
		
		int cmp = cin1.getDay().compareTo(cin2.getDay());		
		if(cmp == 0) {
			return -(cin1.getHour()).compareTo(cin2.getHour());
		}
		return cmp;
		
	}

}
