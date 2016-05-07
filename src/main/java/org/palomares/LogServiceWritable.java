package org.palomares;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class LogServiceWritable implements WritableComparable<LogServiceWritable> {
	  private String day;
	  private static final String MONTHYEAR="/11/2014-";//resto de la fecha sera una coonstante puesto que siempre analizaremos logs del mismo año y mes
	  private String hour;
	  
	public static String getMonthyear() {
		return MONTHYEAR;
	}

	  private String evento;

	  public LogServiceWritable() {}

	  public LogServiceWritable(String hour, String day, String evento) {
	  	this.day = day;
	  	this.hour = hour;
	  	this.evento = evento;
	  }

	  /*GETTERS AND SETTERS*/
	    public String getDay() {
			return day;
		}
	
		public void setDay(String day) {
			this.day = day;
		}
	
		public String getHour() {
			return hour;
		}
	
		public void setHour(String hour) {
			this.hour = hour;
		}
	
		public String getEvento() {
			return evento;
		}
	
		public void setEvento(String evento) {
			this.evento = evento;
		}

	  /*Hashcode and equals*/
	  
	  @Override
	  public String toString() {
	    return "["+day + MONTHYEAR + hour + "] ";
	  }
	  
	  
	  
	  @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((day == null) ? 0 : day.hashCode());
		result = prime * result + ((evento == null) ? 0 : evento.hashCode());
		result = prime * result + ((hour == null) ? 0 : hour.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LogServiceWritable other = (LogServiceWritable) obj;
		if (day == null) {
			if (other.day != null)
				return false;
		} else if (!day.equals(other.day))
			return false;
		if (evento == null) {
			if (other.evento != null)
				return false;
		} else if (!evento.equals(other.evento))
			return false;
		if (hour == null) {
			if (other.hour != null)
				return false;
		} else if (!hour.equals(other.hour))
			return false;
		return true;
	}

	// *********************
	  // * Writable interface
	  // *********************
	  public void readFields(DataInput in) throws IOException {
	    day = in.readUTF();
	    evento = in.readUTF();
	    hour = in.readUTF();
	  }

	  public void write(DataOutput out) throws IOException {
		out.writeUTF(day);
		out.writeUTF(evento);
		out.writeUTF(hour);
	    
	  }

	  // *********************
	  // * Comparable interface
	  // *********************  
	  
	  @Override
	  public int compareTo(LogServiceWritable o) {
		if(day.equals(o.day) && evento.equals(o.evento) && hour.equals(o.hour)){
			return 0;
		}else{
			return 1;
		}
	  	 
	 }
			    
	}
