package org.palomares;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class LogValueEventWritable implements WritableComparable<LogValueEventWritable> {
	  private String evento;
	  private Integer concurrencias;
	  
	  public LogValueEventWritable() {}

	  public LogValueEventWritable(String evento, Integer concurrencias) {
	  	this.concurrencias = concurrencias;
	  	this.evento = evento;
	  }

	  /*GETTERS AND SETTERS*/
	  	public String getEvento() {
			return evento;
		}

		public void setEvento(String evento) {
			this.evento = evento;
		}

		public Integer getConcurrencias() {
			return concurrencias;
		}

		public void setConcurrencias(Integer concurrencias) {
			this.concurrencias = concurrencias;
		}
	  /*Hashcode and equals*/
	 
	@Override
	  public String toString() {
	    return evento+":"+concurrencias;
	  }
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((concurrencias == null) ? 0 : concurrencias.hashCode());
		result = prime * result + ((evento == null) ? 0 : evento.hashCode());
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
		LogValueEventWritable other = (LogValueEventWritable) obj;
		if (concurrencias == null) {
			if (other.concurrencias != null)
				return false;
		} else if (!concurrencias.equals(other.concurrencias))
			return false;
		if (evento == null) {
			if (other.evento != null)
				return false;
		} else if (!evento.equals(other.evento))
			return false;
		return true;
	}

	// *********************
	  // * Writable interface
	  // *********************
	  public void readFields(DataInput in) throws IOException {
	    evento = in.readUTF();
	    concurrencias = in.readInt();
	  
	  }

	  public void write(DataOutput out) throws IOException {
	    out.writeUTF(evento);
		out.writeInt(concurrencias);
	    
	  }

	  // *********************
	  // * Comparable interface
	  // *********************  
	  
	  @Override
	  public int compareTo(LogValueEventWritable o) {
		  int cmp = evento.compareTo(o.evento);
		  	if (cmp != 0) {
				return cmp;
			} else{
				return concurrencias.compareTo(o.concurrencias);
			}
	  	 
	  }
	
		    
	}
