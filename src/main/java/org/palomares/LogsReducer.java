package org.palomares;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
/**
 * Description: Reducer para emitir los diferentes logs a diferentes ficheros en funcion del nombre del evento y su fecha
 * @author Carlos Palomares Campos
 *
 */
public class LogsReducer extends
		Reducer<LogServiceWritable, LogValueEventWritable, Text, Text> {
	
	//Variables del reducer
	private MultipleOutputs<Text, Text> mos;
	private Text outputValueFile =  new Text();
	
	
 	
 	/**
 	 * Decription: Metodo setup donde instanciamos el multiOutput
 	 * @param context
 	 */
	
	public void setup(Context context) {
		System.out.println("Entro al reducer");
		mos = new MultipleOutputs<Text, Text>(context);
	 }
 	
 	/**
 	 * Description: Metodo que me duvelve el nombre del fichero deseado
 	 * Puediendo ser para el kernel o para el resto de logs
 	*/
 	public String generateFileName(String dia, boolean isKernel) {
 		String retorno;
 		if (isKernel){
 			retorno="part-dia-"+dia+"-kernel"; 
 		}else{
 			retorno="part-dia-"+dia;
 		}
 		return	retorno;
 	}
 	
 	/**
 	 * Description : Metodo Reducer
 	 */
	@Override
	protected void reduce(LogServiceWritable key, Iterable<LogValueEventWritable> values, Context context)
			throws IOException, InterruptedException {
		String event = "";
		HashMap<String,Integer> mapEvents = new HashMap<String,Integer>();
		
		//Obtengo todas las concurrencias de cada uno de los eventos que llegan al reducer y las sumo para generar los totales
		for (LogValueEventWritable value : values) {
			event = value.getEvento();
			if (!mapEvents.containsKey(event)) {
				mapEvents.put(event, value.getConcurrencias());
			} else {
				mapEvents.put(event, mapEvents.get(event) + value.getConcurrencias());
			}
		}
		//Tratamos el evento kernel
		if(mapEvents.containsKey("kernel")){ 
				outputValueFile.set("kernel" + ":" + mapEvents.get("kernel"));
				String k = "kernel";
	        	Integer v = mapEvents.get("kernel");
	        	
				context.getCounter("logs", k).increment(v);
				mos.write(new Text(key.toString()),outputValueFile,generateFileName(key.getDay(),true));
				mapEvents.remove("kernel");
		}
		
		//Resto de eventos
		if (mapEvents.size() > 0) {
			ValueComparator bvc =  new ValueComparator(mapEvents, ValueComparator.ASC);
	        TreeMap<String,Integer> sortedMapEvents = new TreeMap<String,Integer>(bvc);
	        sortedMapEvents.putAll(mapEvents);
	        StringBuffer valuesEvents = new StringBuffer();
			
			
			for (Map.Entry<String,Integer> entry : sortedMapEvents.entrySet()) {
	        	String k = entry.getKey();
	        	Integer v = entry.getValue();
	        	if (sortedMapEvents.firstKey() == entry.getKey())
	        		valuesEvents.append(k + ":" + v);
	        	else
	        		valuesEvents.append("," + k + ":" + v);
	        	context.getCounter("logs", k).increment(v);
	        }
	        outputValueFile.set(valuesEvents.toString());
	       
			mos.write(new Text(key.toString()),outputValueFile,generateFileName(key.getDay(),false));
			
		}
		
	}
	
	

	/**
	 * Metodo cleanup, donde cerraremos los ficheros de salida donde hemos escrito
	 * @throws InterruptedException 
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();//Cerramos los output
	}

}