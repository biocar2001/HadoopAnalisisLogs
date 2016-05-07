package org.palomares;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Carlos Palomares Campos
 * Usamos el patron in-mapper combining:
 * La ventaja del in-mapper combining sobre el Combiner tradicional, es que el primero puede que se ejecute o puede que no, 
 * no tenemos control sobre ello. Mientras que el in-mapper combining podemos hacer que se ejecute siempre.
 *  La segunda ventaja de este patrón de diseño es controlar cómo se lleva a cabo exactamente.
 *  Otra ventaja es que con el Combiner se reduce el número de datos que llegan al Shuffle and Sort 
 *  para luego enviarlos al Reducer, pero realmente no reduce el número de pares key/value emitidos por el Mapper.
 *
 */
public class LogsMapper extends Mapper<Object, Text, LogServiceWritable, LogValueEventWritable> {

	private static Map<String, Integer> EventMap;
	private static final int FLUSH_COUNTER = 1000;
	
	@Override
	 protected void setup(Context context)
	   throws IOException, InterruptedException {
		System.out.println("Entro al mapper");
	  if(EventMap == null ){
		  EventMap = new HashMap<String, Integer>();
	  }
	  
	}
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//Leemos cada una de las lineas haciendo split por el espacio 
		String[] line = value.toString().split(" ");
		//Obtenemos el dia de la linea
		String day = line[1];
		//Obtenemos la hora al detalle
		String hhmmss = line[2];
		String [] LineTime = hhmmss.split(":");//nos quedaremos solo con la hora
		//No queremos el host en un principio
		//String host = line[3];
		//Obtenemos el evento y nos quedamos solo con el nombre del mismo
		String tmpEvent = line[4];
		String[] tmp = tmpEvent.split("(\\[[0-9]*\\])|:");
		String evento = tmp[0].toString().trim();
		if(!evento.equals("") && !evento.startsWith("vmnet")){//No emitimos los eventos que contengan la palabra "vmnet" tal y como especifica el enunciado del examen
			
			String eventoMapper = day+"@"+evento+"@"+LineTime[0].toString();
			//Sumamos el numero de veces que aparece un evento
			if(EventMap.containsKey(eventoMapper)){
				EventMap.put(eventoMapper, EventMap.get(eventoMapper)+1);
			}else{
				EventMap.put(eventoMapper.toString(), 1);
			}
			
			if(EventMap.size() >= FLUSH_COUNTER){
			    flush(context, false);
			}
		}
		
	}
	/**
	 * Description: Nos aseguramos que el esta estanciado o no para sobrecargar al mapper con variables innecesarias
	 * @return
	 */
	public Map<String, Integer> getMap() {

		if (null == EventMap)

		EventMap = new HashMap<String, Integer>();

		return EventMap;

	}
	/**
	 * 
	 * Description: Metodo para emitir los valores
	 * @param context
	 * @param force
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void flush(Context context, boolean force) throws IOException, InterruptedException{
	   Map<String, Integer> map = getMap();
	   //Comprobacion necesari para lanzar el metodo en el cleanup y que no falle aunque nos encontremos en el final de todos los logs
	   if (!force) {
		int size = map.size();
		if (size < FLUSH_COUNTER)
		return;

	  }
	   
	  LogServiceWritable keyOutPutB = new LogServiceWritable();
	  LogValueEventWritable valueOutPut = new LogValueEventWritable();
	     
	  for(Map.Entry<String, Integer> entry : EventMap.entrySet()){
		  String [] keyAll = entry.getKey().split("@");
		  keyOutPutB.setDay(keyAll[0]);
		  keyOutPutB.setHour(keyAll[2]);
		  keyOutPutB.setEvento(keyAll[1]);
		  
		  Integer rep = EventMap.get(entry.getKey());
		  valueOutPut.setEvento(keyOutPutB.getEvento());
		  valueOutPut.setConcurrencias(rep);
		  
		  context.write(keyOutPutB, valueOutPut);
	  }
	  EventMap.clear();
	 }
	protected void cleanup(Context context) throws IOException,	InterruptedException {

		flush(context, true); // Hacemos el flush siempre aunque estemos al final de todos los ficheros

	}
	
}
