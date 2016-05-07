package org.palomares;

import org.apache.hadoop.mapreduce.Partitioner;
/**
 * Description: Partitioner mediante el cual emitimos los logs del kernel hacia un reducer y el resto al otro
 * @author Carlos Palomares Campos
 *
 */
public class LogsPartitioner extends Partitioner<LogServiceWritable, LogValueEventWritable>
{

    @Override
    public int getPartition(LogServiceWritable key, LogValueEventWritable value, int numReduceTasks) 
        {
    		  //Hay un comentario en el ejercicio de como desarrolllar la practica que no comprendo:
    		  //2) Determinar cuantos días de logs en total vais a tratar para poder escribir el partitioner.
    		  //Dado que no se especifica que no tratemos a ninguno en concreto, yo tratare todos.
    		  //Mandamos a cada uno de los reducers en funcion del value de la key: kernel
    		System.out.println("proceso :"+ value.toString());
    		  if(key.getEvento().toString().contains("kernel"))
              {
    			  System.out.println("Kernel :"+ value.toString());	
                  return 0;
              }else
              {
            	  System.out.println("Otros :"+ value.toString());
            	  return 1;
              } 
        }
}
