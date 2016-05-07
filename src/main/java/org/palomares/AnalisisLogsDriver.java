package org.palomares;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Description: Clase Driver que lanzara el job con la configuracion que establezcamos
 * @author Carlos Palomares Campos
 *
 */
public class AnalisisLogsDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		//Exigimos 2 parametros :  Directorio de entrada y directorio de salida donde iran los ficheros
		if (args.length != 2) {
			System.out
					.printf("Usage: AnalisisLogsDriver <input dir> <output dir>");
			System.exit(-1);
		}
		//Borramos los ficheros de la ejecucion anterior si es que la hubiera habido
		Path oPath = new Path(args[1]);
		FileSystem.get(oPath.toUri(), getConf()).delete(oPath, true);
		
		//Estancia del job
		Job job = Job.getInstance(getConf(), "Logs linux");
		job.setJarByClass(AnalisisLogsDriver.class);
		job.setJobName("Logs linux");
		
		//Path de entrada y de salida
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//MultiOutPuts de salida con esta clase
		job.setInputFormatClass(TextInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		//Numero de reducers - uno para logs del kernel y otro para el resto
		job.setNumReduceTasks(2);
		
		//salida de los mapper
		job.setMapOutputKeyClass(LogServiceWritable.class);
		job.setMapOutputValueClass(LogValueEventWritable.class);
		
		//Partitioner para emitir info a 2 reducers, uno para eventos kernel y otro para el resto
		job.setPartitionerClass(LogsPartitioner.class);
		job.setSortComparatorClass(LogsSortHourKey.class);
		
		//Establecemos mapper y reducer del job
		job.setMapperClass(LogsMapper.class);
		job.setReducerClass(LogsReducer.class);
		
		job.waitForCompletion(true);
		
		//Contadores
		Counters counters = job.getCounters();
		CounterGroup cg = counters.getGroup("logs");

		//Metemos contadores a un hashmap para su posterior ordenacion
		HashMap<String,Integer> mapLogs = new HashMap<String,Integer>();
		Iterator<Counter> i = cg.iterator();
		while (i.hasNext()) {

			GenericCounter gc = (GenericCounter) i.next();
			if (!mapLogs.containsKey(gc.getName())) {
				mapLogs.put(gc.getName(), (int)gc.getValue());
			} 
		}
		//Ordenamos los contadores de mayor a menor
		ValueComparator bvc =  new ValueComparator(mapLogs, ValueComparator.DES);
        TreeMap<String,Integer> sortedmapLogs= new TreeMap<String,Integer>(bvc);
        sortedmapLogs.putAll(mapLogs);
		System.out.print("========================= CONTADORES ==============================");
		System.out.println("\n");
		for (Entry<String, Integer> entry : sortedmapLogs.entrySet()) {
			String k = entry.getKey();
			Integer v = entry.getValue();
			System.out.print(k + ":");
			System.out.println(v);
		}
		
		return 0;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run( new Configuration(),	new AnalisisLogsDriver(), args);
		
		System.exit(exitCode);
	}

}

