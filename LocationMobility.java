import java.net.URI;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import LocationAccelerometer.LocationAccelerometerMapper;
import LocationAccelerometer.LocationAccelerometerReducer;
import LocationAudio.LocationAudioMapper;
import LocationAudio.LocationAudioReducer;
import LocationDistributedCache.DistributedCacheMapper;
import LocationDistributedCache.DistributedCacheReducer;
import LocationMinMax.LocationMinMaxMapper;
import LocationMinMax.LocationMinMaxReducer;
import LocationMobility.LocationMobilityMapper;
import LocationMobility.LocationMobilityReducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
 
public class LocationMobility { 
 
  public static void main(String[] args) throws Exception { 
     if (args.length != 2) { 
       System.err.println("Usage: <input path> <output path>"); 
       System.exit(-1); 
     }
      
     Job job = Job.getInstance();
     job.setJarByClass(LocationMobility.class);
     job.setJobName("LocationMobility");
 
     FileInputFormat.addInputPath(job, new Path(args[0])); 
     FileOutputFormat.setOutputPath(job, new Path(args[1])); 
      
// Odrediti broj slogova (pojava, torki, događaja) čiji atributi zadovoljavaju određeni uslov
// i registrovani su na određenoj lokaciji u datom vremenu
//     job.setMapperClass(LocationMobilityMapper.class);
//     job.setCombinerClass(LocationMobilityReducer.class);
//     job.setReducerClass(LocationMobilityReducer.class); 
// 
//     job.setMapOutputKeyClass(LongWritable.class);
//     job.setMapOutputValueClass(Text.class);
//     job.setOutputKeyClass(Text.class);
//     job.setOutputValueClass(Text.class);
//     
// Naći minimalne, maksimalne, srednje vrednosti određenog atributa, broj pojavljivanja,
// kao i top N slogova (pojava, torki, događaja) na zadatoj lokaciji i/ili vremenu po uslovu definisanom nad atributima
     job.setMapperClass(LocationMinMaxMapper.class);
     job.setReducerClass(LocationMinMaxReducer.class); 
   
     job.setMapOutputKeyClass(LongWritable.class);
     job.setMapOutputValueClass(Text.class);
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(Text.class);
//     
// Naći lokaciju (sa određenim prečnikom) na kojoj se nalazi najveći broj uređaja sa visokim očitavanjima magnitude u svim vremenskim periodima
// – može da se protumači kao mesto na kome se najviše koriste mobilni uređaji.
//     job.setMapperClass(LocationAccelerometerMapper.class);
//     job.setReducerClass(LocationAccelerometerReducer.class); 
// 
//     job.setMapOutputKeyClass(LongWritable.class);
//     job.setMapOutputValueClass(Text.class);
//     job.setOutputKeyClass(Text.class);
//     job.setOutputValueClass(Text.class);
//     
// Odrediti skup slogova koji zadovoljavaju uslov viskog očitavanja zvuka (sa određenom donjom granicom), odnosno parametra audio_magnitude na određenoj lokaciji 
// u svim vremenskim periodima - može da se protumači kao ispitivanje da li je konkretna lokacija bučno i zauzeto mesto.
//     job.setMapperClass(LocationAudioMapper.class);
//     job.setReducerClass(LocationAudioReducer.class); 
// 
//     job.setMapOutputKeyClass(LongWritable.class);
//     job.setMapOutputValueClass(Text.class);
//     job.setOutputKeyClass(Text.class);
//     job.setOutputValueClass(Text.class);
     
// Napraviti korelaciju izabranih podataka sa podacima, u okviru dataseta ili iz eksternog izvora (npr. sa http://download.geofabrik.de/) koji su
// u prostorno-vremenskoj vezi sa prethodnim, i koji bi bili prosleđeni mehanizmom distribuiranog keša.
//     job.setMapperClass(DistributedCacheMapper.class);
//     job.setReducerClass(DistributedCacheReducer.class); 
// 
//     job.setMapOutputKeyClass(LongWritable.class);
//     job.setMapOutputValueClass(Text.class);
//     job.setOutputKeyClass(Text.class);
//     job.setOutputValueClass(Text.class);
//     
//     try { 
//         // the complete URI(Uniform Resource  
//         // Identifier) file path in HDFS for cached files
//         job.addCacheFile(new URI("/home/ubuntu/ncdc/distributed_cache/insurance_data_sample.csv")); 
//     } 
//     catch (Exception e) { 
//         System.out.println("Distributed Cache File Not Found."); 
//         System.exit(1); 
//     } 
      
     MultipleOutputs.addNamedOutput(job, "temporary", TextOutputFormat.class, Text.class, NullWritable.class);
	 MultipleOutputs.addNamedOutput(job, "analysed", TextOutputFormat.class, IntWritable.class, NullWritable.class);

	 System.exit(job.waitForCompletion(true) ? 0 : 1);
  } 
}
