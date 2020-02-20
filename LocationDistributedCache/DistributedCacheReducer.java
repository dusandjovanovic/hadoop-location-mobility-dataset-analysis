package LocationDistributedCache;

import java.io.*; 
import java.util.*; 
import java.net.URI; 
  
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import Helpers.Helpers;

import org.apache.hadoop.io.NullWritable;

public class DistributedCacheReducer
extends Reducer<LongWritable, Text, Text, Text> {

	private StringBuilder result = new StringBuilder();
	private MultipleOutputs<Text, Text> outputs;
	
	private Text processed = new Text();
	private ArrayList<String> cachedWords = null;
	
	final String DATA_LOCATION = "DATA_LOCATION";
	final String DATA_SENSORS = "DATA_SENSORS";
	
	private long MINIMAL_DISTANCE_METERS = 1000 * 1000;
	
	private String OUTPUT_TEMP = "temporary";
	private String OUTPUT_DONE = "analysed";
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException 
	{ 
		outputs = new MultipleOutputs<Text, Text>(context);
		cachedWords = new ArrayList<String>(); 
		
		URI[] cacheFiles = context.getCacheFiles(); 
		
		if (cacheFiles != null && cacheFiles.length > 0)  
		{ 
			try { 
				String line = ""; 
				FileSystem fs = FileSystem.get(context.getConfiguration()); 
				Path getFilePath = new Path(cacheFiles[0].toString()); 
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath))); 
				
				while ((line = reader.readLine()) != null) { 
					String[] words = line.split(" "); 
					for (int i = 0; i < words.length; i++) { 
						cachedWords.add(words[i]);
					} 
				} 
			} 
			catch (Exception e) { 
				System.out.println("Unable To Read the Cached File."); 
				System.exit(1); 
			} 
		} 
	}

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] output = new String[13];
		
		for (Text record : values) {
			String[] tokens = Helpers.formTokensFromRecord(record);
			
			if (tokens[0].equals(DATA_LOCATION)) { // Location data processing
				double latitude = Double.parseDouble(tokens[2]);
				double longitude = Double.parseDouble(tokens[3]);
				
				cachedWords.forEach((cached) -> {
					String[] cachedTokens = Helpers.formTokensFromCachedRecord(cached);
					try {
						double cachedLat = Double.parseDouble(cachedTokens[11]);
						double cachedLon = Double.parseDouble(cachedTokens[12]);
						
						if (Helpers.distanceInM(latitude, longitude, cachedLat, cachedLon) > MINIMAL_DISTANCE_METERS) {
							output[0] = tokens[0];
							output[1] = tokens[1];
							output[2] = tokens[2];
							output[3] = tokens[3];
							output[4] = cachedTokens[11];
							output[5] = cachedTokens[12];
							output[6] = cachedTokens[13];
							output[7] = cachedTokens[14];
						}
					}
					catch(Exception err) {
						//System.out.println(err.toString());
					}
				});
			}
			else if (tokens[0].equals(DATA_SENSORS)) { // Sensor data processing 
				output[8] = tokens[0];
				output[9] = tokens[1];
				output[10] = tokens[2];
				output[11] = tokens[3];
				output[12] = tokens[4];
			}
		}
		
		result.append(Arrays.toString(output));
		result.append("\n");
		
		outputs.write(OUTPUT_TEMP, key, NullWritable.get());
		
		processed.set(result.toString());
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		outputs.write(OUTPUT_DONE, null, processed);
		outputs.close();
	}
}