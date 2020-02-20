package LocationMinMax;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import Helpers.Helpers;

public class LocationMinMaxReducer
extends Reducer<LongWritable, Text, Text, Text> {

	private MultipleOutputs<Text, Text> outputs;
	
	final String DATA_LOCATION = "DATA_LOCATION";
	final String DATA_SENSORS = "DATA_SENSORS";
	
	private long MINIMAL_DISTANCE_METERS = 10 * 100000;
	private long MINIMAL_TIMEOFFSET_HOURS = 120 * 24;
	
	private long TARGET_TIMESTAMP = 1449601597;
	private double TARGET_LAT = 32.882408;
	private double TARGET_LON = -117.234661;

	private TreeMap<Double, String> tmap; 
	private Double minValue = 999.99;
	private Double maxValue = 0.0;
	private double avgValue = 0.0;
	private double countValue = 0.0;
	private int N = 20;
	
	private String OUTPUT_TEMP = "temporary";
	private String OUTPUT_DONE = "analysed";
	  
    @Override
    public void setup(Context context) throws IOException, InterruptedException 
    { 
    	outputs = new MultipleOutputs<Text, Text>(context);
        tmap = new TreeMap<Double, String>();
    } 

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		boolean includeRecord = true;
		String[] output = new String[7];
		
		for (Text record : values) {
			String[] tokens = Helpers.formTokensFromRecord(record);
			
			if (tokens[0].equals(DATA_LOCATION)) { // Location data processing
				long timestamp = key.get();
				double latitude = Double.parseDouble(tokens[2]);
				double longitude = Double.parseDouble(tokens[3]);
				
				if (Helpers.distanceInM(latitude, longitude, TARGET_LAT, TARGET_LON) > MINIMAL_DISTANCE_METERS)
					includeRecord = false;
				if (Helpers.differenceTimeH(timestamp, TARGET_TIMESTAMP) > MINIMAL_TIMEOFFSET_HOURS)
					includeRecord = false;
				
				output[0] = tokens[0];
				output[1] = tokens[1];
				output[2] = tokens[2];
				output[3] = tokens[3];
			}
			else if (tokens[0].equals(DATA_SENSORS)) { // Sensor data processing 				
				output[4] = tokens[0];
				output[5] = tokens[1];
				output[6] = tokens[2];
			}
		}
		
		if (includeRecord) {
			try {
				double magnitudeSpectrumEneryBand = Double.parseDouble(output[6]);
				
				if (magnitudeSpectrumEneryBand < minValue)
					minValue = magnitudeSpectrumEneryBand;
				if (magnitudeSpectrumEneryBand > maxValue)
					maxValue = magnitudeSpectrumEneryBand;
				
				avgValue += magnitudeSpectrumEneryBand;
				countValue++;
				
				tmap.put(magnitudeSpectrumEneryBand, Arrays.toString(output));
				
				if (tmap.size() > N)
		            tmap.remove(tmap.firstKey()); 
			}
			catch (Exception err) {
				System.err.println(err.toString());
			}
		}
		
		outputs.write(OUTPUT_TEMP, key, Arrays.toString(output));
	}

	@Override
    public void cleanup(Context context) throws IOException, InterruptedException 
    {
		avgValue /= countValue;
		Double avg = avgValue;
		outputs.write(OUTPUT_DONE, new Text("MIN_VALUE"), new Text(minValue.toString()));
		outputs.write(OUTPUT_DONE, new Text("MAX_VALUE"), new Text(maxValue.toString()));
		outputs.write(OUTPUT_DONE, new Text("AVG_VALUE"), new Text(avg.toString()));

        for (Map.Entry<Double, String> entry : tmap.entrySet())  
        { 
        	Text tokens = new Text(entry.getValue());
        	Text magnitude_value = new Text(entry.getKey().toString());
        	outputs.write(OUTPUT_DONE, magnitude_value, tokens);
        }
        
        outputs.close();
    } 
}