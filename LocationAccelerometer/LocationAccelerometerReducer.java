package LocationAccelerometer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import Helpers.Helpers;

public class LocationAccelerometerReducer
extends Reducer<LongWritable, Text, Text, Text> {

	private MultipleOutputs<Text, Text> outputs;
	
	private final String DATA_LOCATION = "DATA_LOCATION";
	private final String DATA_SENSORS = "DATA_SENSORS";
	
	private long MINIMAL_DISTANCE_METERS = 10 * 100;

	private HashMap<Double[], ArrayList<String>> hmap; 
	private double TRESHOLD = 2.0;
	
	private String OUTPUT_TEMP = "temporary";
	private String OUTPUT_DONE = "analysed";
	  
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
    	outputs = new MultipleOutputs<Text, Text>(context);
    	hmap = new HashMap<Double[], ArrayList<String>>();
    }

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] output = new String[7];
		
		for (Text record : values) {
			String[] tokens = Helpers.formTokensFromRecord(record);
			
			if (tokens[0].equals(DATA_LOCATION)) { // Location data processing
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
		
		try {
			if (Double.parseDouble(output[6]) > TRESHOLD) {
				boolean shouldAddLocation = true;
				for(Map.Entry<Double[], ArrayList<String>> entry : hmap.entrySet()) {
				    Double[] presentKey = entry.getKey();
				    ArrayList<String> value = entry.getValue();
				    double latitude = Double.parseDouble(output[2]);
					double longitude = Double.parseDouble(output[3]);
				    if (Helpers.distanceInM(latitude, longitude, presentKey[0], presentKey[1]) < MINIMAL_DISTANCE_METERS) {
				    	value.add(output[6]);
				    	hmap.put(presentKey, value);
				    	shouldAddLocation = false;
				    }
				}
				if (shouldAddLocation) {
					ArrayList<String> timestampList = new ArrayList<String>();
					timestampList.add(output[6]);
					Double[] newLocation = {Double.parseDouble(output[2]), Double.parseDouble(output[3])};
					hmap.put(newLocation, timestampList);
				}
			}
		}
		catch(Exception err) {
			System.err.println(err.toString());
		}
	
		outputs.write(OUTPUT_TEMP, key, Arrays.toString(output));
	}

	@Override
    public void cleanup(Context context) throws IOException, InterruptedException 
    { 
		Integer mostDenseLocationCount = 0;
		Double[] mostDenseLocation = {0.0, 0.0};
		for(Map.Entry<Double[], ArrayList<String>> entry : hmap.entrySet()) {
		    Double[] presentKey = entry.getKey();
		    ArrayList<String> value = entry.getValue();
		    if (value.size() > mostDenseLocationCount) {
		    	mostDenseLocationCount = value.size();
		    	mostDenseLocation = presentKey;
		    }
		}
		
		Text outputLocation = new Text(Arrays.toString(mostDenseLocation));
		Text outputSensorReadingOnLocation = new Text(hmap.get(mostDenseLocation).toString());
		outputs.write(OUTPUT_DONE, outputLocation, outputSensorReadingOnLocation);
		outputs.close();
    } 
}