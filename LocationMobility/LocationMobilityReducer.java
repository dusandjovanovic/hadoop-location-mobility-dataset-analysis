package LocationMobility;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import Helpers.Helpers;

public class LocationMobilityReducer
extends Reducer<LongWritable, Text, Text, Text> {

	private StringBuilder result = new StringBuilder();
	private MultipleOutputs<Text, Text> outputs;
	
	private Text processed = new Text();
	
	private final String DATA_LOCATION = "DATA_LOCATION";
	private final String DATA_SENSORS = "DATA_SENSORS";
	
	private long MINIMAL_DISTANCE_METERS = 10 * 1000;
	private long MINIMAL_TIMEOFFSET_HOURS = 90 * 24;
	
	private long TARGET_TIMESTAMP = 1449601597;
	private double TARGET_LAT = 32.882408;
	private double TARGET_LON = -117.234661;
	
	private String OUTPUT_TEMP = "temporary";
	private String OUTPUT_DONE = "analysed";

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		outputs = new MultipleOutputs<Text, Text>(context);
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
				double batteryStateIsCharging = Double.parseDouble(tokens[2]);
				
				if (batteryStateIsCharging == 0.0)
					includeRecord = false;
				
				output[4] = tokens[0];
				output[5] = tokens[1];
				output[6] = tokens[2];
			}
		}
		
		if (includeRecord) {
			result.append(Arrays.toString(output));
			result.append("\n");
		}
		
		outputs.write(OUTPUT_TEMP, key, Arrays.toString(output));
		
		processed.set(result.toString());
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		outputs.write(OUTPUT_DONE, null, processed);
		outputs.close();
	}
}