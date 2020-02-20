package LocationAudio;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import Helpers.Helpers;

public class LocationAudioMapper
  extends Mapper<Object, Text, LongWritable, Text> {
	private LongWritable record = null;
	private Text word = new Text();
	
	private int[] DATASET_COLUMNS = { 182 };
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		String uuid = fileName.substring(0, fileName.indexOf('.'));
		String[] tokens = value.toString().split(",");
		String timestamp = tokens[0];
		
		boolean includeRecord = true;
		
		includeRecord = Helpers.formStringFromTokens(tokens, DATASET_COLUMNS, sb, uuid, true);
		
		if (includeRecord) {
			record = new LongWritable(Long.parseLong(timestamp));
			word.set(sb.toString());
			context.write(record, word);
		}
	}
}