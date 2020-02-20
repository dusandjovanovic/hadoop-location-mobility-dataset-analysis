package Helpers;

import java.util.Arrays;

import org.apache.hadoop.io.Text;


public class Helpers {
	
	private static String DELIMITER = ",";
	private static String SEPARATOR = "-";
	private static String DATA_LOCATION = "DATA_LOCATION";
	private static String DATA_SENSORS = "DATA_SENSORS";
	private static Integer DATA_LOCATION_LENGTH = 3;
	
	private static Helpers single_instance = null;
	
	public static Helpers getInstance() 
    { 
        if (single_instance == null) 
            single_instance = new Helpers(); 
  
        return single_instance; 
    } 
	
	public static double distanceInM(double lat1, double lon1, double lat2, double lon2) {
		double earthRadius = 6371000;
	    double dLat = Math.toRadians(lat2-lat1);
	    double dLng = Math.toRadians(lon2-lon1);
	    double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
	               Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
	               Math.sin(dLng/2) * Math.sin(dLng/2);
	    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
	    float dist = (float) (earthRadius * c);

	    return dist;
    }
	
	public static double differenceTimeH(long timestamp1, long timestamp2) {
		return Math.abs(timestamp1 - timestamp2) / 60 / 60;
	}
	
	public static String[] formTokensFromRecord(Text record) { 
		return record.toString().split(DELIMITER);
	}
	
	public static String[] formTokensFromCachedRecord(String record) { 
		return record.split(DELIMITER);
	}
	
	public static boolean formStringFromTokens(String[] tokens, int[] tokenIndexes, StringBuilder builder, String uuid, boolean dumpData) {
		boolean valid = true;
		
		if (tokens.length == DATA_LOCATION_LENGTH) {
			builder.append(DATA_LOCATION);
			builder.append(DELIMITER);
			builder.append(uuid);
			builder.append(DELIMITER);
			builder.append(tokens[1]);
			builder.append(DELIMITER);
			builder.append(tokens[2]);
			
			if (tokens[1].equals("nan") || tokens[2].equals("nan"))
				valid = false;
		}
		else { 
			builder.append(DATA_SENSORS);
			builder.append(DELIMITER);
			builder.append(uuid);
			
			for (int index : tokenIndexes) 
			{
				builder.append(DELIMITER);
				builder.append(tokens[index]);
				if (tokens[index].equals("nan") || tokens[index].equals("null") || tokens[index].isEmpty())
					valid = false;
			}
		}
		
		if (dumpData) {
			builder.append(DELIMITER);
			builder.append(Arrays.toString(tokens).replaceAll(DELIMITER, SEPARATOR));
		}
		
		return valid;
	} 
}
