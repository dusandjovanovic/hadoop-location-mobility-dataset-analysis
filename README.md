# Hadoop aplikacija za analizu mobilnosti korisnika u gradu

Ulazni dataset je preuzet sa linka - http://extrasensory.ucsd.edu/.

Razviti i testirati/evaluirati Hadoop aplikaciju koja će na osnovu raspoloživih podataka:
1. Odrediti broj slogova (pojava, torki, događaja) čiji atributi zadovoljavaju određeni uslov i registrovani su na određenoj lokaciji u datom vremenu
2. Naći minimalne, maksimalne, srednje vrednosti određenog atributa, broj pojavljivanja, kao i top N slogova (pojava, torki, događaja) na zadatoj lokaciji i/ili vremenu po uslovu definisanom nad atributima
3. Naći lokaciju (sa određenim prečnikom) na kojoj se nalazi najveći broj uređaja sa visokim očitavanjima magnitude (parametra senzora accelerometer) u svim vremenskim periodima – može da se protumači kao mesto na kome se najviše koriste mobilni uređaji.
4. Odrediti skup slogova koji zadovoljavaju uslov viskog očitavanja zvuka (sa određenom donjom granicom), odnosno parametra audio_magnitude na određenoj lokaciji u svim vremenskim periodima -  može da se protumači kao ispitivanje da li je konkretna lokacija bučno i zauzeto mesto.
5. Napraviti korelaciju izabranih podataka sa podacima, u okviru dataseta ili iz eksternog izvora (npr. sa http://download.geofabrik.de/) koji su u prostorno-vremenskoj vezi sa prethodnim, i koji bi bili prosleđeni mehanizmom distribuiranog keša.
6. Aplikaciju testirati na klasteru računara i evaluirati rad aplikacije na različitom broju računara u klasteru

## Pregled dataset-a

Ulazni podaci koji će se obrađivati predstavljaju veliku kolekciju podataka mobilnosti korisnika. Hijerarhija direktorijuma i datoteka koje čine dataset data je u nastavku.

      ~ncdc
        |
        ->/distributed_cache
        |  |
        |  ->insurance_data_sample.csv
        |
        |
        ->/uuid_absolute_locations
        |  |
        |  ->00EABED2-271D-49D8-B599-1D4A09240601.absolute_locations.csv ...
        |
        |
        ->/uuid_features
        |  |
        |  ->00EABED2-271D-49D8-B599-1D4A09240601.absolute_locations.csv ...
        |
        |
        ->/uuid_merged
        |  |
        |  ->00EABED2-271D-49D8-B599-1D4A09240601.merged.csv ...
        |
        |
        ->/uuid_numbers
        |  |
        |  -><multiple text files>

Direktorijum `distributed_cache` sadrži dataset sa trećeg izvora koji će se koristiti u zadatku broj 5).

Direktorijum `uuid_absolute_locations` se sastoji iz više datoteka koje u imenu imaju jedinstveni uuid uređaja sa koga su prikupljani podaci. Prva kolona ovih datoteka je vrednost **timestamp-a**, a preostale kolone se odnose na parametre lokacije korisnika - odnosno geografsku dužinu i širinu.

Direktorijum `uuid_features` se sastoji iz više datoteka koje u imenu imaju jedinstveni uuid uređaja sa koga su prikupljani podaci. Prva kolona ovih datoteka je vrednost **timestamp-a**, a preostalih ~200 kolona su očitavanja sa različitih senzora poput akcelerometra, žiroskopa i slično. Važno je primetiti da je parametar **timestamp** zajednički i da se kao takav treba koristiti za kombinovanje podataka sa različitih pozicija ovih datoteka.

Direktorijum `uuid_merged` sadrži kombinovane podatke lokacija i senzora za svaki jedinstveni uuid. Ovo je namenski oformljen direktorijum, gde se dve različite datoteke spajaju u jednu. Prva datoteka sadrži očitavanja lokacija za jedan uuid, a druga prikupljene vrednosti sa senzora za taj isti uređaj, odnosno uuid. Redovi ovih datoteka su prosto nadovezani.

Direktorijum `uuid_numbers` sadrži više tekstualnih datoteka koje sadrže sve jedinstvene uuid brojeve.

## Lokalno pokretanje na Linux operativnom sistemu

Neophodno je generisati build projekta, na repozitorijumu se može naći već generisana `RunMapReduce.jar` datoteka. U glavnoj klasi treba odkomentarisati željeno podešavanje s obzirom da je korišćena ista aplikacija za svih pet zadataka.

Kada je `.jar` datoteka generisana, treba nad lokalnom `hadoop` konfiguracijom pokrenuti istu. Takođe, dataset direktorijum treba smestiti u `/home` direktorijum lokalnog operativnog sistema. Nakon sledeće komande, uz postavljeno okruženje, dolazi do obrade podataka. Prvi argumenat komande je ulazni direktorijum koji iz koga će sve datoteke biti obrađene. Drugi argumenat je izlazni direktorijum u kome će se naći rezultati obrade.

` $ hadoop jar /home/ubuntu/Desktop/LocationMobility/RunMapReduce.jar ncdc/uuid_merged ncdc/output`

![alt text][screenshot_terminal]

[screenshot_terminal]: meta/screenshot_terminal.png

## Struktura projekta

Za svaki od zadataka postoji poseban `package`, opisno direktorijum, u kome se nalaze klase **mapper-a** i **reducer-a**. Osnovna klasa u kojoj se postavlja `hadoop` job okruženje i setuju mapper/reducer klase je `LocationMobility.java`.

```
/
  Helpers/
      Helpers.class
  LocationAccelerometer/
      LocationAccelerometerMapper.java
      LocationAccelerometerReducer.java
  LocationAudio/
      LocationAudioMapper.java
      LocationAudioReducer.java
  LocationDistributedCache/
      LocationDistributedCacheMapper.java
      LocationDistributedCacheReducer.java
  LocationMinMax/
      LocationMinMaxMapper.java
      LocationMinMaxReducer.java
  LocationMobility/
      LocationMobilityMapper.java
      LocationMobilityReducer.java
  LocationMobility.java
```

Tako, na primer, za zadatak broj 5) koristi se direktorijum `LocationDistributedCache` - tačnije maper i reducer klase definisane u ovom direktorijumu. Ove klase se uvoze u glavnoj `LocationMobility.java` klasi.

## MapReduce i glavna klasa LocationMobility.java

```java
import java.net.URI;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

...
import LocationMobility.LocationMobilityMapper;
import LocationMobility.LocationMobilityReducer;
...
 
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
     job.setMapperClass(LocationMobilityMapper.class);
     job.setCombinerClass(LocationMobilityReducer.class);
     job.setReducerClass(LocationMobilityReducer.class); 
 
     job.setMapOutputKeyClass(LongWritable.class);
     job.setMapOutputValueClass(Text.class);
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(Text.class);
    
     MultipleOutputs.addNamedOutput(job, "temporary", TextOutputFormat.class, Text.class, NullWritable.class);
     MultipleOutputs.addNamedOutput(job, "analysed", TextOutputFormat.class, IntWritable.class, NullWritable.class);
      
     System.exit(job.waitForCompletion(true) ? 0 : 1);
  } 
}
```

Za svaki od zadataka potrebno je koristiti različite klase reducer-a i maper-a. Ove klase se jednom poslu dodeljuju metodatama `job.setMapperClass` i `job.setReducerClass`. Format ulaznih i izlaznih parova ključeva i vrednosti koje se prosleđuju se takođe pojedinačno setuje blokom koda ispod izbora pomenutih klasa.

Okruženju su dodata dva izlaza `temporary` i `analysed`. Prvi izlaz se koristi u procesu građenja drugog poput sistema logovanja gde se korisni podaci ili međurezultati šalju na izlaz. Drugi izlaz je važniji i sadrži rezultat obrade. Naravno, kod svake reducer klase treba da se rukovodi saglasno ovome.

## Deljena ponašanja i klasa Helpers.java

```java
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
	
	public static Helpers getInstance() { 
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
```

Deljena ponašanja koja se koriste u različitim mapper ili reducer klasama su smeštena u Singleton klasi `Helpers.java`.

1) `static double distanceInM` - nalaženje distance u metrima između dve tačke u prostoru
2) `static double differenceTimeH` - nalaženje vremenske razlike u satima između dve timestamp vrednosti
3) `static String[] formTokensFromRecord` - formiranje tokena na osnovu sloga
4) `static String[] formTokensFromCachedRecord` - formiranje tokena na osnovu sloga keširane datoteka
5) `static boolean formStringFromTokens` - formiranje Stringa na osnovu niza tokena
      * konfigurabilna metoda, koristi se za prenos podataka između maper-a i reducer-a 
      * na osnovu svih tokena, preko niza indeksa treba uvrstiti samo odabrane tokene u rezultat
      * sadrži DELIMITER elemenat u vidu karaktera `,`
      * može ili ne mora na kraj niza da preslika ceo izvorni slog

## Zadatak 1)

Za potrebe prvog zadatka koriste se klase `LocationMobilityMapper.java` i `LocationMobilityReducer.java`.

```java
private int[] DATASET_COLUMNS = { 196 };

@Override
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	StringBuilder sb = new StringBuilder();
	String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	String uuid = fileName.substring(0, fileName.indexOf('.'));
	String[] tokens = value.toString().split(",");
	String timestamp = tokens[0];

	boolean includeRecord = true;

	includeRecord = Helpers.formStringFromTokens(tokens, DATASET_COLUMNS, sb, uuid, false);

	if (includeRecord) {
		record = new LongWritable(Long.parseLong(timestamp));
		word.set(sb.toString());
		context.write(record, word);
	}
}
```

Mapper treba da obezbedi potrebne podatke reducer-u. Korišćenjem pomoćne metode izvlače se timestamp, uuid, lat/lng koordinate i token sa rednim brojem 196 zadat konstantom `DATASET_COLUMNS`. U slučaju beležnika lokacija postoji prefiks `DATA_LOCATION`, a u slučaju skupa podataka senzora `DATA_SENSORS`. Implementacioni detalji izvlačenja neophodnih tokena mogu se pogledati u prethono objašnjenoj metodi `Helpers.formStringFromTokens`.

Ključ koji će se koristiti u reducer klasi je **timestamp** vrednost s obzirom da treba okupiti lokacione podatke i podatke sa senzora koji odgovaraju istoj vremenskoj vrednosti.

U kodu koji sledi dat je pregled ključnog ponašanja reducer klase. Može se videti da je klasa u potpunosti konfigurabilna zahvaljujući konstantama. `MINIMAL_DISTANCE_METERS` određuje minimalo potrebno rastojanje u metrima između dve tačke. `MINIMAL_TIMEOFFSET_HOURS` određuje minimalnu razliku u satima između dve timestamp vrednosti. Konstane koje počinju prefiksom `TARGET` ukazuju na ciljne vrednosti. Tokeni se zatim izvlače i upoređuju sa ovim konstantama kako bi se odredila njihova pripadnos rešenju.

```java
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
```

Može se videti da se formirani niz Stringova `output` formira iz dva koraka. Prvo se za isti ključ, odnosno timestamp vrednost, posmatraju lokacione vrednosti i upoređuju sa ciljnim konstantama. Zatim, isto važi i za prikupljenu vrednost sa senozra. U ovom slučaju to je vrednost `battery_state_is_charging` i samo ukoliko je pozitivna slog se smatra relevantnim - što je i cilj ovog zadatka.

Promenljiva `includeRecord` se vezuje za svaki ključ i nakon obrade lokacionih/senzorskih podataka ukazuje na to da li taj slog treba uvrstiti u rezultat. Zbog preglednosti nije preslikan ceo slog već samo ključni podaci koji pristižu sa reducer-a.

Primer izlaza:
```
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.882134, -117.234553, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.882134, -117.234553, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.882444, -117.234586, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.882458, -117.234607, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884762, -117.243499, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884762, -117.243505, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884767, -117.243477, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
...
```

Dakle, na izlazu reducer-a nalaze se slogovi koji zadovoljavaju lokacionu i vremensku zavisnost, a pritom i vrednosnu relevantnu jednom očitavanju senzora - svi relevantni podaci su prikazani na izlazu. 

## Zadatak 2)

Za potrebe prvog zadatka koriste se klase `LocationMinMaxMapper.java` i `LocationMinMaxReducer.java`.

Kako je analogija mapper klase jako slična prethodnom zadatku, neće biti objašnjavana detaljno. Treba napomenuti da se ponovo izvlači jedna od kolona senzora i koriste identični prefiksi za podatke koji potiču sa lokacionih izvora i senzora. Ovi podaci se vezuju za timestamp vrednost kao ključ. Važno je da se izabrani atribut senzorskih očitavanja u reducer klasi koristi za selekciju u mepper klasi - po svojoj vrednosti.

Što se tiče reducer klase, implementacioni detalji mogu se videti u kodu koji sledi. Klasa je takođe konfigurabilna i sadrži iste konstane poput prošle uz dodatu `N` vrednost koja određuje koliko top-elemenata će biti uključeno u rezultat.

```java
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
```

Za potrebe nalaženja top-N elemenata reducer prikupljene podatke smešta u `TreeMap` strukturu sa ograničenom dužinom od N. Maksimalna, minimalna i srednja vrednost se računaju uzimajući u obzir svaki slog. Na kraju, u override-ovanoj metodi `cleanup(Context)` se vrši punjenje izlaza.

Primer izlaza:
```
MIN_VALUE	0.0
MAX_VALUE	3.467959
AVG_VALUE	0.22254551741862044
2.360237	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.880849, -117.237451, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.360237]
2.36092	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884160, -117.241318, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.360920]
2.38021	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.881373, -117.238617, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.380210]
2.382824	[DATA_LOCATION, 00EABED2-271D-49D8-B599-1D4A09240601, 32.882591, -117.234693, DATA_SENSORS, 00EABED2-271D-49D8-B599-1D4A09240601, 2.382824]
2.409278	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.881127, -117.237852, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.409278]
2.411022	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.880441, -117.237178, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.411022]
2.456913	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884798, -117.243554, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.456913]
2.5041	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884820, -117.243504, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.504100]
2.511466	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884776, -117.243476, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.511466]
2.542937	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884793, -117.243524, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.542937]
2.547586	[DATA_LOCATION, 00EABED2-271D-49D8-B599-1D4A09240601, 32.882422, -117.234651, DATA_SENSORS, 00EABED2-271D-49D8-B599-1D4A09240601, 2.547586]
2.575053	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.881049, -117.237824, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.575053]
2.61373	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884783, -117.243506, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.613730]
2.718203	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884646, -117.242512, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.718203]
2.760866	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.880010, -117.235893, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.760866]
2.777368	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.880049, -117.235895, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.777368]
3.015119	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884847, -117.243528, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 3.015119]
3.236268	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.880251, -117.238169, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 3.236268]
3.291168	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.883719, -117.241701, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 3.291168]
3.467959	[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884801, -117.243485, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 3.467959]
```

Dakle, na izlazu reducer-a nalaze se slogovi koji zadovoljavaju lokacionu i vremensku zavisnost, a pritom se po vrednosti izabranog atributa nalaze u top-N skupu.
