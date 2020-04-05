# Hadoop aplikacija za analizu mobilnosti korisnika u gradu

Ulazni dataset je preuzet sa linka - http://extrasensory.ucsd.edu/.

Razviti i testirati/evaluirati Hadoop aplikaciju koja će na osnovu raspoloživih podataka:
1. Odrediti broj slogova (pojava, torki, događaja) čiji atributi zadovoljavaju određeni uslov i registrovani su na određenoj lokaciji u datom vremenu.
2. Naći minimalne, maksimalne, srednje vrednosti određenog atributa, broj pojavljivanja, kao i top N slogova (pojava, torki, događaja) na zadatoj lokaciji i/ili vremenu po uslovu definisanom nad atributima.
3. Naći lokaciju (sa određenim prečnikom) na kojoj se nalazi najveći broj uređaja sa visokim očitavanjima magnitude (parametra senzora accelerometer) u svim vremenskim periodima – može da se protumači kao mesto na kome se najviše koriste mobilni uređaji.
4. Odrediti skup slogova koji zadovoljavaju uslov viskog očitavanja zvuka (sa određenom donjom granicom), odnosno parametra audio_magnitude na određenoj lokaciji u svim vremenskim periodima -  može da se protumači kao ispitivanje da li je konkretna lokacija bučno i zauzeto mesto.
5. Napraviti korelaciju izabranih podataka sa podacima, u okviru dataseta ili iz eksternog izvora (npr. sa http://download.geofabrik.de/) koji su u prostorno-vremenskoj vezi sa prethodnim, i koji bi bili prosleđeni mehanizmom distribuiranog keša.
6. Aplikaciju testirati na klasteru računara i evaluirati rad aplikacije na različitom broju računara u klasteru.

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
      Helpers.java
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

> Odrediti broj slogova (pojava, torki, događaja) čiji atributi zadovoljavaju određeni uslov i registrovani su na određenoj lokaciji u datom vremenu.

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

Ključ koji će se koristiti u reducer klasi je **timestamp** vrednost s obzirom da treba okupiti lokacione podatke i podatke sa senzora koji odgovaraju istoj vremenskoj vrednosti. To znači da će reducer klasa uz istu vrednost timestamp-a, odnosno ključa, imati skup vrednosti lokacionih podataka i drugi odvojeni skup vrednosti podataka očitanih sa senzora.

`[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.880849, -117.237451], 
[DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 2.360237]`

Ovo je primer dva sloga koji u prikazanom obliku stižu do reducer-a. Konstante `DATA_SENSORS` i `DATA_LOCATION` opisuju podatke koji dolaze sa različitih izvora i početni su tokeni ovih slogova. Ova dva sloga se vezuju za isti `LongWritable` ključ koji, kao što je več pomenuto, predstavlja timestamp vrednost.

U kodu koji sledi dat je pregled ključnog ponašanja reducer klase. Može se videti da je klasa u potpunosti konfigurabilna zahvaljujući konstantama. `MINIMAL_DISTANCE_METERS` određuje minimalo potrebno rastojanje u metrima između dve tačke. `MINIMAL_TIMEOFFSET_HOURS` određuje minimalnu razliku u satima između dve timestamp vrednosti. Konstane koje počinju prefiksom `TARGET` ukazuju na ciljne vrednosti. Tokeni se zatim izvlače i upoređuju sa ovim konstantama kako bi se odredila njihova pripadnost rešenju.

Pomoćnim metodama `Helpers.distanceInM` i `Helpers.differenceTimeH` se izdvojene vrednosti tokena upoređuju sa ciljnim vrednostima i utvrđuje da li slog treba da bude uključen u rezultat ili ne. Treba za slog takođe proveriti i izabranu kolonu senzorskih očitavanja.

```java
private Text processed = new Text();
private Long processedCount = (long) 0.0;

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
		processedCount++;
		result.append(Arrays.toString(output));
		result.append("\n");
	}

	outputs.write(OUTPUT_TEMP, key, Arrays.toString(output));

	processed.set(result.toString());
}

@Override
public void cleanup(Context context) throws IOException, InterruptedException {
	outputs.write(OUTPUT_DONE, new Text("FOUND_RECORDS"), new Text(processedCount.toString()));
	outputs.write(OUTPUT_DONE, null, processed);
	outputs.close();
}
```

Može se videti da se formirani niz Stringova `output` formira iz dva koraka. Prvo se za isti ključ, odnosno timestamp vrednost, posmatraju lokacione vrednosti i upoređuju sa ciljnim konstantama. Zatim, isto važi i za prikupljenu vrednost sa senozra. U ovom slučaju to je vrednost `battery_state_is_charging` i samo ukoliko je pozitivna slog se smatra relevantnim - što je i cilj ovog zadatka.

Promenljiva `includeRecord` se vezuje za svaki ključ i nakon obrade lokacionih/senzorskih podataka ukazuje na to da li taj slog treba uvrstiti u rezultat. Zbog preglednosti nije preslikan ceo slog već samo ključni podaci koji pristižu sa reducer-a.

Na kraju, na izlazu se može videti **broj slogova koji zadovoljavaju postavljene uslove** kao i podaci ovih slogova u nastavku.

Primer izlaza:
```
FOUND_RECORDS	931
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.882134, -117.234553, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.882134, -117.234553, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.882444, -117.234586, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.882458, -117.234607, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884762, -117.243499, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884762, -117.243505, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
[DATA_LOCATION, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 32.884767, -117.243477, DATA_SENSORS, 0A986513-7828-4D53-AA1F-E02D6DF9561B, 1.000000]
...
```

Dakle, na izlazu reducer-a nalaze se slogovi koji zadovoljavaju lokacionu i vremensku zavisnost, a pritom i vrednosnu relevantnu jednom očitavanju senzora - svi relevantni podaci su prikazani na izlazu. **`FOUND_RECORDS`** vrednost je rešenje postavljenog problema.

## Zadatak 2)

> Naći minimalne, maksimalne, srednje vrednosti određenog atributa, broj pojavljivanja, kao i top N slogova (pojava, torki, događaja) na zadatoj lokaciji i/ili vremenu po uslovu definisanom nad atributima.

Za potrebe drugog zadatka koriste se klase `LocationMinMaxMapper.java` i `LocationMinMaxReducer.java`.

Kako je analogija mapper klase jako slična prethodnom zadatku, neće biti objašnjavana detaljno. Treba napomenuti da se ponovo izvlači **jedna od kolona podataka očitanih sa senzora** i koriste identični prefiksi za podatke koji potiču sa lokacionih izvora i senzora. Ovi podaci se vezuju za **timestamp vrednost kao ključ**. Važno je da se izabrani atribut senzorskih očitavanja u reducer klasi koristi za selekciju u mapper klasi - po svojoj vrednosti.

Što se tiče reducer klase, implementacioni detalji mogu se videti u kodu koji sledi. Klasa je takođe konfigurabilna i sadrži iste konstane poput prošle uz dodatu `N` vrednost koja određuje koliko top-elemenata će biti uključeno u rezultat.

Pomoćnim metodama `Helpers.distanceInM` i `Helpers.differenceTimeH` se izdvojene vrednosti tokena upoređuju sa ciljnim vrednostima i utvrđuje da li slog treba da bude uključen u rezultat ili ne. Pored ovoga, postoji i logika za praćenje važnih vrednosti `minValue`, `maxValue`, `avgValue` i `countValue`. Na primer, vrednosti `avgValue` i `countValue` zavise od svakog sloga koji poseduje validnu vrednost relevantnog atributa.

```java
private TreeMap<Double, String> tmap; 
private Double minValue = 999.99;
private Double maxValue = 0.0;
private Double avgValue = 0.0;
private Double countValue = 0.0;
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
	avgValue /= countValue;
	outputs.write(OUTPUT_DONE, new Text("MIN_VALUE"), new Text(minValue.toString()));
	outputs.write(OUTPUT_DONE, new Text("MAX_VALUE"), new Text(maxValue.toString()));
	outputs.write(OUTPUT_DONE, new Text("AVG_VALUE"), new Text(avgValue.toString()));
	outputs.write(OUTPUT_DONE, new Text("ATTRIBUTE_COUNT"), new Text(countValue.toString()));

	for (Map.Entry<Double, String> entry : tmap.entrySet())  
	{ 
		Text tokens = new Text(entry.getValue());
		Text magnitude_value = new Text(entry.getKey().toString());
		outputs.write(OUTPUT_DONE, magnitude_value, tokens);
	}

	outputs.close();
} 
```

Za vođenje evidencije minimalne, maksimalne, prosečne vrednosti i vrednosti ukupnog broja validnog ponavljanja atributa koriste se promenljive tih imena koje se mogu videti na početku koda. Važno je da svaki slog koji ima validan izabrani atribut utiče na promenu vrednosti pomenutih promenljivih.

Za potrebe nalaženja **top-N elemenata** reducer prikupljene podatke smešta u `TreeMap` strukturu sa ograničenom dužinom od N. Maksimalna, minimalna i srednja vrednost se računaju uzimajući u obzir svaki slog. Na kraju, u override-ovanoj metodi `cleanup(Context)` se vrši punjenje izlaza.

Primer izlaza:
```
MIN_VALUE	0.0
MAX_VALUE	3.467959
AVG_VALUE	0.22254551741862044
ATTRIBUTE_COUNT	3502.0
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

Dakle, na izlazu reducer-a nalaze se slogovi koji zadovoljavaju lokacionu i vremensku zavisnost, a pritom se po vrednosti izabranog atributa nalaze u top-N skupu. Pre izlistavanja ovih slogova nalaze se vrednosti promenljivih maksimalne, minimalne, srednje i vrednosti broja pronađenog atributa.

## Zadatak 3)

> Naći lokaciju (sa određenim prečnikom) na kojoj se nalazi najveći broj uređaja sa visokim očitavanjima magnitude (parametra senzora accelerometer) u svim vremenskim periodima – može da se protumači kao mesto na kome se najviše koriste mobilni uređaji.

Za potrebe trećeg zadatka koriste se klase `LocationAccelerometerMapper.java` i `LocationAccelerometerReducer.java`.

Kako je analogija mapper klase jako slična prvom zadatku, neće biti objašnjavana detaljno. Treba napomenuti da se ponovo izvlači jedna od kolona senzora (**accelerometer magnitude** vrednost) i koriste identični prefiksi za podatke koji potiču sa lokacionih izvora i senzora. Ovi podaci se vezuju za timestamp vrednost kao ključ. Važno je da se izabrani atribut senzorskih očitavanja u reducer klasi koristi za izračunavanja u mepper klasi - po svojoj vrednosti.

Što se tiče reducer klase, implementacioni detalji mogu se videti u kodu koji sledi. Klasa je konfigurabilna po prethodnoj analogiji, s tim što ne uključuje filtriranje po vremenu, već se uzimaju u obzir **podaci svih vremenskih izvora**.

```java
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
```

Za nalaženje lokacije se uzima u obzir konstana `MINIMAL_DISTANCE_METERS`, a za filtriranje vrednosti sa senzora akcelerometra konstanta praga `TRESHOLD`. Logika se bazira na tome da treba naći **skupove "bliskih" očitavanja podataka**. Odnosno, ako viže slogova različitih ključeva postoje uokviru prečnika `MINIMAL_DISTANCE_METERS` oni će činiti jedan skup. Pritom, prvi pronađeni slog koji ne pripada nijednom skupu će biti referentni slog tog skupa, odnosno epicentar.

Na kraju, u metodi `cleanup(Context)` se iz heš mape bira samo skup sa najviše elemenata. Referentna lokacija skupa, kao i svi elementi se prikazuju na izlazu. Elementi su zbog preglednosti samo očitavanja sa senzora koja pripadaju tom skupu.

Primer izlaza:
```
[32.882466, -117.234563]	[2.031780, 2.457509, 2.086027, 2.436246, 2.006950, 2.498900, 2.057491, 2.167026, 2.080274, 2.138992, 2.169596, 2.081357, 2.261530, 2.531129, 2.198963, 2.619804, 2.347578, 2.067516, 2.533997, 2.469944, 2.553077, 2.428680, 2.545529, 2.047766, 2.126125, 2.568496, 2.578853, 2.209051, 2.465159, 2.514485, 2.628503, 2.113009, 2.419530, 2.092362, 2.034774, 2.055202, 2.726070, 2.569663, 2.419090, 2.330864, 2.156727, 2.119523, 2.514203, 2.238154, 2.191705, 2.529372, 2.024186, 2.487336, 2.363583, 2.313552, 2.396475, 2.270248, 2.195126, 2.199230, 2.069535, 2.196051, 2.189151, 2.421333, 2.045536, 2.273849, 2.551881, 2.520372, 2.684049, 2.500901, 2.525715, 2.673258, 2.540631, 2.525412, 2.519567, 2.496687, 2.429996, 2.378320, 2.657684, 2.682784, 2.650231, 2.582649, 2.476192, 2.459081, 2.116752, 2.518011, 2.535757, 2.523841, 2.504349, 2.504515, 2.614284, 2.518233, 2.570651, 2.559228, 2.463162, 2.745002, 2.060366, 2.785243, 2.817384, 2.825640, 2.686698, 2.800566, 2.788636, 2.769874, 2.754278, 2.373089, 2.241865, 2.327761, 2.356970, 2.776504, 2.735976, ...
```

Na izlazu se dakle prvo može videti par geografske dužine i širine referentnog elementa skupa - što predstavlja najgušću lokaciju. Zatim, u nizu se mogu videti i sva očitavanja senzora iz tog skupa. Ova očitavanja realno predstavljaju sve uređaje koji su dovoljno blizu referentnog elementa skupa i pritom imaju visoke očitane vrednosti atributa.

## Zadatak 4)

> Odrediti skup slogova koji zadovoljavaju uslov viskog očitavanja zvuka (sa određenom donjom granicom), odnosno parametra audio_magnitude na određenoj lokaciji u svim vremenskim periodima -  može da se protumači kao ispitivanje da li je konkretna lokacija bučno i zauzeto mesto.

Za potrebe četvrtog zadatka koriste se klase `LocationAudioMapper.java` i `LocationAudioReducer.java`.

Kako je analogija mapper klase jako slična prvom zadatku, neće biti objašnjavana detaljno. Treba napomenuti da se ponovo izvlači jedna od kolona senzora (audio magnitude vrednost) i koriste identični prefiksi za podatke koji potiču sa lokacionih izvora i senzora. Ovi podaci se vezuju za timestamp vrednost kao ključ. Važno je da se izabrani atribut senzorskih očitavanja u reducer klasi koristi za izračunavanja u mepper klasi - po svojoj vrednosti.

Dodatno, u ovom zadatku se preslikava **celokupni slog** u poslednji token i prosleđuje reducer klasi jer se na izlazu očekuje u tom obliku.

Što se tiče reducer klase, implementacioni detalji mogu se videti u kodu koji sledi. Klasa je konfigurabilna po prethodnoj analogiji, s tim što ne uključuje filtriranje po vremenu, već se uzimaju u obzir **podaci svih vremenskih izvora**.

```java
private long MINIMAL_DISTANCE_METERS = 10 * 100;
private double TARGET_LAT = 32.882408;
private double TARGET_LON = -117.234661;

private double TRESHOLD = 1.0;

private String OUTPUT_TEMP = "temporary";
private String OUTPUT_DONE = "analysed";

@Override
public void setup(Context context) throws IOException, InterruptedException 
{ 
outputs = new MultipleOutputs<Text, Text>(context);
} 

@Override
public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	boolean includeRecord = true;
	String[] output = new String[8];

	for (Text record : values) {
		String[] tokens = Helpers.formTokensFromRecord(record);

		if (tokens[0].equals(DATA_LOCATION)) { // Location data processing
			double latitude = Double.parseDouble(tokens[2]);
			double longitude = Double.parseDouble(tokens[3]);

			if (Helpers.distanceInM(latitude, longitude, TARGET_LAT, TARGET_LON) > MINIMAL_DISTANCE_METERS)
				includeRecord = false;

			output[0] = tokens[0];
			output[1] = tokens[1];
			output[2] = tokens[2];
			output[3] = tokens[3];
		}
		else if (tokens[0].equals(DATA_SENSORS)) { // Sensor data processing 
			double audioReading = Double.parseDouble(tokens[2]);

			if (Math.abs(audioReading) <= TRESHOLD)
				includeRecord = false;

			output[4] = tokens[0];
			output[5] = tokens[1];
			output[6] = tokens[2];
			output[7] = tokens[3];
		}
	}

	if (includeRecord) {
		result.append(output[7]);
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

Za nalaženje lokacije se uzima u obzir konstana `MINIMAL_DISTANCE_METERS`, a za filtriranje vrednosti sa audio senzora konstanta praga `TRESHOLD`.

Na kraju, u metodi `cleanup(Context)` se na izlaz prosleđuju svi slogovi koji zadovoljavaju pomenute uslove. Obimnost ovih slogova ukazuje da li se lokacija `TARGET_LAT`/`TARGET_LON` može smatrati zauzetim mestom. Za razliku od prethodnih zadataka, prikazuju se slogovi u izvornom obliku sirovih očitavanja sa senzora.

Primer izlaza:
```
[1444079161- 0.996815- 0.003529- -0.002786- 0.006496- 0.995203- 0.996825- 0.998502- 1.748756- 6.684605- 5.043970- 0.000042- 0.000364- 0.000761- 0.005429- 0.429853- 0.173685- 0.148988- 0.002331- 0.004614- -0.996790- 0.003269- 0.003521- 0.003539- 0.106920- 0.516842- 0.255494- 0.002558- 0.001510- 0.001832- 0.002531- 0.001526- 0.002196- 0.003230- 2.236059- 6.532865- 5.149616- 2.818909- 3.757026- 2.952941- 4.312930- 1.766920- 4.193949- 0.107787- 0.000412- 0.000448- -0.000541- 0.001705- 0.001987- 0.001144- -0.372543- 0.175125- -0.033004- 618.751929- 0.784768- -0.313058- 1.038986- 618.243941- 618.796487- 619.260475- 2.594321- 6.684611- 5.043589- 0.000015- 0.000103- 0.000070- 0.000684- 0.429649- 2.430594- 0.131063- 223.246192- 97.503453- -568.776302- 1.009838- 0.827983- 0.769873- -0.069913- 0.046582- 0.021992- 0.999996- 0.999996- 0.999996- 0.999996- 0.999995- 1032.508157- 17.195149- 12.121621- 25.390972- 1024.812178- 1033.146650- 1041.537325- 2.220749- 6.214470- 5.045673- 0.004896- 0.023827- 0.022606- 0.083165- 0.432260- 2.533760- 0.114619- -0.592000- -55.824000- -1030.912000- 9.109201- 9.684680- 17.198147- -0.045248- -0.117629- 0.028053- 4.775776- 4.820572- 5.866857- 4.938766- 5.503634- 5.034010- 1.224514- 1.730564- 1.093057- 1.796074- 5.045700- 0.004994- 0.023702- 0.022539- 0.083756- 0.999849- 0.999835- 0.999836- 0.999832- 0.999819- nan- nan- nan- nan- nan- nan- nan- nan- nan- 4.000000- 0.064895- 0.070473- 108.230255- 108.239944- nan- nan- 65.000000- 10.000000- 10.102999- 2.312832- 0.000029- 0.000032- -0.000067- -0.000073- 0.000111- 0.000120- -4.219657- -0.012806- -1.298291- 0.094373- -1.220977- -0.851300- -1.656516- -0.898563- -0.503573- -0.518072- -0.907876- -0.681724- -0.683515- 2.276349- 1.271966- 1.177478- 0.670053- 0.389200- 0.535904- 0.468701- 0.358315- 0.401412- 0.408432- 0.277176- 0.416881- 0.263832- -2.605413- 2.605549- 0.000000- 1.000000- 0.000000- 0.000000- 0.000000- 0.000000- 0.000000- 1.000000- 0.000000- 0.000000- 0.000000- 0.000000- 1.000000- 0.000000- 0.000000- 1.000000- 0.000000- 0.000000- 0.000000- 0.000000- 0.000000- 1.000000- 0.000000- 1.000000- 0.000000- 0.000000- nan- nan- nan- 0.000000- nan- 0.460000- 0.381436- nan- 0.000000- 0.000000- 0.000000- 1.000000- 1.000000- 0.000000[1444079161- 0.996815- 0.003529- -0.002786- 0.006496- 0.995203- 0.996825- 0.998502- 1.748756- 6.684605- 5.043970- 0.000042- 0.000364- 0.000761- 0.005429- 0.429853- 0.173685- 0.148988- 0.002331- 0.004614- -0.996790- 0.003269- 0.003521- 0.003539- 0.106920- 0.516842- 0.255494- 0.002558- 0.001510- 0.001832- 0.002531- 0.001526- 0.002196- 0.003230- 2.236059- 6.532865- 5.149616- 2.818909- 3.757026- 2.952941- 4.312930- 1.766920- 4.193949- 0.107787- 0.000412- 0.000448- -0.000541- 0.001705- 0.001987- 0.001144- -0.372543- 0.175125- -0.033004- 618.751929- 0.784768- -0.313058- 1.038986- 618.243941- 618.796487- 619.260475- 2.594321- 6.684611- 5.043589- 0.000015- 0.000103- 0.000070- 0.000684- 0.429649- 2.430594- 0.131063- 223.246192- 97.503453- -568.776302- 1.009838- 0.827983- 0.769873- -0.069913- 0.046582- 0.021992- 0.999996- 0.999996- 0.999996- 0.999996- 0.999995- 1032.508157- 17.195149- 12.121621- 25.390972- 1024.812178- 1033.146650- 1041.537325- 2.220749- 6.214470- 5.045673- 0.004896- 0.023827- 0.022606- 0.083165- 0.432260- 2.533760- 0.114619- -0.592000- -55.824000- -1030.912000- 9.109201- 9.684680- 17.198147- -0.045248- -0.117629- 0.028053- 4.775776- 4.820572- 5.866857- 4.938766- 5.503634- 5.034010- 1.224514- 1.730564- 1.093057- 1.796074- 5.045700- 0.004994- 0.023702- 0.022539- 0.083756- 0.999849- 0.999835- 0.999836- 0.999832- 0.999819- nan- nan- nan- nan- nan- nan- nan- nan- nan- 4.000000- 0.064895- 0.070473- 108.230255- 108.239944- nan- nan- 65.000000- 10.000000- 10.102999- 2.312832- 0.000029- 0.000032- -0.000067- -0.000073- 0.000111- 0.000120- -4.219657- -0.012806- -1.298291- 0.094373- -1.220977- -0.851300- -1.656516- -0.898563- -0.503573- -0.518072- -0.907876- -0.681724- -0.683515- 2.276349- 1.271966- 1.177478- 0.670053- 0.389200- 0.535904- 0.468701- 0.358315- 0.401412- 0.408432- 0.277176- 0.416881- 0.263832- -2.605413- 2.605549- 0.000000- 1.000000- 0.000000- 0.000000- 0.000000- 0.000000- 0.000000- 1.000000- 0.000000- 0.000000- 0.000000- 0.000000- 1.000000- 0.000000- 0.000000- 1.000000- 0.000000- 0.000000- 0.000000- 0.000000- 0.000000- 1.000000- 0.000000- 1.000000- 0.000000- 0.000000- nan- nan- nan- 0.000000- nan- 0.460000- 0.381436- nan- 0.000000- 0.000000- 0.000000- 1.000000- 1.000000- 0.000000- 0.000000- 0.000000- 0- 1- 0- nan- nan- 0- nan- 0- 1- 1- nan- nan- nan- 0- nan- nan- 0- nan- nan- nan- 0- 0- nan- nan- 0- 0- nan- nan- 0- nan- nan- nan- nan- nan- nan- 0- 0- nan- nan- 0- nan- nan- nan- nan- 0- nan- nan- nan- 1- 1- nan- 2]
...
```

Dakle, za izabranu lokaciju, na izlazu treba dostaviti **celokupne slogove** sa svim senzorskim očitavanjima. Pritom se uzimaju u obzir samo oni slogovi koji su relativno blizu navedene referentne lokacije.

## Zadatak 5)

> Napraviti korelaciju izabranih podataka sa podacima, u okviru dataseta ili iz eksternog izvora (npr. sa http://download.geofabrik.de/) koji su u prostorno-vremenskoj vezi sa prethodnim, i koji bi bili prosleđeni mehanizmom distribuiranog keša.

Za potrebe petog zadatka koriste se klase `LocationDistributedCacheMapper.java` i `LocationDistributedCacheReducer.java`.

Kako je analogija mapper klase jako slična prvom zadatku, neće biti objašnjavana detaljno. Treba napomenuti da se izvlači **tri kolona senzora** i koriste identični prefiksi za podatke koji potiču sa lokacionih izvora i senzora. Ovi podaci se vezuju za timestamp vrednost kao ključ. Važno je da se izabrani atribut senzorskih očitavanja u reducer klasi koristi za **nalaženje korelacije sa keširanim dataset-om**, pre svega koriste se vresnoti lokacionih atributa.

Dalje, da bi se koristilo keširanje i distribuiranje stranog dataseta do svakog čvora, koristi se podešavanje okruženja:

```java
try { 
	// the complete URI(Uniform Resource  
	// Identifier) file path in HDFS for cached files
	job.addCacheFile(new URI("/home/ubuntu/ncdc/distributed_cache/insurance_data_sample.csv")); 
} 
catch (Exception e) { 
	System.out.println("Distributed Cache File Not Found."); 
	System.exit(1); 
} 
```

Datoteka `insurance_data_sample.csv` je strani dataset koji između ostalog sadrži i slogove koji su relativno blizu lokacijama izvornog dataseta. Ideja je u rezultat uvrstiti kombinovane slogove koji su u **lokacionoj korelaciji**. A pritom, i navesti kombinacije atributa koje su prepoznate na tim lokacijama.

Što se tiče reducer klase, implementacioni detalji mogu se videti u kodu koji sledi. Klasa je konfigurabilna po prethodnoj analogiji, s tim što ne uključuje filtriranje po vremenu, već se uzimaju u obzir **podaci svih vremenskih izvora**.

```java
private long MINIMAL_DISTANCE_METERS = 1000 * 1000;

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
```

Za nalaženje lokacione zavisnosti uzima se u obzir konstana `MINIMAL_DISTANCE_METERS`. Pritom se traže poklapanja sa svakim slogom keširanog dataset-a. Ukoliko ovakva poklapanja postoje, slogovi se kombinuju i treba ih uključiti na izlazu. Svaki kombinovani slog koji se nađe na izlazu će uključivati obe bliske lokacije koje su u korelaciji, kao i **vrednosti očitanih atributa iz oba dataset-a**.

Na kraju, u metodi `cleanup(Context)` se na izlaz prosleđuju sve kombinacije slogova za koje se može reći da su relativno blizu. Svaki elemenat izlaza kombinuje tokene iz oba dataset-a što potencijalno može nagoveštavati korelaciju podataka.

Primer izlaza:
```
[DATA_LOCATION, 00EABED2-271D-49D8-B599-1D4A09240601, 32.882408, -117.234661, 28.06444, -82.77459, Residential, Masonry, DATA_SENSORS, 00EABED2-271D-49D8-B599-1D4A09240601, 0.001144, -0.372543, 0.175125]
[DATA_LOCATION, 00EABED2-271D-49D8-B599-1D4A09240601, 32.882466, -117.234577, 28.06444, -82.77459, Residential, Masonry, DATA_SENSORS, 00EABED2-271D-49D8-B599-1D4A09240601, 0.001105, -0.425875, 0.138763]
[DATA_LOCATION, 00EABED2-271D-49D8-B599-1D4A09240601, 32.882466, -117.234563, 28.06444, -82.77459, Residential, Masonry, DATA_SENSORS, 00EABED2-271D-49D8-B599-1D4A09240601, 0.001360, -0.506993, 0.181114]
[DATA_LOCATION, 00EABED2-271D-49D8-B599-1D4A09240601, 32.882470, -117.234562, 28.06444, -82.77459, Residential, Masonry, DATA_SENSORS, 00EABED2-271D-49D8-B599-1D4A09240601, 0.001164, -0.565952, 0.108946]
[DATA_LOCATION, 00EABED2-271D-49D8-B599-1D4A09240601, 32.882422, -117.234651, 28.06444, -82.77459, Residential, Masonry, DATA_SENSORS, 00EABED2-271D-49D8-B599-1D4A09240601, 0.114795, -0.318971, 0.097856]
[DATA_LOCATION, 00EABED2-271D-49D8-B599-1D4A09240601, 32.882438, -117.234630, 28.06444, -82.77459, Residential, Masonry, DATA_SENSORS, 00EABED2-271D-49D8-B599-1D4A09240601, 0.001838, -0.352214, 0.011116]
[DATA_LOCATION, 00EABED2-271D-49D8-B599-1D4A09240601, 32.882481, -117.234569, 28.06444, -82.77459, Residential, Masonry, DATA_SENSORS, 00EABED2-271D-49D8-B599-1D4A09240601, 0.001473, -0.408883, -0.118182]
[DATA_LOCATION, 00EABED2-271D-49D8-B599-1D4A09240601, 32.882425, -117.234608, 28.06444, -82.77459, Residential, Masonry, DATA_SENSORS, 00EABED2-271D-49D8-B599-1D4A09240601, 0.001175, -0.524412, 0.008613]
...
```

Na primeru prvog sloga izlaza, `DATA_SENSORS, 00EABED2-271D-49D8-B599-1D4A09240601, 0.001144, -0.372543, 0.175125` predstavlja očitavnja senzora izvornog dataseta. Sa druge strane, `28.06444, -82.77459, Residential, Masonry` potiče iz keširanog dataset-a.

Kako je ovo pokazni primer, konstanta koja određuje da li su dve tačke dovoljno blizu je visoka, odnosno `MINIMAL_DISTANCE_METERS` je 1000km. U pokazanom slogu distanca u km između `(32.882408, -117.234661)` i `(28.06444, -82.77459)` je 535km što je po konfiguraciji klase dovoljno blizu i može se smatrati da su **slogovi u korelaciji**. Ponovo, ovo je pokazni primer edukativnog karaktera, u realnim analizama bi granična vrednost koja diktira korelaciju bila znatno manja od 1000km.

## Zadatak 6)

> Aplikaciju testirati na klasteru računara i evaluirati rad aplikacije na različitom broju računara u klasteru.

U procesu razvijanja aplikacije su ravnopravno korišćeni **lokalni** i **pseduo-distribuirani mod**. Za potrebe izvršavanja aplikacije na klasteru računara korišćen je besplatni trial provajdera **CloudxLab**.

![alt text][screenshot_cloudlabx]

[screenshot_cloudlabx]: meta/screenshot_cloudlabx.png

Sve izvorne datoteke se pre svega kloniraju sa ovog repozitorijuma, neophodno je dostaviti i potrebnu `buid.xml` konfiguracionu datoteku. Zatim, sa terminala paltforme treba generisati izvršnu `jar` datoteku projekta.

`$ git clone https://github.com/dusandjovanovic/hadoop-location-mobility-dataset-analysis.git`

`$ ant jar`

`$ hadoop jar RunMapReduce.jar /ncdc/uuid_merged ncdc/output`

Nakon postavljanja ulaznih datoteka i pokretanja nad `hadoop-om` sa odgovarajućim argumentima, job se izvršava na klasteru. Pritom, podešavanja broja čvorova treba uraditi pre toga.

![alt text][screenshot_jobstart]

[screenshot_jobstart]: meta/screenshot_jobstart.png

Nakon pokretanja više poslova u različitim konfiguracijama, pregled izvršenih poslova može se videti na `Dashboard-u`.

![alt text][screenshot_jobs]

[screenshot_jobs]: meta/screenshot_jobs.png

![alt text][screenshot_jobdesc]

[screenshot_jobdesc]: meta/screenshot_jobdesc.png

Rezultat obrade i izvršavanja aplikacije na klastaru nalzi se na `hdfs-u`. Rezultati su identični kao i u slučaju lokalnih testiranja. 

![alt text][screenshot_hdfs]

[screenshot_hdfs]: meta/screenshot_hdfs.png
