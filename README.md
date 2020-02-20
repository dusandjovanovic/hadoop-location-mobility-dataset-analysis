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
