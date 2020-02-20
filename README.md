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
