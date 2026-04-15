# Own docs regarding porting over from Pandas to Pyspark.
---

## Arkitekturbeslut & Insikter: Portning från Pandas till PySpark

Detta dokument sammanfattar de viktigaste konceptuella och tekniska skillnaderna vid migrering av transformationer i Silver-lagret från Pandas till distribuerad PySpark.

## 1. Minneshantering och Exekvering (Eager vs. Lazy)

* **Pandas (Eager Evaluation):** Läser in all data i minnet (RAM) på en gång och bearbetar den sekventiellt som en enda tabell. Fungerar utmärkt för 58 000 rader, men leder ofrånkomligen till "Out of Memory" (OOM)-krascher vid skalan av 58 miljoner rader. Transformationer sker *direkt* när koden körs.

* **PySpark (Lazy Evaluation):** Arbetar distribuerat. Istället för att ladda all data i minnet bygger Spark en exekveringsplan (ett logiskt träd av transformationer). Koden exekveras först när en "Action" anropas (t.ex `.write`, `.count()`, `.collect()`)

    * *Konceptuell liknelse:* Pandas lagar maten direkt så fort du säger ett steg. Spark skriver ner hela receptet, optimerar ordningen, och lagar maten först när tallriken faktiskt behövs.

    * *Parallellism:* Lokalt med `local[*]` delar Spark upp datan i partitioner och bearbetar dem parallellt över alla datorns tillgängliga CPU-kärnor.

## 2. Scheman: Från gissningar till stenhårda kontrakt

* **Pandas:** Infererar (beräknar/gissar) scheman och datatyper automatiskt. Smidigt vid prototyper, men farligt i produktion då felaktig data kan tyst smyga sig in och krascha nedströms-system.

* **PySpark:** Här definierar jag schemat **explicit** via `StructType` och `StructField`.
    * *Designbeslut:* Detta är medvetet. Jag vill inte att Spark ska gissa. Detta explicita schema fungerar som ett strikt kontrakt mellan Bronze och Gold. Det garanterar datakvalitet och stabilitet.

## 3. Datautvinning: Iterrows -> UDF -> Spark Native
Att platta ut (flatten) nästlad JSON-data var den största arkitektoniska förändringen.

* **Nivå 1: Pandas `iterrows()`:** En Python `for-loop` som körs sekventiellt rad för rad. Extremt ineffektivt för stora datamängder.

* **Nivå 2: PySpark UDF (User Defined Function):** Ett sätt att köra anpassad Python-kod på Spark-partitioner. 
    * *Prestandafällan:* Spark körs i en JVM (Java Virtual Machine). Om jag använder en Python-UDF måste all data serialiseras från JVM till Python, bearbetas, och sedan serialiseras tillbaka. Det är som att skicka ett paket från Sverige till USA för att öppna det, och sedan skicka tillbaka innehållet. Det skapar en massiv overhead.

* **Nivå 3: Spark Native (`get_json_object`):** Den optimala lösningen.

    * *Varför det är bäst:* Inbyggda Spark-funktioner körs direkt i JVM (ofta optimerade i Sparks *Tungsten*-motor) i C/Java-hastighet. Datat lämnar aldrig motorn. `get_json_object` låter mig extrahera specifika fält direkt ur en rå JSON-sträng utan att behöva deserialisera hela objektet först. Det är snabbt och extremt minneseffektivt.

## 4. Hive-Partitionering och vikten av "Zero Padding"
I slutet av pipelinen skrivs datan till Parquet-filer partitionerade efter år, månad och dag (`year=YYYY/month=MM/day=DD`).

* **Problemet:** Utan formatering skapas mappar som `month=3` och `month=10`. För filsystem är det bara textsträngar. När frågemotorer (som DuckDB, AWS Athena eller dbt) läser dessa uppstår problem med *lexikografisk sortering*. Filsystemet kommer sortera filerna som `1, 10, 11, 2, 3`, vilket förstör kronologin vid läsning och optimering.

* **Lösningen (Zero Padding):** Genom att tvinga fram inledande nollor med `date_format(col, "MM")` och `"dd"` skapas partitioner som `month=03`. Sorteringen blir då korrekt: `01, 02, 03... 10, 11`. Detta är en liten men affärskritisk detalj för att downstream-verktyg ska kunna göra snabba "partition pruning"-sökningar.

---
**Slutsats / Overhead-notering:**
Spark har en oundviklig uppstartstid (overhead) för att spinna upp JVM och exekveringsplanen. För små filer kan Pandas verka snabbare lokalt, men när datamängderna skalar upp är det PySparks distribuerade, lazy evaluerade arkitektur som förhindrar systemkrascher.

***