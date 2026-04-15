# Own docs for regarding incremental reading.
Egna tankar och docs skrivna för djupare förståelse för incremental reading och fördelar/trade offs med en 'watermark'/'checkpoint'-fil som trackar bronze filer som redan blivit processerade.


## Incremental reading.
Vad jag förstått det så finns det **två** vägar att gå här. Dessa är:
- A: 'High water mark' (Dvs, en timestamp på när någonting blivit processat). 

- B: 'File State Tracking' (Dvs, den vägen jag ska gå i detta projekt).

---
### Metod A, 'The high watermark'
Det är det vanligaste och mer eleganta sättet att jobba, så som det sköts i 'Produktion' om jag inte misstolkat det. Vad processen innebär är att när mitt Silver script startar så börjar det med att titta i den befintliga silver-tabellen och frågar: `SELECT MAX(created_at)`. Det är mitt `watermark`. Sen så läser den från Bronze men filtrerar DIREKT på `Where event_time > watermark`. På så sätt processeras enbart data som skapats *efter* min senaste körning.

### Metod B, 'File state tracking' (Den vägen jag valt att gå)
Här bygger jag en 'mekanism' som gör att jag skapar en liten lokal fil, typ `processed_files.json/csv/parquet`, eller en kontrolltabell i en databas som loggar namnet på varje enskild `.parquet`-fil som har lästs från Bronze. Nästa gång jag kör mitt script listar det alla filer i bronze mappen, jämför med "kvittot" och bearbetar endast de nya filerna som inte finns i min `processed_files.`


- Båda dom här metoderna eliminerar extra arbete med att processera all min historiska data från scratch vid körning(Så som jag gör i skrivande stund). Det kommer underlätta framöver när jag har hämtat hem mycket mer live data ifrån mitt pollande av githubs API samt hämtat ner data ifrån Github Archives. 

- Meningen är att det ska eliminera den exekveringstid jag står med just nu. I början tar det lika lång tid som innan, men efter endast **EN** körning kommer en stor mängd data redan finnas i min `processed_files.` och kommer ej gås igenom igen om jag inte gör någon ändring i mina script.

### Fördelar VS Nackdelar.
Eftersom att jag fortfarande är student och inte har några riktiga "overhead" kostnader i form av cloud-storage eller inte tillräckligt mycket data för att behöva mer utrymme i form av en server så kommer jag använda mig utav checkpoint alternativet(**B**). MEN med det sagt så ser jag redan brister. 

**Potentiella brister / bottlenecks:**
Nu skriver jag en fil för att tracka mina processed filer. Vad händer när jag har en fil för att tracka antal filer?

- Det börjar litet, men för varje fil som kommer in i mitt system eller processas läggs en till rad till osv osv osv. 
    - OM jag har gått igenom 100miljoner - 500miljoner filer innebär det att min checkmark fil innehåller 500MILJONER RADER AV ENBART FILER JAG GÅTT IGENOM.
    - Mitt script måste FÖRST öppna en GIGANTISK fil, ladda in X rows i minnet, lista min bronze folder och sen jämföra mina två listor för att enbart hitta några nya filer. Det är **extremt ineffektivt**

Cloud API kostnader (**LIST problemet**).  

- Om jag tänker efter på hur 'skalbart' projektet är innebär det en helt annan approach. Då måste jag ha med overhead kostnader i minnet. För att lagra allting uppe i molnet t.ex och ta reda på den datan(filer) jag lagrat där.
    - För att exakt veta vilka mappar/filer jag har lagrat i molnet måste jag göra ett API-anrop som heter `LIST`. Om jag nu har miljontals med filer så tar en `LIST`-operation lång tid och leverantörer tar betalt för **VARJE** anrop. Att ständigt scanna efter miljontals med filer kommer kosta väldigt mycket.


## För mitt egna Data Lake projekt och MvP:
För min MvP duger det gott och väl med en checkpoint fil. Det ger mig filspårningen jag behöver och framförallt det ger mig den *precision* som är bra att ha. Om jag använder ett datum-`watermark` och enbart läser in filer från idag och ett event "trillar" in försent vilket är en möjlighet i ett distribuerande system så kommer mitt `watermark` att missa den sena filen helt. Filspårning via en `checkpoint` fil garanterar att jag aldrig missar någonting oavsett *NÄR* den landar i mappen. Så för just min MVP är detta rätt väg att gå då jag inte har någon typ av overhead att tänka på, det som kan behövas om jag väljer att skala och bredda det jag söker efter efter MvP v3 är nådd kan vara lagringsutrymme, det är enkelt löst med mer hårdvara som är kostnadseffektivt.

### Hur löser stora företag det här problemet i verkligheten?
Värt att tänka på ifrån den research jag har gjort som är värd att tänka på när jag går ut på LIA är just två lösningar som storföretagen har kommit fram till.

1) **Tabellformat (Delta Lake / Apache Iceberg)**: Istället för att spara råa Parquet filer(Som jag gör i min MvP), så sparar dom datan i format som Delta Lake. De har inbyggda transaktionsloggar som snabbt berättar för Spark exakt vilka filer som har lagts till sedan förra körningen. De slipper bygga spårningen själv.

2) **Cloud Event Notifications:** Istället för att Datalaken frågar "Finns det några nya filer?" ställer man in molnet så att t.ex `S3` skickar ett meddelande (Till en Kafka queue / liknande) varje gång en fil landar. På så vid bearbetar pipelinen bara det som står i kö.