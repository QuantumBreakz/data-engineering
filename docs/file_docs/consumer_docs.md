# Own docs for producer.py script
Nästa naturliga steg efter producern är consumern. Den som *LYSSNAR* på kafka topicen och skriver alla events till `Bronze` som Parquet. Consumern är den **andra** halvan av ingestion lagret och när den väl är på plats så är `Bronze` klar. Då är bronze pipelinen klar. Från `Github API -> Producer -> Kafka -> Consumer -> Parquet`

## Snabb teoretisk överblick om consumern.
```
Consumern har ett mycket viktigare och ännu mer komplext jobb än producern. Producern är s.k 'stateless'. Den bryr sig inte om vad som hände tidigare poll cykler. Consumern däremot måste tänka på: 

- BATCHING, dvs samla ihop events och skriva dom som en Parquet fil istället för att skriva en fil per event.

- Offset hantering, Kafka håller koll på vad consumern redan har läst så att OM den kraschar kan den fortsätta där den slutade.

- Partitionering, Skriva till rätt year=/month=/day/ folder automatiskt baserat på eventets timestamp.
```
**Enkel liknelse vs producer och consumer**
- Producern är dum som tåget. Allt den behöver fokusera på är att hålla tiden för avgång. Konstant avgång var 5e minut.

- Consumern däremot är dock mer komplicerad. Passande liknelse för consumern kan vara en självkörande bil på en motorväg.
    - På motorvägen måste "bilen" alltid hålla koll på trafikflödet(`batching`). Den har en automatisk hastighetshållare som anpassar sig beroende på hastigheten på vägen(`offset handling`) och sen måste den se till att ligga i RÄTT fil för att kunna ta RÄTT avfart på motorvägen för att nå sin destination(`partitionering`)


## Visuell förklaring över consumer flödet av data

![Consumer flow](../visuals/consumer_visual.png)

```
Det kritiska att lägga märke till i diagrammet är ordningen på de två sista stegen. 
WRITE kommer före COMMIT. Det är inte slumpmässigt. 

Om jag committade offset till Kafka först och sen kraschade innan Parquet-filen hann skrivas till disk skulle jag ha lurat Kafka att tro att jag bearbetat events som aldrig faktiskt sparats. 

De eventen är borta för alltid. Genom att skriva till disk först och sedan bekräfta till Kafka garanterar jag att om något går fel, kör jag om från samma offset och skriver filen igen. Parquet-skrivning är idempotent, att skriva samma data två gånger är ofarligt. Att tappa data är inte det.
Det är samma tanke som låg bakom .upsert() i Glossary DB. Hellre en gång för mycket än en gång för lite.
```

## De tre viktiga koncepten att tänka på med consumer scriptet.

- Manuell offset-commit är det viktigaste designbeslutet i hela filen. `enable.auto.commit: False` betyder att Kafka aldrig automatiskt markerar ett meddelande som "hanterat". Det är jag som bestämmer det, och jag gör det efter att data är säkert på disk. Det är den viktigaste filosofin i arbete med data: **data till disk är alltid prio ett**

- Batch-flush med dubbelt villkor fungerar som en säkerhetsventil i två riktningar. `BATCH_SIZE=100` "skyddar" mot att batchen växer obegränsat under hög belastning. `BATCH_TIMEOUT_SEC=60` skyddar mot det omvända, att en batch aldrig skrivs för att communityt är lugnt och events droppar in långsamt. Utan timeout villkoret skulle data kunna sitta i minnet i timmar utan att nå disk.

- `KeyboardInterrupt` hanteringen är en detalj som separerar produktionskod från skol-kod. När jag trycker Ctrl+C för att stoppa consumern skulle en naiv implementation bara dö och tappa allt som samlats i batchen sedan senaste flush. Den här implementationen fångar signalen, skriver det som finns kvar, committar till Kafka, och stänger sedan rent. 
Det kallas för *graceful shutdown* och är vad jag förstått ett standardkrav i alla system som finns i produktion.