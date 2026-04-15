# Own docs for bootstrap_historical.py script

Min förståelse av vad `bootstrap_historical.py` är för något och varför det scriptet spelar roll.

Fram tills nu har jag "fiskat i ett extremt stort hav med en **väldigt** liten håv". Ett event här och ett event där beroende på vad Github råkar producera just när min producer vaknar till liv var 120sek. Det har i skrivandes stund gett mig 82 silver records på två kvällar(Söndag 29/03 och måndag 30/03). 

Det är tillräckligt av ett Proof of Concept för att visa att pipen fungerar med producer-kafka-consumer och Githubs API. **MEN** det är långt ifrån tillräckligt för att mitt val av `PySpark` ska vara meningsfullt att använda, eller för att gold layers aggregeringar ska ge något typ av intressanta svar.

**Lösningen på det?**
- Lösningen heter 'Github Archive'
    - Sen 2011 har någon(galning?) spelat in varje publikt Github-event och lagrat dom som komprimerade JSON filer, en fil per timme... Den datan finns tillgänglig på `https://data.gharchive.org/`

En timmes fil ifrån ett aktivt år *kan* innehålla hundratusentals events. Det är härifrån jag kommer kunna rättfärdiga min användning av deps så som PySpark. Jag behöver stora mängder med data för att det ens ska vara värt det.

## Points worth diving deeper into and know.

- NDJSON(Newline Delimited JSON) och JSONL(JSON Lines) är funktionellt identiska dataformat där varje rad är sitt eget JSON objekt. JSONL/NDJSON är kung jämfört med rena JSON filer där hela filen är ett objekt. Stora dataset och loggfiler är där JSONL/NDJSON skiner att parsa vs JSON.

- NDJSON är anledningen till att jag kan parsa Github archive filer effektivt. Istället för ett stort JSON objekt som måste laddas i minnet *I SIN HELHET* är varje rad ett eget självständigt JSON objekt. Det innebär att jag kan läsa en rad, parsa den, filtrera den och gå vidare till nästa utan att hålla **hela** filens innehåll i minnet samtidigt(OOM issues ganska så snabbt). Det är ett vanligt designmönster som ofta stöts på när det kommer till större datamängder.

- `bootstrap-` prefix på filnamn är ett medvetet val och medveten detalj. `consumer`skriver filer som `part-TIMESTAMP.parquet` och bootstrap skriver `bootstrap-TIMESTAMP.parquet`. Silver transformationen bryr sig *ej* om namnprefixet utan den läser alla `.parquet` den kan hitta. Men om jag någonsin behöver debugga och förstå **varifrån** ett specifikt event kom ifrån kan jag enkelt om inte omedelbart se om det är live data ifrån kafka eller historisk data ifrån Github archive. Det är *inbyggd spårbarhet* i namnet.

- `argparse` upplägget med tre körlägen är det samma mönster jag är van vid, t.ex `pipeline_runner.py` ifrån tidigare projekt eller som jag kommer använda i `run_pipeline.py`. Konsistens i hur mina scripts exponerar sin interface är en form av "developer experience". Nästa gång Jag eller någon annan ser ett script i det här repot vet dom exakt hur de ska interagera med det.