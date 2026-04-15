# Own docs regarding run_pipeline.py script (CLI commands using argparse)
---

## Notes och insikter kring run_pipeline scriptet.
Hela syftet med `run_pipeline.py` är att ha ett script med egna CLI kommandon för att enkelt kunna köra vilka script jag nu vill. Detta för att inte behöva memorera vad script heter, vilken pathing mina olika script har utan samla allting på en plats med liknande kommandon.

Ett exempel på hur hela pipelinen ska köras från bronze till silver till gold layers aggregeringar är något i stil med:  
`uv run python -m scripts.run_pipeline --layer all`

Varje funktion importerar och kör sitt respektive transform-script.
Jag importerar inuti funktionerna (lazy imports) istället för på toppnivå av en enkel anledning: om du bara kör --layer silver behöver Python aldrig ladda PySpark för gold-lagret. Det sparar uppstartstid och undviker onödiga beroenden när du bara vill köra ett specifikt lager.

**Viktiga mönster att förstå i transformationskedjan:**  

Det viktigaste i hela transformationskedjan är att allting körs i RÄTT ordning. Bronze måste **alltid** köras innan Silver, Silver innan Gold, det är medallion-arkitekturens grundregel.   
Alltid Bronze före silver, silver före gold. OM det inte finns ny raw data i bronze som hämtats hem från att jag hämtat hem events via githubs api, då kan jag börja på silver då Bronze tekniskt sett redan "gjort sin grej". `run_pipeline` scriptet säkerställer att denna ordning aldrig kan brytas av mänsklig miss.

**Viktigt mönster att förstå med Lazy imports:**  

Det viktigaste designbeslutet att förstå här är de `lazy imports` inuti varje funktion. När Python läser en fil och stöter på en import-sats på toppnivå, laddar den modulen direkt oavsett om den faktiskt behövs eller inte. 

Det betyder att om jag hade `from transforms.bronze_to_silver import run_bronze_to_silver` längst upp i filen skulle PySpark starta upp varje gång jag körde scriptet, även om jag bara ville kolla hjälptexten med `--help`.  
Genom att flytta importerna inuti funktionerna laddar Python bara det som faktiskt behövs för det valda lagret.

En vanlig import som nämns ovan. T.ex:  

`from transforms.bronze_to_silver import run_bronze_to_silver` högst upp i ett script hade tvingat min dator att ladda in HELA scriptet i arbetsminnet(RAM) direkt när jag kör scriptet.

Med Lazy imports (Fördröjd laddning) så innebär det att jag säger till Python att "Kom ihåg att det här scriptet finns MEN ladda inte in det förens jag faktiskt använder det i koden!"

---
## Varför används lazy imports?
*Det finns två "huvudsyften" till att använda lazy imports*

1) Snabba uppstart tider. När jag bygger något som behöver köras i terminalen (CLI tools) vill användaren(jag) ha instant respons när jag trycker på enter. Om koden/mitt script måste ladda in massiva librarys eller deps (Tänk något stort som TensorFlow eller PyTorch) direkt vid start så kommer scriptet att kännas **EXTREMT** trögt och lagga. Men tack vare lazy imports så laddas det tunga bara in om användaren(jag) faktiskt säger specifikt att funktionen kräver det.

2) Modulära paket och mindre onödiga beroenden *(dependencies(deps))*. När en bygger verktyg som andra användare eller utvecklare ska använda så har ett verktyg ofta flera olika funktioner. Om en användare bara vill använda en funktion av verktyget så är det inte rimligt att den användaren ska behöva hämta hem en massa deps för att endast en funktion ska användas. Genom att använda sig av lazy imports så kan man undgå det så gott det bara går och hålla andra borta från att behöva hämta hem fler tunga deps än vad som är nödvändigt.

**Lazy imports** är där en försöker optimera resursanvändning och UX(user experience) vilket är viktigare än man kan tänka sig.