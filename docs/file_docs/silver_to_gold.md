# Own docs for silver_to_gold.py script


## Notes om funktionerna i silver_to_gold scriptet
`def _write_gold`:  
Rensar befintlig Gold-tabell och skriver ny version.  
Gold-tabeller är till skillnad från Silver inte partitionerade per dag, de är aggregerade över hela datamängden och skrivs som en enda tabell. Därför rensar jag hela output_dir istället för enskilda dags partitioner.

`def build_tool_growth`:  
Denna funktion är till för att svara på en fråga: 
Vilka DE-verktyg växer snabbast mätt i stars per vecka? Jag filtrerar på `WatchEvent` (som är en star på GitHub) och `ForkEvent`, jag grupperar per repo och vecka, och räknar antalet händelser. `date_trunc("week", ...)` rundar sen ner ett datum till Måndagen i samma vecka, vilket i sin tur gör att alla dagar i samma vecka får samma "vecko nyckel" vilket är precis vad jag vill ha för en veckovis aggregering.

`def build_activity_heatmap`:  
Denna funktion är till för att svara på frågan NÄR Data Engineer/engineering communityt är aktivt på Github. 

Jag extraherar timme (0-23) OCH veckodag (1=Söndag, 2=Måndag ... ... 7=Lördag i Spark) från `created_at`och räknar totalt antal events per kombination. Resultatet SKA vara en 7x24 matrix, en heatmap som visar vilka timmar OCH dagar som är mest aktiva. Det är den tabellen som sen kommer ge mig det visuellt intressanta mönstret när jag börjar jobba med att implementera Grafana vid senare MvP.


`def build_pr_cycle_times`:
Denna funktion är till för att svara på HUR lång en typisk PR cykel är i DE-repos. Det är en komplex aggregering, en self join. Silver har separata rader för opened och closed PRs. Jag behöver para ihop dom för att kunna beräkna tidsskillnaden.

**Self join i praktiken:**  
        `df_opened:  alla rader där pr_action = "opened"`  
        `df_closed:  alla rader där pr_action = "closed"`  
        `JOIN på:    repo_name (samma repo)`

När jag joinar på bara `repo_name` så får jag **ALLA** kombinationer av opened+closed inom samma repo. Vilket är alldeles för brett.. Jag behöver också matcha på PR-nummer, men PR numret finns inte explicit i mitt silver-schema då jag inte sparade `payload.number` i `_flatten` funktionen när den fanns.

---

**Förklaring om pr_cycle_times funktionen:**   
En vanlig join kombinerar två olika tabeller. En self join kombinerar en tabell med sig själv, men behandlar den som om den vore två separata tabeller med olika alias. 

Tänk på det som att du har en lista med händelser och vill para ihop varje "start" händelse med dess motsvarande "slut" händelse. Listan är densamma, men du tittar på den från två vinklar samtidigt.

**För PR-cykler ser det ut så här i praktiken:** 

Silver layer har rader som `repo=apache/airflow`, `pr_action=opened`, `created_at=måndag 10:00` och `repo=apache/airflow`, `pr_action=closed`, `created_at=tisdag 14:00`. De är separata rader utan någon explicit koppling. 

Self joinen skapar kopplingen: "hitta alla par där repo är samma, opened matchas med closed, och beräkna sedan tidsskillnaden." Resultatet blir en ny rad: `repo=apache/airflow, cycle_hours=28.0.`


# Från cartesian joins och M x N till något som fungerar.

**Min första lösning för min MvP var alldeles för godtrogen och NAIV:**  
Jag antog att jag kunde acceptera att matchningen är "ungefärlig" och filtrerar på att `closed_time > opened_time` samt att tidsskillnaden är rimlig (< 30 dagar). Jag *TRODDE* det skulle ge mig en ganska så bra uppskattning av cykeltider utan att jag behöver göra om och porta om mitt silver-schema. Jag får ta och notera det som en känd begränsning i min ROADMAP.md var tanken. Men problem uppstod direkt pga blåögdhet.

**Vad jag lärde mig nu när jag sitter och faktoriserar min egen kod klockan 22:50 en Lördagskväll:**  
- **Genvägar i scheman straffar dig ALLTID!**. Om jag saknar en länk mellan två händelser (Ett unikt ID i detta fall) så ska jag ALDRIG försöka gissa mig till den via timestamps eller namn.. Det är bättre att jag *direkt* går tillbaka till the source(Bronze) och hämtar mitt jäkla ID.

- **Cardinality is KING:** Innan jag ens tänker på att göra en Join i PySpark bör jag fråga mig själv.. 'Hur unik är nyckeln jag joinar på?'. Att jag joinar på `user_id` är bra, dvs hög kardinalitet. Att joina på någonting som t.ex `country` eller `region` eller `repo_name` är att be om en krasch om det inte följs av ett unikt id, dvs låg kardinalitet. Det kanske fungerar för tillfället men det är som att be om att någonting ska gå sönder vid mer stress/belastning. 

- Det fick jag själv uppleva. 900k events, you do the math (Jag gör den åt dig. Två tables med 900k events var eller en table med 900k events i sig själv inneär en nuke med antal rows. För att vara mer specifik. *900 000 x 900 000* som är **810 MILJARDER RADER!!!**)
--- 