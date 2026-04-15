# Own docs for producer.py script

- Producer.py have THREE responsibilitys, they are:
    - Poll github events API 
    - Filter away events I dont care about(see config.py) 
    - Send relevant events to Kafka 

*Nothing more and nothing less*

## De viktiga koncepten att förstå

**`producer.poll(0)` vs `producer.flush()`** - det här är det svåraste att förstå i confluent-kafka. Tänk på det så här:
```
produce()     -> Lägger ett brev i din brevlåda
poll(0)       ->  Postbudet tittar i brevlådan en gång (utan att vänta)
flush()       ->  Postbudet sitter kvar tills ALLA brev är hämtade
```

Jag kör `poll(0)` inne i loopen för att ge Kafka chansen att skicka löpande. Du kör `flush()` efter loopen för att garantera att ingenting är kvar i kön innan du sover i 5 minuter.

`seen_ids: set[str]` - GitHub Events API är inte en stream, det är en snapshot. Varje gång du pollar får du de 300 senaste events. Om communityt är lugnt kanske bara 5 nya events har dykt upp sedan förra poll - de andra 295 har du redan sett. Utan `seen_ids` skickar du dem till Kafka igen och igen.

`_is_de_relevant()` i producern - vi diskuterade detta i teorin: filtrera tidigt. Det finns en medveten designbeslut här som är värd att förstå. Bronze ska inte vara en soptipp av allt GitHub producerar - det ska vara rådata vi valt att samla in. Det är skillnaden mellan att spara en hel sjö och att samla regnvatten i en tunna.


## I data lake projektet jobbar producern så här jämfört med dataplatform development labben
```
Producer = HÄMTAR och VIDAREBEFODRAR datan.
Github API -> Producer -> Kafka
Datan existerar redan. Producer agerar "brevbärare"
```
**Liknelse:** 
```
Tänk en tidningsredaktion. I Dataplatform labben var Producer författare då jag använde den för att generera syntetisk data med faker.

Här är det annorlunda. I det här projektet är producern som en prenumration på en tidning. Reuters skriver artiklarna(Github). Min producer hämtar dom och lägger dom i redaktionens inkorg(Kafka) för att resten av systemet ska bearbeta dom. Producern bryr sig inte alls om vad som händer med alla events sen. Den har endast ett ansvar:
1: Polla api.github.com/events var femte minut.
2: Skicka varje event vidare till Kafka Topic.
3: Thats it!
```

## Visual explanation on where producer works and the processes it handles
```
GitHub API          Producer               Kafka Topic
    │                   │                       │
    │    (sover)        │                       │
    │                   │                       │
    │◄── GET /events ───│  (vaknar var 5:e min) │
    │─── 300 events ───►│                       │
    │                   │── produce(event) ────►│
    │                   │── produce(event) ────►│
    │                   │── produce(event) ────►│  (alla 300)
    │                   │                       │
    │                   │  (sover igen)         │
    │                   │                       │
```