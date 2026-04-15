# Own docs for test_transforms.py script

## Insikter och tankar kring testing och varför.
Unit testing är oerhört värdefullt. Syftet är att testa enskilda funktioner isolerat utan att behöva bry mig om resten av systemet. Liknelse: Testar en kugge på ett kugghjul som ska monteras in i ett urverk för att se till att det fungerar korrekt innan jag monterar ihop hela klockan.

De två funktioner jag vill testa först och främst är:

1) `_is_valid()`-funktionen ur `bronze_to_silver.py`

2) `_flatten()`-funktionen ur `bronze_to_silver.py`

- Anledningen till det är att de funktionerna är "hjärtat" i hela transformationen samt att de är perfekta att enhetstesta. Dom tar en dict in och returnerar ett resultat ut. Det finns inga externa beroenden, inget som har med Kafka att göra, ingenting som har med disk att göra heller. Utan de är "rena" och förutsägbara.

- Det som ska testas är både **Happy path(Det som SKA fungera)** och **Sad path(Det som SKA misslyckas)**

- Det mest "värdefulla" testet är bland annat det sista `test_flatten_returns_all_expected_keys`.
    - Varför?
        - Det fungerar som ett schema kontrakt. Om jag om ett par veckor lägger till en ny comlumn i `_flatten()`, exempel language. MEN jag glömmer att uppdatera gold layers förväntningar så kommer jag ha en tyst bug(silent bug) som inte kraschar utan enbart producerar **FEL DATA**. Det testet gör den ändringen **OMÖJLIG** att missa då min CI pipe kommer skrika att mitt förväntade schema inte längre stämmer!