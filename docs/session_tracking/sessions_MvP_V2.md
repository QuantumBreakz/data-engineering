# Session tracking notes for MvP v2
**DONE**


**Monday 30/03-2026** 
*Goals for today:*
- Add more docs + visuals for `bootstrap_historical.py`
    - **Done**

- Add visuals to better understand purpose of `bootstrap_historical.py`
    - **Done**


**Tuesday 31/03-2026**
*Goals for today:*
- Write `bootstrap_historical.py` script to start getting historical data as parquet files.
    - **Done**

- Write more docs for better understanding of `bootstrap_historical.py` script
    - **Done** NOTE: STILL NEEDS MORE TO ADD REGARDING CODE!

- Fetch more historical data.
    - **Done**

**Saturday 04/04-2026**
*Goals for today:*
- Figure out where the bug is that makes it so I dont get `commit_counts`
    - **Done**

- Laugh/cry/facepalm over entire night with debugging and insanity because `commit_counts` is not even relevant for my analysis.
    - **Done** **Done** **Done** **Done**
    


**Sunday 05/04-2026**
*Goals for today:*
- Port over to PySpark from Pandas and read up on docs
    - **Done**


**Friday 10/04-2026**
*Goals for today:*
- Write mermaid diagram over `silver_to_gold.py` to better understand upcoming script and have a visual understanding of it
    - **Done**

- Branch out to `silver_to_gold.py` and start writing silver to gold aggregation script
    - **Done**
    
- Write `silver_to_gold.py` docs to better understand PySparks strenghts and weaknesses compared to Pandas
    - **Done**

**Saturday 11/04-2026**
*Goals for today:*
- Branch out to `silver_to_gold.py` and start writing silver to gold aggregation script
    - **Done**
    
- Write `silver_to_gold.py` docs to better understand PySparks strenghts and weaknesses compared to Pandas
    - **Done**

- Refactor `silver_to_gold.py` script since i stumbled upon the infamous "Cartesian join".
    - **DONE!!!!!!**
    - **Done with rite of passage fumble every aspiring Data Engineer stumbles upon**

- Write more docs regarding silver to gold script explaining my error and naivety relying upon low cardinality assuming it would work.
    - **Done**

- Refactor `bronze_to_silver.py` script to include `pr_number` in the flattening.
    - **Done**

- Do EDA(exploratory Data Analysis on my own fetched data)!
    - **Done**

**Sunday 12/04-2026**
*Goals for today:*
- Write my own CLI commands for all layers in pipeline. Bronze to Gold aggregation. `run_pipeline.py`
    - **Done**

- Write own docs regarding `lazy imports` to better understand the purpose of them in `run_pipeline.py` script
    - **Done**

- Start reading up on theory regarding incremental reading to prepare for tomorrows script writing session.
    - **Done + ongoing**

**Wednesday 15/04-2026**
*Goals for today:*
- Write own docs from Sundays theory session regarding incremental reading.
    - **Done**
    
- Add Incremental reading as noted in ROADMAP.md
    - **Done**

- Containerize PySpark for ease of use.
    - Ongoing

