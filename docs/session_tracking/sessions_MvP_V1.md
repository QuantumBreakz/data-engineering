# Session tracking notes for MvP v1
**COMPLETED**

**Monday 16/03/2026**  
*Goals for today:*
- Setup entire repo structure
    - **Done**
- Add all needed deps for MvP 1
    - **Done**
    
**Saturday 21/03/2026**  
*Goals for today:*
- Write config.py in repo
    - **Done**

- Write kafka_producer.py in repo
    - **Done**

- Write kafka_consumer.py in repo
    - **Done**

- Write small docs for todays session (config, kafka_producer and kafka_consumer)
    - **Done**


**Sunday 29/03/2026**  
*Goals for today:*
- Added visuals in form of .mmd diagrams to show flow with KRaft instead of Zookeeper for kafka
    - **Done**

- Configure Dockerfile + docker-compose.yml file
    - **Done**

- Fetch my first events from github
    - **Done**

- Removed keyword filter in producer to see if pipeline was working. It is!
- (3 .parquet files to disk. Hivestyle partitioning works as intended)
    - **Done**

**BRONZE TO SILVER**
- Write `bronze_to_silver.py` script.
    - **Done**

- Write what `bronze_to_silver.py` script does in docs/file_docs/bronze_to_silver.md
    - **Done**

**Testing**
- Write unit tests for `_is_valid()` and `_flatten()` functions in bronze_to_silver.py script.
    - **Done**
    
**CI-PIPE**
- Add more to my github actions CI pipeline before committing `bronze_to_silver.py`-script.
    - **Done**

**Monday 30/03-2026**  
*Goals for today(birthday, yay!):*
- Idempotency fix in `bronze_to_silver.py`. Last thing open on ROADMAP.md for MvP V1.
    - **Done**
        - Identified which day partitions the silver output would touch. I read created_at from the flattened DataFrame and collect unique days. **THESE ARE THE ONES AND ONLY THE ONES I CLEAR**. Used `import shitil`and `shutil.rmtree(partition)`to achieve this.

- README.md structure for MvP v1
    - Ongoing