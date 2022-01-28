
README
---

1. configure the directory of mounted Intel Optane Memory

    change line 9 of ./dbx1000/system/nvm\_memory.h
    change line 62 of ./dbx1000/collect\_data.py

2. execute and collect the result.

    python ./collect\_data.py

3. notes

* collect\_data.py changes the CC\_ALG for different concurrency control methods. It compiles and executes the source code 5 times and report the average performance. The result is written in results.csv. 
* Please use the paramethers defined in collect\_data.py and config.h. The program may fail to execute if you change the parameters.   

