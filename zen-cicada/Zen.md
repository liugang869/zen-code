
README
---

1. install the dependency following ./cicada-engine/Cicada.md

2. enter the directory ./cicada-engine/build

3. configure the directory of mounted Intel Optane Memory

    change the line 55-56 of cicada-engine/src/mica/alloc/hugetlbfs_shm.h following the annotation in the code  

4. run the following command to build Zen engine

    cmake ..
    make -j

5. run the following command to test Zen engine by using YCSB

    ./test_ycsb number\_of_tuples<e.g. 100000000> request\_per\_transaction<e.g. 10> read\_ratio<e.g. 0.9> ziphian\_theta<e.g. 0.9> transactions\_per\_thread<e.g. 16> number\_of_\thread<e.g. 16>

6. run the following command to test Zen engine by using full TPCC

    ./test_tpcc num\_of\_warehouse<e.g. 64> num\_of\_thread<e.g. 16> number\_of\_transaction\_per\_thread<e.g. 20000> number\_of\_transaction\_per\_thread\_for_warmup<e.g. 10000>

