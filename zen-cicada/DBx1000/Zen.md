
README
======

We use DBx1000 as the testbed to provide TPCC-NP queries, the backend is Zen-Cicada.

1. install the dependency required by DBx1000. Cityhash can be found [Here](https://github.com/google/cityhash).

2. enter the directory ../cicada-engine, then build the Zen integrated engine.

    cd build
    cmake .. -DZEN=ON
    make -j

after the make, libcommon.a should be found in ../cicada-engine/build.

3. enter the directory ../silo, then compile the code as dependency.

    cd ../silo/
    make -j 

4. enter the directory ./DBx1000, build the excutable testbed according the configuration in config-std.h.

    cp config-std.h config.h
    make -j

after the make, rundb should be found in ./DBx1000.

the configuration in config.h can be modification following the ./DBx1000/README.

