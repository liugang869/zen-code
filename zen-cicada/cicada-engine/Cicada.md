
Cicada SIGMOD 2017 evaluation
=============================

Hardware requirements
---------------------

 * Dual-socket Intel CPU >= Haswell
   * Interleaved CPU core ID mapping (even numbered cores on CPU 0, odd numbered cores on CPU 1)
   * Turbo Boost disabled for more accurate core scalability measurement
   * Hyperthreading enabled (though experiments do not use it directly)
 * DRAM >= 128 GiB
   * Ensure to use all memory channels (while keeping the maximum frequency) for full bandwidth
 * Disk space >= 15 GB
   * SSD recommended

Base OS
-------

 * Ubuntu 14.04 LTS amd64 server

Installing packages
-------------------

	# for g++-5
	sudo apt-get update
	sudo apt-get install -y software-properties-common
	sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test

	# common
	sudo apt-get update
	sudo apt-get install -y build-essential cmake git g++-5 libjemalloc-dev libnuma-dev
	# for Silo
	sudo apt-get install -y libdb6.0++-dev
	# for FOEDUS/MOCC
	sudo apt-get install -y libgoogle-perftools-dev papi-tools

	# for experiments
	sudo apt-get install -y psmisc python3

	# for analysis
	sudo apt-get install -y python3-pip
	sudo apt-get install -y texlive-generic-recommended texlive-latex-recommended texlive-latex-extra texlive-fonts-recommended texlive-fonts-extra dvipng
	pip3 install --user 'pandas>=0.20,<0.21' 'pandasql>=0.7,<0.8' 'matplotlib>=1.5,<2.0'

 * Estimated time: 1 hour

Configuring system
------------------

	# for non-interactive experiment execution
	echo "`whoami` ALL=(ALL:ALL) NOPASSWD:ALL" | sudo tee -a /etc/sudoers

	# for third-party engines
	echo "`whoami` - memlock unlimited" | sudo tee -a /etc/security/limits.conf
	echo "`whoami` - nofile 655360" | sudo tee -a /etc/security/limits.conf
	echo "`whoami` - nproc 655360" | sudo tee -a /etc/security/limits.conf
	echo "`whoami` - rtprio 99" | sudo tee -a /etc/security/limits.conf

	sudo groupadd hugeshm
	sudo usermod -a -G hugeshm `whoami`

	sudo reboot

 * Estimated time: 15 minutes

Downloading source code
-----------------------

	git clone --recursive https://github.com/efficient/cicada-exp-sigmod2017.git

 * Estimated time: 1 minute

Building all engines
--------------------

	cd cicada-exp-sigmod2017
	./build_cicada.sh
	./build_ermia.sh
	./build_foedus.sh
	./build_silo.sh

 * Estimated time: 15 minutes

Running all experiments
-----------------------

	EXPNAME=MYEXP
	./run_exp.py exp_data_$EXPNAME run

 * Estimated time: 3 days
 * Experiment output files are created in exp\_data\_$EXPNAME
 * Each run automatically rebuilds DBx1000 (and cicada-engine for some experiments) to apply system/benchmark parameters

Analyzing experiment output
---------------------------

	cd result_analysis
	./analyze.sh ../exp_data_$EXPNAME

 * Estimated time: 15 minutes
 * Analysis output files are created under result\_analysis/output\_$EXPNAME

Analysis output
---------------

	Figure 3
	 (a) output_macrobench_tpcc-full_warehouse_1.pdf
	 (b) output_macrobench_tpcc-full_warehouse_4.pdf
	 (c) output_macrobench_tpcc-full_fixed_ratio.pdf

	Figure 4
	 (a) output_macrobench_tpcc-full_warehouse_1_siu.pdf
	 (b) output_macrobench_tpcc-full_warehouse_4_siu.pdf
	 (c) output_macrobench_tpcc-full_fixed_ratio_siu.pdf

	Figure 5
	 (a) output_macrobench_tpcc_warehouse_1.pdf
	 (b) output_macrobench_tpcc_warehouse_4.pdf
	 (c) output_macrobench_tpcc_fixed_ratio.pdf

	Figure 6
	 (a) output_macrobench_ycsb_record_100_req_16_read_0_50_zipf_0_99.pdf
	 (b) output_macrobench_ycsb_record_100_req_16_read_0_50_thread_28.pdf
	 (c) output_macrobench_ycsb_record_100_req_16_read_0_95_thread_28.pdf

	Figure 7
	 output_macrobench_ycsb_record_100_req_1_read_0_95_zipf_0_99.pdf

	Figure 8
	 output_inlining_ycsb_req_16_read_0_95_zipf_0_00_thread_28.pdf

	Table 2
	 output_query.txt: "factor-ycsb-contended-tx"

	Figure 9
	 output_gc_tpcc-full.pdf

	Figure 10
	 output_backoff_tpcc-full_warehouse_4.pdf
	 output_backoff_tpcc_warehouse_4.pdf
	 output_backoff_ycsb_req_1_read_0_50_zipf_0_99.pdf

	Figure 11
	 output_macrobench_ycsb_record_100_req_1_read_0_50_zipf_0_99.pdf
	 output_macrobench_ycsb_record_100_req_1_read_0_50_thread_28.pdf
	 output_macrobench_ycsb_record_100_req_1_read_0_95_thread_28.pdf

Authors
-------

Hyeontaek Lim (hl@cs.cmu.edu)

License
-------

        Copyright 2014, 2015, 2016, 2017 Carnegie Mellon University

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and

