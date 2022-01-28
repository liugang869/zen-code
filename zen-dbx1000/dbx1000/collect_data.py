import os
import csv

all_cc = ['WAIT_DIE', 'NO_WAIT', 'DL_DETECT', 'MVCC', 'HEKATON', 'HSTORE', 'OCC', 'TICTOC', 'SILO']
csv_file = open('results-e.csv', 'w')
writer = csv.writer(csv_file)

#modify the configure file
for cc_alg in all_cc:
    print ('method: E cc_alg:%s' % cc_alg)

    data = ''
    with open('config.h', 'r') as f:
        for line in f.readlines():
            if (line.find('CC_ALG') != -1):
                line = '#define CC_ALG \t\t\t\t\t' + cc_alg + '\n'
            data += line

    with open('config.h', 'w') as f:
        f.writelines(data)

    #compile
    os.system('make -j20')

    #rundb and collect data
    result = [0, 0, 0]
    for i in range(3):
        f = os.popen('./rundb')

        sim_time = ''
        sim_time_n = 0
        txn_cnt = ''
        txn_cnt_n = 0

        for line in f.readlines():
            if (line.find('SimTime') != -1):
                for c in line[16:]:
                    if (c != '\n'):
                        sim_time += c
                #print(sim_time)
                sim_time_n = int(sim_time)
                #print(sim_time_n)
            if (line.find('summary') != -1):
                for c in line[18:]:
                    if(c != ','):
                        txn_cnt += c
                    else: 
                        break
                #print(txn_cnt)
                txn_cnt_n = int(txn_cnt)
                #print(txn_cnt_n)
        result[i] = txn_cnt_n*1000000000/sim_time_n
        #print (result[i])

    #compute
    avg_result = sum(result)/len(result)
    print(avg_result)
    writer.writerow([cc_alg, str(avg_result)])

    os.system('make clean')

os.system('rm /mnt/mypmem0/chenleying/dbx*')
