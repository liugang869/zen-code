
## 此脚本用于批量运行test_tx测试程序

import os,re,datetime,sys,time

curr_path = os.path.abspath(__file__)
curr_dirs = os.path.dirname(curr_path)
work_dirs = curr_dirs

# mode = ['MMDB','WBL','ZEN']

def cmake_with_param (mode):
    print ('[消息]：cmake ', mode)
    os.chdir (work_dirs)
    cmake_command = 'cmake .. -D'+mode+'=ON'
    os.system (cmake_command)
    if os.path.exists (work_dirs+'/'+'Makefile'):
       return True
    else:
       print ('[错误]：cmake错误，Makefile不存在')
       return False

def remove_cmake ():
    print ('[消息]：remove cmake ')
    os.chdir (work_dirs)
    os.system ('rm -r CMake*')
    os.system ('rm -r cmake*')
    os.system ('rm Makefile')
    if not os.path.exists (work_dirs+'/'+'Makefile'):
       return True
    else:
       print ('[错误]：remove cmake错误，Makefile存在')
       return False

def make_with_no_param ():
    print ('[消息]：make ')
    os.chdir (work_dirs)
    make_command = 'make -j20'
    os.system (make_command)
    if os.path.exists (work_dirs+'/'+'test_tx'):
       return True
    else:
       print ('[错误]：make错误，test_tx不存在')
       return False

def remove_make ():
    print ('[消息]：remove make ')
    os.chdir (work_dirs)
    make_command = 'make clean'
    os.system (make_command)
    if not os.path.exists (work_dirs+'/'+'test_tx'):
       return True
    else:
       print ('[错误]：make clean错误，test_tx存在')
       return False

# '10000000' '16' '0.5' '0.99' '200000' '10' ['mmdb','wbl',zen]  'result'
def run_with_param (num_rows, req_per_tx, read_ratio, zipf_theta, tx_count, thread_cnt, prefix_mode, relative_result_dir):
    os.chdir (work_dirs)
 
    p1 = 'numsrows@'+str(num_rows)
    p2 = 'reqpertx@'+str(req_per_tx)
    p3 = 'readratio@'+str(read_ratio)
    p4 = 'zipftheta@'+str(zipf_theta)
    p5 = 'txcount@'+str(tx_count)
    p6 = 'threadcnt@'+str(thread_cnt)
    params = [p1, p2, p3, p4, p5, p6]
    result_file_name = prefix_mode+'_'+'_'.join(params)

    result_file_dirs = curr_dirs+'/'+relative_result_dir
    if not os.path.exists (result_file_dirs):
        os.system ('mkdir '+result_file_dirs)

    result_file_path = result_file_dirs+'/'+result_file_name
    if os.path.exists (result_file_path):
        print ('[消息]：文件已存在，跳过运行！', result_file_name)
        return 

    run_params = [str(num_rows),str(req_per_tx),str(read_ratio),str(zipf_theta),str(tx_count),str(int(thread_cnt))]
    run_command = './test_tx '+' '.join(run_params)+' > '+result_file_path
    print ('[消息]：运行命令 %s' % run_command)
    try:
        os.system (run_command)
    except Exception as e:
        print ('[错误]：test_tx程序运行错误！')
        print (e)
    pass

# 定义要运行的测试
def test_frame_1 (prefix_mode_input, relative_result_dir_input):
    prefix_mode = prefix_mode_input
    relative_result_dir = relative_result_dir_input

    if prefix_mode == 'mmdb':
        mode = 'MMDB'
    elif prefix_mode == 'wbl':
        mode = 'WBL'
    elif prefix_mode == 'zen':
        mode = 'ZEN'
    else:
        print ('[错误]：test_frame输入参数错误！')
        return 

    cmake_with_param (mode)
    make_with_no_param ()

    rank = 1;
    array_num_rows = [10000000]
    array_req_per_tx = [16]
    array_read_tatio = [1.0]
    array_zipf_theta = [0.0,0.6,0.95]
    array_tx_count = [200000]
    array_thread_cnt = [16]

    total = len(array_num_rows)*len(array_req_per_tx)*len(array_read_tatio)*len(array_zipf_theta)*len(array_tx_count)*len(array_thread_cnt)
    for num_rows in array_num_rows:
        for req_per_tx in array_req_per_tx:
            for read_ratio in array_read_tatio:
                for zipf_theta in array_zipf_theta:
                    for tx_count in array_tx_count:
                        for thread_cnt in array_thread_cnt:
                            starttime = datetime.datetime.now()
                            # while starttime.hour < 23 and starttime.hour > 6:
                            #     time.sleep(60)
                            #     starttime = datetime.datetime.now()
                            # if starttime.hour > 22:
                            #    return
                            print ('[%d/%d] 开始时间：%s'%(rank,total,starttime))
                            run_with_param (num_rows,req_per_tx,read_ratio,zipf_theta,tx_count,thread_cnt,prefix_mode,relative_result_dir)
                            endtime = datetime.datetime.now()
                            usetime = (endtime - starttime).seconds
                            print ('[%d/%d] 结束时间时间：%s 花费时间：%d 秒'%(rank,total,endtime,usetime))
                            rank += 1
        remove_make ()
    remove_cmake ()
    pass

def test_1 (prefix_mode='mmdb|wbl|zen', relative_result_dir='result/Mar1'):
    run_param = prefix_mode.split('|')
    rank = 1
    for param in run_param:
        print ('[当前组数：%d/实验组数：%d]：%s'%(rank,len(run_param),param))
        test_frame_1 (param, relative_result_dir)
        rank += 1
    pass

if __name__ == '__main__':
    print ('[消息]: 当前路径:', curr_path)
    print ('[消息]: 当前目录:', curr_dirs)
    print ('[消息]: 工作目录:', work_dirs)
    test_1 ()
    pass

