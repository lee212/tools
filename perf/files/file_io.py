import glob, os
import time
import logging
from multiprocessing.pool import Pool

jobid=os.environ['SLURM_JOBID']
logging.basicConfig(filename="file_io_test_parallel24.log", level=logging.INFO)
fname='/oasis/scratch/comet/hrlee/temp_project/alchemlyb/perf/fsizes/32/0003/WORK/dhdl/dhdl.part0001.xvg'

# timeout
# fsize
# concurrency

with open(fname) as f:
    content = f.read()

def fread(fname,seconds, idx=0):
    with open(fname) as f:
        s0 = time.time()
        d = f.read()
        s1 = time.time()
        msg=("read,{},{},{},{},{}".format(fname, idx, seconds, s0, s1))
        logging.info(msg)

def fwrite(fname, seconds, idx=0):
    global content
    with open(fname, 'w') as f:
        s0 = time.time()
        d = f.write(content)
        s1 = time.time()
        msg=("write,{},{},{},{},{}".format(fname, idx, seconds, s0, s1))
        logging.info(msg)


def single_run():
    for i in range(20):
        seconds = 2**i
        fread(fname, seconds)
        time.sleep(seconds)

def fread_p(*args):
    fname, seconds, idx = args[0]
    fread(fname, seconds, idx)

def fwrite_p(*args):
    fname, seconds, idx = args[0]
    fwrite(fname, seconds, idx)

def get_new_fname(fname_idx=0):

    global jobid
    fname_idx += 10
    didx, fidx = divmod(fname_idx, 10)
    base='/oasis/scratch/comet/hrlee/temp_project/alchemlyb/perf/fsizes/32'
    # Local
    base='/scratch/hrlee/{}/temp_project/alchemlyb/perf/fsizes/32'.format(jobid)
    fname = "{}/{:04d}/WORK/dhdl/dhdl.part{:04d}.xvg".format(base, didx, fidx)
    fname = "{}/{:04d}_dhdl.part{:04d}.xvg".format(base, didx, fidx)
    return fname

def multi_run(ftype, f_count):
    if ftype == "write":
        func = fwrite_p
    else:
        func = fread_p
    p = Pool(48)
    for i in range(1):
        seconds = 2**i
        args = [[get_new_fname(idx), seconds, idx] for idx in range(f_count)]
        #list(p.map(freadp, args))
        list(p.map(func, args))
        time.sleep(seconds)

def update_logger_fname(fname):

    log = logging.getLogger()  # root logger
    for hdlr in log.handlers[:]:  # remove all old handlers
        log.removeHandler(hdlr)
    fp = logging.FileHandler(fname, 'a')
    log.addHandler(fp)      # set the new handler

def cleanup():
    for fname in glob.glob('temp_project/alchemlyb/perf/fsizes/32/*'):
        os.remove(fname)

if __name__ == "__main__":

    for i in range(100, 1100,100):
        update_logger_fname('local_scratch_48_multi32MB_{}.log'.format(i))
        cleanup()
        multi_run("write", i)#files
        multi_run("read", i)#files
