from __future__ import with_statement
import os
import sys
import numpy as np

def make_dir_name(root):
    return root.replace('.','_')+'_shared'


def make_cmd(root, start_dex, end_dex, start_date, duration):
    return 'srun -n 1 --exclusive --mem-per-cpu=3000 ' \
           + 'python $NEODIR/gen_ssm_db/gen_coeff_flexible_recursive.py ' \
           + '$SCRATCH/neoFullInputFiles/%s ' % (root) \
           + '%.1f 30 14 %d ' % (float(start_date), duration) \
           + '$SCRATCH/%s %d %d' % (make_dir_name(root), start_dex, end_dex)

def make_header(handle):

    handle.write("#!/bin/bash -l\n\n")

    handle.write("#SBATCH -p shared\n")
    handle.write("#SBATCH -n 1\n")
    handle.write("#SBATCH -t 6:00:00\n\n")
    handle.write("#SBATCH -A m1727\n\n")

    handle.write("echo 'starting now'\n")
    handle.write("date\n\n")

    handle.write("module load python/2.7-anaconda\n\n")

    handle.write("export NEODIR=$HOME/neo_generation\n")
    handle.write("export PATH=$PATH:$NEODIR/oorb/main\n")
    handle.write("export LD_LIBRARY_PATH=$NEODIR/oorb/lib:$LD_LIBRARY_PATH\n")
    handle.write("export DYLD_LIBRARY_PATH=$NEODIR/oorb/lib:$DYLD_LIBRARY_PATH\n")
    handle.write("export PYTHONPATH=$NEODIR/oorb/python:$PYTHONPATH\n")
    handle.write("export OORB_DATA=$NEODIR/oorb/data\n")
    handle.write("export OORB_CONF=$NEODIR/gen_ssm_db/config/oorb.conf\n")
    handle.write("export PYTHONPATH=$NEODIR/gen_ssm_db:$PYTHONPATH\n\n")


def write_conclusion(handle, data_root, start_dex, end_dex):
    dir = make_dir_name(data_root)
    handle.write("\ncd $SCRATCH/%s\n" % dir)
    handle.write("\ndeclare -i goahead\n")
    handle.write("goahead=1\n")
    handle.write("for (( ii=59580; ii<63233; ii+=30 ));\n")
    handle.write("do\n")
    handle.write("    if [ ! -e %s_%d_%d_$ii\.done\.txt ]; then\n"
                 % (data_root, start_dex, end_dex))
    handle.write("        goahead=0\n")
    handle.write("    fi\n")
    handle.write("done\n")
    handle.write("\n")
    handle.write("if [ $goahead == 1 ]; then\n")

    handle.write("    tar -cf objects_%d_%d.tar *%d_%d* --remove-files\n"
                 % (start_dex, end_dex, start_dex, end_dex))
    handle.write("    gzip objects_%d_%d.tar\n" % (start_dex, end_dex))
    handle.write("fi\n")


if __name__ == "__main__":

    need_file = sys.argv[1]
    data_root = need_file.replace('_needs.txt', '')
    pickup_dir = data_root.replace('.','_')+'_pickups'

    if not os.path.exists(pickup_dir):
        os.makedirs(pickup_dir)

    print 'need_file ',need_file
    print 'data_root ',data_root

    dtype = np.dtype([('startdex', np.int), ('enddex', np.int),
                      ('date', np.int)])

    need_data = np.genfromtxt(need_file, dtype=dtype)

    unique_starts = np.unique(need_data['startdex'])
    print 'unique starts ',len(unique_starts)

    for start_dex in unique_starts:
        script_name_root = os.path.join(pickup_dir, '%s_%d' % (data_root, start_dex))
        batch  = need_data[np.where(need_data['startdex'] == start_dex)]
        batch_dates = batch['date']
        end_dex = batch['enddex'][0]
        batch_dates.sort()
        date_info_list = []
        t0 = batch_dates[0]
        for ii in range(1, len(batch_dates), 1):
            if batch_dates[ii]>batch_dates[ii-1]+30 or batch_dates[ii]-t0>1200:
                date_info_list.append((t0, batch_dates[ii-1]-t0+30))
                t0 = batch_dates[ii]
        date_info_list.append((t0, batch_dates[-1]-t0+30))
        for ii, date_info in enumerate(date_info_list):
            script_name = script_name_root+'_%d.sl'%ii
            with open(script_name, 'w') as script_file:
                make_header(script_file)
                cmd = make_cmd(data_root, start_dex, end_dex,
                               date_info[0], date_info[1])
                script_file.write('\n%s\n\n' % cmd) 
                write_conclusion(script_file, data_root,
                                 start_dex, end_dex)
