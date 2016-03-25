from __future__ import with_statement
import os

n_nodes = 1

def make_header(handle, total_tasks):

    handle.write("#!/bin/bash -l\n\n")

    handle.write("#SBATCH -p regular\n")
    handle.write("#SBATCH -n %d\n" % total_tasks)
    handle.write("#SBATCH --ntasks-per-node=32\n")
    handle.write("#SBATCH -t 10:00:00\n\n")

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




if __name__ == "__main__":

    total_jobs = 1000000
    sleep_length = 1
    dn = 300
    batches_per_script = 1

    sub_dex = -1
    cmd_ct = 0

    sub_script_dir = os.path.join(os.getenv('SCRATCH'), 'subScripts')
    if not os.path.exists(sub_script_dir):
        os.makedirs(sub_script_dir)

    data_dir = os.getenv('SCRATCH')
    data_dir = os.path.join(data_dir,'neoFullInputFiles')
    list_of_orbit_files = os.listdir(data_dir)

    i_master = -1
    master_script = None 
    sub_script = None

    input_orbit = 'S1_00.s3m.0.des'

    for i_start in range(0, total_jobs, dn):

        if master_script is None:
            i_master += 1
            master_script_name = '%s.master_script_%d.sl' % (input_orbit, i_master)
            output_dir = os.path.join('$SCRATCH', master_script_name.replace('.','_'))
            master_script = open(master_script_name, 'w')
            make_header(master_script, min(32*n_nodes, 1+(total_jobs-i_start)/(batches_per_script*dn)))

        if sub_script is None:
            sub_dex += 1
            sub_name = os.path.join(sub_script_dir, '%s.sub_script_%d.sh' % (input_orbit, sub_dex))
            sub_sub_dir = "sub_out_%d" % sub_dex
            sub_output_dir = os.path.join(output_dir, sub_sub_dir)
            sub_script = open(sub_name, 'w')
            sub_script.write('#!/bin/bash\n')
 

        cmd = 'python $NEODIR/gen_ssm_db/gen_coeff_flexible_recursive.py $SCRATCH/neoFullInputFiles/%s 59580.0 30 14 3653 %s %d %d\n' % (input_orbit, sub_output_dir, i_start, i_start+dn-1)

        sub_script.write(cmd)

        cmd_ct += 1
        if cmd_ct % batches_per_script == 0:
            tar_name = 'subset_%d.tar' % sub_dex
            sub_script.write('cd %s\n' % sub_output_dir)
            sub_script.write('tar -cf %s *\n' % tar_name)
            sub_script.write('gzip %s\n' % tar_name)
            sub_script.write('cd ..\n')
            sub_script.write('mv %s/%s.gz ./\n' % (sub_sub_dir, tar_name))
            sub_script.write('rm %s/*\n' % sub_sub_dir)
            sub_script.write('rmdir %s\n' % sub_sub_dir)
            sub_script.close()
            os.chmod(sub_name, 0777)
            master_script.write('srun --exclusive -c 1 -n 1 -N 1 --mem-per-cpu=3000 --gres=craynetwork:0 %s &\n' % sub_name)
            master_script.write('sleep %s\n\n' % str(sleep_length))
            sub_script = None

        if cmd_ct % (n_nodes*32*batches_per_script)==0:
            master_script.write("\nwait\n")
            master_script.write("echo 'done with %s'\n" % master_script_name)
            master_script.write("date")
            master_script.close()
            master_script=None
            os.chmod(master_script_name, 0777)


    if sub_script is not None:
        master_script.write('srun --exclusive -c 1 -n 1 -N 1 --mem-per-cpu=1500 --gres=craynetwork:0 %s &\n' % sub_name)
        sub_script.close()
        os.chmod(sub_name, 0777)

    if master_script is not None:
        master_script.write("\nwait\n")
        master_script.write("echo 'done with %s'\n" % master_script_name)
        master_script.write("date")
        master_script.close()
        master_script=None
        os.chmod(master_script_name, 0777)




