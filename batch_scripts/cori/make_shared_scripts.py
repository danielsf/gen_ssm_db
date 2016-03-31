from __future__ import with_statement
import os

n_nodes = 1

def make_header(handle):

    handle.write("#!/bin/bash -l\n\n")

    handle.write("#SBATCH -p shared\n")
    handle.write("#SBATCH -n 1\n")
    handle.write("#SBATCH -t 6:00:00\n\n")

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
    dn = 300
    cmd_ct = 0

    data_dir = os.getenv('SCRATCH')
    data_dir = os.path.join(data_dir,'neoFullInputFiles')
    list_of_orbit_files = os.listdir(data_dir)

    i_master = -1
    master_script = None 

    input_orbit = 'S1_00.s3m.0.des'

    output_dir = os.path.join('$SCRATCH', input_orbit.replace('.','_')+'_shared')

    for i_start in range(0, total_jobs, dn):
        i_end=i_start+dn-1
        i_master += 1
        master_script_name = '%s.shared_script_%d.sl' % (input_orbit, i_master)
        with open(master_script_name, 'w') as master_script:

            make_header(master_script)

            cmd = 'srun -n 1 --exclusive --mem-per-cpu=3000 python $NEODIR/gen_ssm_db/gen_coeff_flexible_recursive.py $SCRATCH/neoFullInputFiles/%s 59580.0 30 14 3653 %s %d %d\n' % (input_orbit, output_dir, i_start, i_end)

            master_script.write(cmd)

            tar_name = 'objects_%d_%d.tar' % (i_start, i_end)
            master_script.write('\ncd %s\n' % output_dir)
            master_script.write('tar -cf %s *%d_%d* --remove-files\n' % (tar_name,i_start,i_end))
            master_script.write('gzip %s\n' % tar_name)

            master_script.write("echo 'done with %s'\n" % master_script_name)
            master_script.write("date")

        os.chmod(master_script_name, 0777)





