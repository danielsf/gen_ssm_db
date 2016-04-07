from __future__ import with_statement
import sys
import os
import numpy as np

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


def write_conclusion(handle, dir, data_root, start_dex, end_dex):
    handle.write("\ndeclare -i goahead\n")
    handle.write("goahead=1\n")
    handle.write("for (( ii=59580; ii<63233; ii+=30 ));\n")
    handle.write("do\n")
    handle.write("    if [ ! -e $SCRATCH/%s/%s_$ii\.done\.txt ]; then\n"
                 % (dir, data_root))
    handle.write("        goahead=0\n")
    handle.write("    fi\n")
    handle.write("done\n")
    handle.write("\n")
    handle.write("if [ $goahead == 1 ]; then\n")

    handle.write("    tar -cf objects_%d_%d.tar *%d_%d* --remove-files\n"
                 % (start_dex, end_dex, start_dex, end_dex))
    handle.write("    gzip objects_%d_%d.tar\n" % (start_dex, end_dex))
    handle.write("\n")


def generate_coeff_name(root_name, start_dex, end_dex, month):
   return "%s_%d_%d_%d.coeff_vartime_14.dat" % (root_name, start_dex, end_dex, month) 


def does_coeff_exist(target_dir, root_name, start_dex, end_dex, month):
    file_name = generate_coeff_name(root_name, start_dex, end_dex, month)
    full_name = os.path.join(target_dir, file_name)
    if not os.path.exists(full_name):
        return False
    return True

def validate_coeff_file(target_dir, root_name, start_dex, end_dex, month):
    dn = end_dex - start_dex + 1

    dtype_list = [('ii', np.int), ('name', str, 300),
                  ('start_date', np.float), ('end_date', np.float)]

    for ii in range(28):
        dtype_list.append(('x%d' % ii, np.float))

    dtype = np.dtype(dtype_list)


    file_name = generate_coeff_name(root_name, start_dex, end_dex, month)
    full_name = os.path.join(target_dir, file_name)
    if not os.path.exists(full_name):
        return False

    try:
        coeff_data = np.genfromtxt(full_name, dtype=dtype)
    except:
        return False
  
    unique_names = np.unique(coeff_data['name'])
 
    if len(unique_names) != dn:
        return False

    for object_name in unique_names:
        end_date_array = coeff_data['end_date'][np.where(coeff_data['name'] == object_name)[0]]
        end_date_sorted = np.sort(end_date_array)
        if np.abs(end_date_sorted[-1] - (month+30.0))>1.0e-20:
            return False
    
    return True 
                 


if __name__ == "__main__":

    root = sys.argv[1]
    print 'root ',root

    local_dir = root.replace('.','_')+'_shared'
    
    dn = 300
    start_date = 59580
    duration = 3653
    list_of_dates = range(start_date, start_date+duration, 30)
    total_asteroids = 1000000

    start_pts = range(0, total_asteroids, dn)
    end_pts = [ii+dn-1 if ii<total_asteroids else total_asteroids-1 for ii in start_pts]

    target_dir = os.path.join(os.getenv('SCRATCH'),local_dir)
    raw_list_of_files = os.listdir(target_dir)

    list_of_tar_files = [ff for ff in raw_list_of_files if ff.endswith("tar")]

    for ff in list_of_tar_files:
        full_name = os.path.join(target_dir, ff)
        os.unlink(full_name)

    with open(root+"_needs.txt", "w") as needs_file:
        for ss, ee in zip(start_pts, end_pts):
            final_file = "objects_%d_%d.tar.gz" % (ss, ee)
            if os.path.exists(os.path.join(target_dir, final_file)): 
                for date in list_of_dates:
                    if does_coeff_exist(target_dir, root, ss, ee, date):
                        needs_file.write('WARNING %s exists but %d has coeffs\n'
                                         % (final_file, ss))
            else:
                for date in list_of_dates:
                    local_need = validate_coeff_file(target_dir, root, ss, ee, date)
                    if not local_need:
                        needs_file.write("ss %d ee %d date %d'n"
                                         % (ss, ee, date))
                        if does_coeff_exist(target_dir, root, ss, ee, date):
                            os.system("rm %s/%s_%d_%d_%d*" %
                                      (target_dir, root, ss, ee, date))

