from __future__ import with_statement
import sys
import os
import numpy as np


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
    end_pts = [ii+dn-1 if ii+dn-1<total_asteroids else total_asteroids-1 for ii in start_pts]

    target_dir = os.path.join(os.getenv('SCRATCH'),local_dir)
    raw_list_of_files = os.listdir(target_dir)

    list_of_tar_files = [ff for ff in raw_list_of_files if ff.endswith("tar")]

    for ff in list_of_tar_files:
        full_name = os.path.join(target_dir, ff)
        os.unlink(full_name)

    with open(root+"_can_be_tarred.txt", "w") as safe_file:
        with open(root+"_needs.txt", "w") as needs_file:
            for ss, ee in zip(start_pts, end_pts):
                final_file = "objects_%d_%d.tar.gz" % (ss, ee)
                if os.path.exists(os.path.join(target_dir, final_file)): 
                    for date in list_of_dates:
                        if does_coeff_exist(target_dir, root, ss, ee, date):
                            needs_file.write('WARNING %s exists but %d has coeffs\n'
                                             % (final_file, ss))
                else:
                    n_invalid = 0
                    for date in list_of_dates:
                        local_need = validate_coeff_file(target_dir, root, ss, ee, date)
                        if not local_need:
                            n_invalid += 1
                            needs_file.write("%d %d %d\n"
                                             % (ss, ee, date))
                            if does_coeff_exist(target_dir, root, ss, ee, date):
                                os.system("rm %s/%s_%d_%d_%d*" %
                                          (target_dir, root, ss, ee, date))

                    if n_invalid == 0:
                        safe_file.write("can tar %d_%d\n" % (ss, ee))
 

