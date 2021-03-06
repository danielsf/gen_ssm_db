# gen_ssm_db

This repository contains the code used make a solar system model of 11 million orbital elements queryable. For more information on context see SSDBM2011 poster: http://www.astro.washington.edu/users/yusra/SSDBM/SSDBMYA_print_final3.pdf


## Step 1: Install OpenOrb

```
git clone https://github.com/EUPSForge/oorb.git
cd oorb
./configure gfortran opt
cd python
make pyoorb
cd ..
```

### OpenOrb Libraries

This produces two libraries:
 * python/pyoorb.so
 * lib/liboorb.so or ../lib/liboorb.dylib

Note: do not be tricked by the file called python/liboorb.f90. This is not the liboorb you are looking for.

Make pyoorb is visible to your `$PYTHONPATH` and the liboorb.* is visible to your `$LD_LIBRARY_PATH` or `$DYLD_LIBRARY_PATH`:
```
export LD_LIBRARY_PATH=$PWD/lib/:$LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH=$PWD/lib/:$DYLD_LIBRARY_PATH
export PYTHONPATH=$PWD/python/:$PYTHONPATH
```

### OpenOrb configuration

You also need to control the integration configuration paramters. You can use the default configuration
in `main/oorb.conf` or point to any file you choose. The configuration used in CATSIM's 2011 ephemeris generation is in `gen_ssm_db/config/oorb.conf`.

csh:
```
setenv OORB_DATA $PWD/data
setenv OORB_CONF $PWD/main/oorb.conf
```
bash:
```
export OORB_DATA=$PWD/data
export OORB_CONF=$PWD/main/oorb.conf
```

You will also need the planetary ephemeris file: de405.dat.
Put this in data/

## Step 2: Clone and run unit tests
```
git clone https://github.com/yalsayyad/gen_ssm_db.git
cd gen_ssm_db
export PYTHONPATH=$PWD:$PYTHONPATH
python tests/testChebFitAndEval.py
```
and check the main executable:
```
python gen_coeff_flexible_recursive.py tests/S1_00.zzz_49353.des 49383 30 14 30
```

## Running on a cluster

There are two types of clusters: clusters in which you recieve a whole node per job and must utlize all cores, and clusters in which you receive one core per job. This following procedure assumes the former.

* Install gnu parallel: http://www.gnu.org/software/parallel/
* Copy over your orbit (.des) files and split them into files with 10000 lines.
```
$SHARED_SCRATCH/orbits/49353/
	S1_00/
		S1_00.xaa_49353.des
		S1_00.xab_49353.des
		...
		S1_00.xdv_49353.des
	S1_00/
		S1_01.xaa_49353.des
		S1_01.xab_49353.des
		...
		S1_01.xdv_49353.des	
	...
	S1_12/	
		S1_12.xaa_49353.des
		S1_12.xab_49353.des
		...
		S1_12.xdv_49353.des	
```

* Make an empty directory structure of MJDs spaced 30 days apart to hold the output coefficients.
```
$SHARED_SCRATCH/coeffs/
	49343/
		S1_00
		S1_01
		...
		S1_12
		S0
		Sc
		..
		St8
  	49373/
  	49403/
  	...
  	52973/
```

* Check that the shell script that sets up all necessary environment variables,  copies the data to and back from the node, and runs the main executable gen_coeff_flexible_recursive.py works. 

* Make pbs files or scheduler-specific jobs.

* submit pbs files e.g.
`for file  in  S1_10_*.pbs; do qsub $file; done;`
