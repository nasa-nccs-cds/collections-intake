import os, xarray, time, math
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from multiprocessing import Pool
import multiprocessing as mp
from glob import glob

base_dirs = glob("/nfs4m/css/curated01/merra2/data/*")
errors_file = f"/tmp/bad_files-merra2.csv"
suffix = ".nc4"

lines = []
def test_files( base_dir: str ):
    print( f"Walking file system from {base_dir}")
    for root, dirs, files in os.walk( base_dir ):
        if len(files) > 0:
           for fname in files:
               if fname.endswith(suffix):
                   file_path = os.path.join(root, fname)
                   try:
                       ds = xarray.open_dataset(file_path)
                   except Exception as err:
                       print(f"{err}")
                       lines.append( f"{err}" )


t0 = time.time()
nproc = len(base_dirs) # 2*mp.cpu_count()
with Pool(processes=nproc) as pool:
    pool.map( test_files, base_dirs )

print( f"Completed test_files in {(time.time()-t0)/60.0} minutes using {nproc} processes" )

bad_files = open(errors_file, "w")
bad_files.writelines(lines)
bad_files.close()
print( f"Wrote bad files list to {errors_file}, nfiles: {len(lines)}" )



