import os, xarray, time, math
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from multiprocessing import Pool
import multiprocessing as mp
from glob import glob

base_dirs = glob("/nfs4m/css/curated01/merra2/data/*")
bad_files_name = f"/tmp/bad_files-merra2.csv"
suffix = ".nc4"
print( base_dirs )

file_locations = []
lines = []

def test_files( base_dir: str ):
    print( f"Walking file system from {base_dir}")
    for root, dirs, files in os.walk( base_dir ):
        if len(files) > 0:
           print(".", end =" ")
           for fname in files:
               if fname.endswith(suffix):
                   file_path = os.path.join(root, fname)
                   try:
                       ds = xarray.open_dataset(file_path)
                   except Exception as err:
                       print(f"\n ** Found bad file {file_path}, err: {err}")
                       lines.append( f"{file_path}, {err}" )


t0 = time.time()
nproc = 2*mp.cpu_count()
chunksize = math.ceil( len(file_locations) / nproc )
with Pool(processes=nproc) as pool:
    pool.map( test_files, base_dirs, chunksize )

print( f"Completed test_files in {(time.time()-t0)/60.0} minutes using {nproc} processes" )

bad_files = open(bad_files_name, "w")
bad_files.writelines(lines)
bad_files.close()
print( f"Wrote bad files list to {bad_files_name}, nfiles: {len(lines)}" )



