import os, xarray, time, math
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from multiprocessing import Pool
import multiprocessing as mp

base_dir = "/nfs4m/css/curated01/merra2"
suffix = ".nc4"
lines = []
files = []

for root, dirs, files in os.walk( base_dir ):
   if len( files ) > 0:
       files.append( dict( dir=root, files=files ) )

def test_files( root: str, files: List[str]):
   print( f"     -- Scanning dir {root}, nfiles: {len(files)}" )
   for fname in files:
       if fname.endswith(suffix):
           file_path = os.path.join(root, fname)
           try:
               ds = xarray.open_dataset(file_path)
           except Exception as err:
               print(f"** Found bad file {file_path}, err: {err}")
               lines.append( f"{file_path}, {err}" )


t0 = time.time()
nproc = 2*mp.cpu_count()
chunksize = math.ceil( len(files) / nproc )
with Pool(processes=nproc) as pool:
    pool.map( test_files, files, chunksize )

print( f"Completed test_files in {(time.time()-t0)/60.0} minutes using {nproc} processes" )

bad_files_name = f"/tmp/bad_files-{os.path.basename(base_dir)}.csv"
bad_files = open(bad_files_name, "w")
bad_files.writelines(lines)
bad_files.close()
print( f"Wrote bad files list to {bad_files_name}, nfiles: {len(lines)}" )



