import os, xarray
base_dir = "/nfs4m/css/curated01/merra2"
suffix = ".nc4"

lines = []
for root, dirs, files in os.walk( base_dir ):
   print( f"     -- Scanning dir {root}, nfiles: {len(files)}" )
   for fname in files:
       if fname.endswith(suffix):
           file_path = os.path.join(root, fname)
           try:
               ds = xarray.open_dataset(file_path)
           except Exception as err:
               print(f"** Found bad file {file_path}, err: {err}")
               lines.append( f"{file_path}, {err}" )

bad_files_name = f"/tmp/bad_files-{os.path.basename(base_dir)}.csv"
bad_files = open(bad_files_name, "w")
bad_files.writelines(lines)
bad_files.close()
print( f"Wrote bad files list to {bad_files_name}, nfiles: {len(lines)}" )



