import os, xarray
base_dir = "/nfs4m/css/curated01/merra2"
suffix = ".nc4"

lines = []
for root, dirs, files in os.walk( base_dir ):
   for fname in files:
       if fname.endswith(suffix):
           file_path = os.path.join(root, fname)
           try:
               ds = xarray.open_dataset(file_path)
           except Exception as err:
               lines.append( f"{file_path}, {err}" )

bad_files = open(f"/tmp/bad_files-{os.path.basename(base_dir)}.csv", "w")
bad_files.writelines(lines)
bad_files.close()


