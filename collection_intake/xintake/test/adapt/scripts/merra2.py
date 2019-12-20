from glob import glob
from collection_intake.xintake.aggregation import Aggregation
from collection_intake.xintake.collection import Collection
import os

collection_name = "cip_merra2_mon"
collection_root = "/nfs4m/css/curated01/create-ip/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/mon/atmos"
agg_dirs = glob( f"{collection_root}/*" )
agg_dirs_test = [ agg_dirs[0] ]

for agg_dir in agg_dirs_test:
    agg_name = os.path.basename( agg_dir )
    print( f"Creating aggregation {agg_name}")
    agg_files =  f"{agg_dir}/*.nc"
    agg = Aggregation( agg_name, collection=collection_name, files=agg_files )
    agg.printMetadata()
#    agg.writeCatalogFile(attr = agg_attr)
