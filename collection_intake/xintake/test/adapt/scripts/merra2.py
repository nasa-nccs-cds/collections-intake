from glob import glob
from collection_intake.xintake.aggregation import Aggregation
from collection_intake.xintake.collection import Collection
import os

collection_name = "cip_merra2_mon"
collection_root = "/nfs4m/css/curated01/create-ip/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/mon/atmos"
agg_dirs = f"{collection_root}/*"
print( f"Creating aggregations using glob: {agg_dirs}")

for agg_dir in glob( agg_dirs ):
    agg_name = os.path.basename( agg_dir )
    print( f"Creating aggregation {agg_name}")
#    agg = Aggregation( )

