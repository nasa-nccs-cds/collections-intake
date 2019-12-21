from glob import glob
from collection_intake.xintake.aggregation import Aggregation
from collection_intake.xintake.collection import Collection
import os, intake
print( f"Intake drivers: {list(intake.registry)}" )

collection_name = "cip_merra2_mon"
collection_root = "/nfs4m/css/curated01/create-ip/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/mon/atmos"
agg_dirs = glob( f"{collection_root}/*" )
catalog_files = []

for agg_dir in agg_dirs:
    agg_name = os.path.basename( agg_dir )
    print( f"Creating aggregation {agg_name}")
    agg_files =  f"{agg_dir}/*.nc"
    agg = Aggregation( agg_name, collection=collection_name, files=agg_files )
    md = agg.getMetadata()
    agg.description = f"{md['source']}: {md['title']}"
    agg.version = md['creation_date']
    catalog_files.append( agg.writeCatalogFile() )

collection = Collection( collection_name )
collection.generate( cats = catalog_files )

root_collection = Collection(  )
root_collection.generate(  )

