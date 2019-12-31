from glob import glob
from collection_intake.xintake.scrap.aggregation import Aggregation
from collection_intake.xintake.scrap.collection import Collection
import os, intake
print( f"Intake drivers: {list(intake.registry)}" )

collection_name = "cip_merra2_mon"
collection_root = "/nfs4m/css/curated01/create-ip/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/mon"
agg_dirs = glob( f"{collection_root}/*/*" )
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

collection = Collection( collection_name, description="NASA-GMAO MERRA2 Reanalysis monthly means, reprocessed for Create-IP" )
collection.generate( cats = catalog_files )

root_collection = Collection( description="NCCS data collections" )
root_collection.generate(  )

