from glob import glob
from collection_intake.xintake.aggregation import Aggregation
from collection_intake.xintake.collection import SourceCollection, Collection
import os, intake
print( f"Intake drivers: {list(intake.registry)}" )

collection_name = "cip_merra2_mon"
collection_root = "/nfs4m/css/curated01/create-ip/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/mon"
agg_dirs = glob( f"{collection_root}/*/*" )
aggs = {}

for agg_dir in agg_dirs:
    agg_name = os.path.basename( agg_dir )
    print( f"Creating aggregation {agg_name}")
    agg_files =  f"{agg_dir}/*.nc"
    agg = Aggregation( agg_name, collection=collection_name, files=agg_files )
    md = agg.getMetadata()
    agg.description = f"{md['source']}: {md['title']}"
    agg.version = md['creation_date']
    aggs[ agg_name ] = agg.dataSource

collection = SourceCollection( collection_name, description="NASA-GMAO MERRA2 Reanalysis monthly means, reprocessed for Create-IP" )
collection.generate( aggs = aggs )

root_collection = Collection( description="NCCS data collections" )
root_collection.generate(  )
