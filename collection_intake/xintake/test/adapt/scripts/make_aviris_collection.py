from glob import glob
from collection_intake.xintake.aggregation import Aggregation
from collection_intake.xintake.collection import Collection
from collection_intake.xintake.base import Grouping, pp
import os, intake
print( f"Intake drivers: {list(intake.registry)}" )

collection_name = "ORNL_ABoVE_Airborne_AVIRIS_NG"
collection_root = "/att/pubrepo/ABoVE/archived_data/ORNL/ABoVE_Airborne_AVIRIS_NG/"
aggs = dict( ang_rfl_v2r2 = f"{collection_root}/data/ang*rfl/ang*_rfl_v2r2/*_img" )
catalog_files = []

for agg_name, agg_dir in aggs.items():
    print( f"Creating aggregation {agg_name}")
    agg_files =  glob( f"{agg_dir}/ang*rfl/ang*/*_img" )
    print( "Got Aviris files: ")
    pp( agg_files )

    source: intake.DataSource = intake.open_rasterio(agg_files, concat_dim="time")
    source.discover()
    print( source.shape )

#    agg = Aggregation( agg_name, collection=collection_name, files=agg_files )
#    md = agg.getMetadata()
#    agg.description = f"{md['description']}"
#    catalog_files.append( agg.writeCatalogFile() )

# collection = Collection( collection_name, description="ORNL ABoVE Airborne AVIRIS NG 2017" )
# collection.generate( cats = catalog_files )
#
# root_collection = Collection( description="NCCS data collections" )
# root_collection.generate(  )

