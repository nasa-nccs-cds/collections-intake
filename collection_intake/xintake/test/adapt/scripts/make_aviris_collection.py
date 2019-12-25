from glob import glob
from collection_intake.xintake.aggregation import Aggregation
from collection_intake.xintake.collection import Collection
from collection_intake.xintake.base import Grouping, pp
from collections import OrderedDict
from dateutil.parser import parse
import os, intake
print( f"Intake drivers: {list(intake.registry)}" )

def get_time( fname: str )-> str:
    time_start = len(collection_root) + 9
    time_end = time_start + 15
    timeval: str = fname[time_start:time_end]
    tval = parse( timeval.replace( "t", " " ) )
    return timeval

collection_name = "ORNL_ABoVE_Airborne_AVIRIS_NG"
collection_root = "/att/pubrepo/ABoVE/archived_data/ORNL/ABoVE_Airborne_AVIRIS_NG"
aggs = dict( ang_rfl_v2r2 = f"{collection_root}/data/ang*/ang*_rdn_v2r2/*_img" )
catalog_files = []

ordered_files = OrderedDict()
for agg_name, agg_files_glob in aggs.items():
    print( f"Creating aggregation {agg_name}")
    agg_files =  glob( agg_files_glob )
    print( "Got Aviris files: " )
    agg_file_dict = OrderedDict( [ ( get_time(agg_file), agg_file ) for agg_file in agg_files ] )
    print( agg_file_dict.keys() )

    source: intake.DataSource = intake.open_rasterio( agg_file_dict.values(), chunks = {}, concat_dim="time" )
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

