from glob import glob
from collection_intake.xintake.aggregation import Aggregation
from collection_intake.xintake.collection import Collection
from collection_intake.xintake.base import Grouping, pp
from collections import OrderedDict
from dateutil.parser import parse
from collection_intake.xintake.sources.aviris import AvirisDataSource
import xarray as xa
import os, intake
print( f"Intake drivers: {list(intake.registry)}" )

def get_time( fname: str ):
    time_start = len(collection_root) + 9
    time_end = time_start + 15
    timeval: str = fname[time_start:time_end]
    tval = parse( timeval ) # .replace( "t", " " ) )
    return tval

def is_valid( aviris_file: str ):
    try:
        result: xa.DataArray = xa.open_rasterio(aviris_file)
        return True
    except Exception:
        return False

collection_name = "ORNL_ABoVE_Airborne_AVIRIS_NG"
collection_root = "/att/pubrepo/ABoVE/archived_data/ORNL/ABoVE_Airborne_AVIRIS_NG"
aggs = dict( ang_rfl_v2r2 = f"{collection_root}/data/ang*/ang*_rdn_v2r2/*_img" )
catalog_files = []

ordered_files = OrderedDict()
for agg_name, agg_files_glob in aggs.items():
    agg_files =  glob( agg_files_glob )
    print( f"Validating aggregation files for {agg_name}")
    valid_agg_files = [ agg_file for agg_file in agg_files if is_valid(agg_file) ]
    agg_files_dict = { get_time(agg_file): agg_file for agg_file in valid_agg_files }

    print( f"Creating aggregation {agg_name}")
#    source: intake.DataSource = intake.open_rasterio( agg_files_dict.values(), chunks = {}, concat_dim="time" )
    source: intake.DataSource = AvirisDataSource( list(agg_files_dict.values()), chunks = {}, concat_dim="time" )
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

