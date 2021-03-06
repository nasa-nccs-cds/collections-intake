from glob import glob
from collection_intake.xintake.scrap.collection import Collection
from dateutil.parser import parse
from collection_intake.xintake.sources.aviris import AvirisFileCatEntry
import intake
print( f"Intake drivers: {list(intake.registry)}" )

def get_time( fname: str ):
    time_start = len(collection_root) + 9
    time_end = time_start + 15
    return str( parse( fname[time_start:time_end] ) ).replace( " ", "_" ).replace( ":", "-" )

collection_name = "ORNL_ABoVE_Airborne_AVIRIS_NG"
collection_root = "/att/pubrepo/ABoVE/archived_data/ORNL/ABoVE_Airborne_AVIRIS_NG"
aggregations = dict( ang_rfl_v2r2 = f"{collection_root}/data/ang*/ang*_rdn_v2r2/*_img" )
sub_collections = []

for agg_name, agg_files_glob in aggregations.items():
    agg_files =  glob( agg_files_glob )
    cat_files = []
    for data_file in agg_files:
        fileId = get_time(data_file)
        entry = AvirisFileCatEntry( fileId, collection_name, data_file )
        cat_file = entry.writeCatalogFile( cat_nodes=[ collection_name, agg_name ] )
        if cat_file is not None:
            cat_files.append( cat_file )

    collection = Collection( agg_name, description=f"ORNL ABoVE Airborne AVIRIS NG: {agg_name}" )
    sub_collections.append( collection.generate( cat_nodes=[ collection_name, agg_name ], cats = cat_files ) )

above_collection = Collection( collection_name, description="ORNL ABoVE Airborne AVIRIS NG" )
above_collection.generate( cat_nodes=[ collection_name ], cats=sub_collections )

root_collection = Collection( description="NCCS Data Holdings" )
root_collection.generate()

