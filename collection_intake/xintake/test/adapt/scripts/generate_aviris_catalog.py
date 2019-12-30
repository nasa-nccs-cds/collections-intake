from glob import glob
from collection_intake.xintake.collection import Collection
from collection_intake.xintake.manager import CollectionsManager
from collection_intake.xintake.base import pp, Grouping
from dateutil.parser import parse
from collection_intake.xintake.sources.aviris import AvirisFileCatEntry

import os, intake
print( f"Intake drivers: {list(intake.registry)}" )

def get_time( fname: str ):
    time_start = len(collection_root) + 9
    time_end = time_start + 15
    return str( parse( fname[time_start:time_end] ) ).replace( " ", "_" ).replace( ":", "-" )

collection_name = "ORNL_ABoVE_Airborne_AVIRIS_NG"
collection_root = "/att/pubrepo/ABoVE/archived_data/ORNL/ABoVE_Airborne_AVIRIS_NG"
aggregations = dict( ang_rdn_v2r2 = f"{collection_root}/data/ang*/ang*_rdn_v2r2/*_img" )
sub_collections = []

base_cat = Grouping.getCatalogBase()
image_cat = base_cat.addSubGroup( "image", description="Remote sensing image collections" )
ABoVE_cat = image_cat.addSubGroup( "ABoVE", description="ABoVE Project data collections" )
ORNL_AVIRIS_NG_cat = ABoVE_cat.addSubGroup( "ORNL_AVIRIS_NG", description="ORNL ABoVE Airborne AVIRIS NG Collections" )
ang_rdn_v2r2_cat = ORNL_AVIRIS_NG_cat.addSubGroup( "ang_rdn_v2r2", description="ang_rdn_v2r2 collection", metadata={} )
ang_rdn_v2r2_cat.addDataSources( f"{collection_root}/data/ang*/ang*_rdn_v2r2/*_img" )




