from glob import glob
from collection_intake.xintake.collection import Collection
from collection_intake.xintake.manager import CollectionsManager
from collection_intake.xintake.base import pp
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

cm = CollectionsManager()

base = cm.getBase()

col_image = cm.addCollection( base, "image", description="", metadata={} )

col_above = cm.addCollection( base, "ORNL_ABoVE_Airborne_AVIRIS_NG", description="", metadata={} )

col_ang_rdn_v2r2 = cm.addCollection( col_above, "ang_rdn_v2r2", description="", metadata={} )

cm.addFiles( col_above, f"{collection_root}/data/ang*/ang*_rdn_v2r2/*_img", description="", metadata={} )




