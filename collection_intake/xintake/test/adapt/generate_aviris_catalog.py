from collection_intake.xintake.catalog import CatalogNode
from dateutil.parser import parse

import intake
print( f"Intake drivers: {list(intake.registry)}" )

def get_time( fname: str ):
    time_start = len(collection_root) + 9
    time_end = time_start + 15
    return str( parse( fname[time_start:time_end] ) ).replace( " ", "_" ).replace( ":", "-" )

collection_name = "ORNL_ABoVE_Airborne_AVIRIS_NG"
collection_root = "/att/pubrepo/ABoVE/archived_data/ORNL/ABoVE_Airborne_AVIRIS_NG"
aggregations = dict( ang_rdn_v2r2 = f"{collection_root}/data/ang*/ang*_rdn_v2r2/*_img" )
sub_collections = []

base_cat = CatalogNode.getCatalogBase()
image_cat = base_cat.addCatalogNode( "image", description="Remote sensing image collections" )
ABoVE_cat = image_cat.addCatalogNode( "ABoVE", description="ABoVE Project data collections" )
ORNL_AVIRIS_NG_cat = ABoVE_cat.addCatalogNode( "ORNL_AVIRIS_NG", description="ORNL ABoVE Airborne AVIRIS NG Collections" )
ang_v2r2 = ORNL_AVIRIS_NG_cat.addDataCollection( "ang_v2r2", description="ang_v2r2 collection", metadata={} )
ang_v2r2.addDataSource( "rdn", f"{collection_root}/data/ang*/ang*_rdn_v2r2/*_img", driver="rasterio", chunks={} )




