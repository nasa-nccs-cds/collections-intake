from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.collection import DataCollectionGenerator
from
from dateutil.parser import parse

import intake
print( f"Intake drivers: {list(intake.registry)}" )

def get_source_name( collection: str, index: int, fname: str ):
    time_start = len(collection_root) + 9
    time_end = time_start + 15
    timeStr =  str( parse( fname[time_start:time_end] ) ).replace( " ", "_" ).replace( ":", "-" )
    return f"{collection}-{index}-{timeStr}"

collection_name = "ORNL_ABoVE_Airborne_AVIRIS_NG"
collection_root = "/att/pubrepo/ABoVE/archived_data/ORNL/ABoVE_Airborne_AVIRIS_NG"
aggregations = dict( ang_rdn_v2r2 = f"{collection_root}/data/ang*/ang*_rdn_v2r2/*_img" )
sub_collections = []

base_cat: CatalogNode = CatalogNode.getCatalogBase()
image_cat: CatalogNode = base_cat.addCatalogNode( "image", description="Remote sensing image collections" )
ABoVE_cat: CatalogNode = image_cat.addCatalogNode( "ABoVE", description="ABoVE Project data collections" )
ORNL_AVIRIS_NG_cat: CatalogNode = ABoVE_cat.addCatalogNode( "ORNL_AVIRIS_NG", description="ORNL ABoVE Airborne AVIRIS NG Collections" )
ang_rdn_v2r2: DataCollectionGenerator = ORNL_AVIRIS_NG_cat.addDataCollection( "ang_rdn_v2r2", description="ang_rdn_v2r2 collection", metadata={} )
ang_rdn_v2r2.addFileCollection( f"{collection_root}/data/ang*/ang*_rdn_v2r2/*_img", get_name=get_source_name, driver="rasterio", chunks={} )




