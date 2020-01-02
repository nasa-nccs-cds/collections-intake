import intake
from intake.catalog.local import YAMLFileCatalog, Catalog, LocalCatalogEntry
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.manager import collections

image_entry: LocalCatalogEntry = collections.catalog.image
print( image_entry.describe() )

image: YAMLFileCatalog = image_entry.get()
print( image.discover() )

ang_rdn_v2r2: Catalog = collections.getCatalog( 'image/ABoVE/ORNL_AVIRIS_NG/ang_rdn_v2r2')

print( list(ang_rdn_v2r2) )
