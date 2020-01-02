import intake
from intake.catalog.local import YAMLFileCatalog, Catalog, LocalCatalogEntry
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.manager import collections

image_entry: LocalCatalogEntry = collections.catalog.image
print( image_entry.describe() )

image: YAMLFileCatalog = image_entry.get()
image.discover()

print( "Elements:" )
print( list( dict( image.items() ).keys() ) )

above_entry = image.ABoVE
print( above_entry.describe() )