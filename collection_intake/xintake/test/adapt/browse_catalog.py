import intake
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog, Catalog, LocalCatalogEntry
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.manager import collections

image_entry: LocalCatalogEntry = collections.catalog.image
image: Catalog = image_entry.get()

print( image_entry.describe() )

print( "Elements:" )
for item in image.items():
    print( item.__class__.__name__ )
    print( item )