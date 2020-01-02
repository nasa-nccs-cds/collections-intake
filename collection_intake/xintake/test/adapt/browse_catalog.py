import intake
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog, Catalog
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.manager import collections

images: Catalog = collections.catalog.image
print( images )

print( "Elements:" )
for item in images.items():
    print( item )