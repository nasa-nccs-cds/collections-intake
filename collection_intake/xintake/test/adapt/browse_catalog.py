import intake
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog, Catalog, LocalCatalogEntry
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.manager import collections

image = collections.catalog.image.get()
print( image )


print( collections.catalog.__class__.__name__ )
print( image.__class__.__name__ )


# print( "Elements:" )
# for item in image.el:
#     print( item )