import intake
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog, Catalog
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.manager import collections

image: Catalog = collections.catalog.image
print( image )


print( collections.catalog.__class__.__name__ )
print( image.__class__.__name__ )


# print( "Elements:" )
# for item in image.el:
#     print( item )