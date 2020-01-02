import intake
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog, Catalog
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.manager import collections

ABoVE: Catalog = collections.catalog.image.ABoVE
print( ABoVE )

print( "Elements:" )
for item in ABoVE.items():
    print( item )