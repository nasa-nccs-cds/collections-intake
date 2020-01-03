from collection_intake.xintake.manager import collections
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog, Catalog
import xarray as xa

cat_path = 'reanalysis/MERRA'
print(f'Reading {cat_path}')
merra: Catalog = collections.getCatalog(cat_path)

child_ids = [ item[0] for item in merra.items() ]
child_id = child_ids[0]

print( f"Child ID: {child_id}" )
print( f"merra[ {child_id} ]: \n{merra[ child_id ]}" )


