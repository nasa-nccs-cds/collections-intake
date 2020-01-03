from collection_intake.xintake.manager import collections
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog, Catalog
import os
import xarray as xa

cat_path = 'reanalysis/MERRA'
merra: Catalog = collections.getCatalog(cat_path)
print(f'Reading {merra.path}')
items_list =  list(merra.items())
print(f'Items: {[item for item in items_list]}')

for id,item in items_list:
    print(f'Validating Item {id}, path = {item.path}')
    if not os.path.isfile(item.path):
        merra.pop( id )
        print( f"Removing Item {id}" )
    else: print( "Valid!")

collections.save( merra )




