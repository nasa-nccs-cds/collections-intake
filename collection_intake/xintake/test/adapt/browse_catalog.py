import intake
from intake.catalog.local import YAMLFileCatalog, Catalog, LocalCatalogEntry
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.manager import collections
from intake_xarray.raster import RasterIOSource
def cn( x ): return x.__class__.__name__
def pcn( x ): print( x.__class__.__name__ )

cat_path = 'image/ABoVE/ORNL_AVIRIS_NG/ang_rdn_v2r2'
print( f'Reading {cat_path}' )
ang_rdn_v2r2: Catalog = collections.getCatalog( cat_path )

print( f'Result: {ang_rdn_v2r2.discover()}'  )

data_source: RasterIOSource =  ang_rdn_v2r2['ang_rdn_v2r2-99-2018-07-22_23-00-37'].get()

dask_data_source = data_source.to_dask()
print( f'dask_data_source: {cn(dask_data_source)}'  )

