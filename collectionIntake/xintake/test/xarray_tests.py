import intake
from intake.catalog.local import YAMLFileCatalog
import xarray as xa

data_source: YAMLFileCatalog = intake.open_catalog( "./catalog_local.yaml", driver="yaml_file_cat" )
data_source.discover()

ds: xa.Dataset = data_source.netcdf.to_dask()

print( f"variable QV: dims: {ds.QV.dims}, shape: {ds.QV.shape}, chunks = {ds.QV.chunks}"  )