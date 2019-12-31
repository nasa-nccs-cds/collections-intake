from collection_intake.xintake.scrap.collection import Collection
from intake.catalog.local import YAMLFileCatalog
import xarray as xa

source_coll = Collection( "merra2" )
source_coll.generate( )

collection = Collection( "merra2" )
catalog: YAMLFileCatalog = collection.getCatalog()

ds: xa.Dataset = collection.open(agg="merra2-6hr")
print(f"variable QV: dims: {ds.QV.dims}, shape: {ds.QV.shape}, chunks = {ds.QV.chunks}")

