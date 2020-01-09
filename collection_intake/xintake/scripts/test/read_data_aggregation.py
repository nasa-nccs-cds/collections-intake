from collection_intake.xintake.manager import collections
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog, Catalog
from intake.catalog.entry import CatalogEntry
from intake.source.base import DataSource
import xarray as xa

catalog_path = "reanalysis/MERRA/DAILY"
agg_name = "DAILY"
catalog_node: Catalog = collections.getCatalog(catalog_path )
cat_entry: CatalogEntry = catalog_node[ agg_name ]
data_source: DataSource = cat_entry.get( chunks=dict( time=1 ) )
data_array: xa.DataArray = data_source.to_dask()
print("DONE")
