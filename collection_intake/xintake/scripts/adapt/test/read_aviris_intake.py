from collection_intake.xintake.manager import collections
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog, Catalog
from intake.catalog.entry import CatalogEntry
from intake.source.base import DataSource
import xarray as xa

catalog_path = "image/ABoVE/ORNL_AVIRIS_NG/ang_rdn_v2r2"
file_name = "ang_rdn_v2r2-99-2018-07-22_23-00-37"
ang_rdn_v2r2: Catalog = collections.getCatalog(catalog_path )
aviris_cat_entry: CatalogEntry = ang_rdn_v2r2[ file_name ]
aviris_data_source: DataSource = aviris_cat_entry.get( chunks=dict( y=100, x=100 ) )
aviris_data_array: xa.DataArray = aviris_data_source.to_dask()
mean_value = aviris_data_array.mean( dim="band" )
