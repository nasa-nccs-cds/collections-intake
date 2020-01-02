import intake, os, pprint
from intake_xarray.netcdf import NetCDFSource
from intake.config import conf as iconf
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog, Catalog
from collection_intake.xintake.base import IntakeNode, pp, str_dict
from collection_intake.xintake.catalog import CatalogNode
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import xarray as xa
from intake.source.base import DataSource

# class Catalog()

class CollectionsManager:
    def __init__( self, **kwargs ):
        self.catalog_path = kwargs.get( "cat_path", CatalogNode.getCatalogURI() )

    @property
    def catalog(self) -> Catalog:
        return intake.open_catalog( self.catalog_path, driver="yaml_file_cat" )

    @classmethod
    def getCatalog(cls, cat_path: str ) -> Catalog:
        return intake.open_catalog( CatalogNode.getCatalogURI( cat_path.split('/') ) )

collections = CollectionsManager()



