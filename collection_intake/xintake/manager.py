import intake, os, pprint
from intake_xarray.netcdf import NetCDFSource
from intake.config import conf as iconf
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog
from collection_intake.xintake.base import IntakeNode, pp, str_dict
from collection_intake.xintake.catalog import CatalogNode
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import xarray as xa
from intake.source.base import DataSource

# class Catalog()

class CollectionsManager:
    def __init__( self, **kwargs ):
        catalog_path: List[str] = kwargs.get( "cat_path", None )
        self.baseCatFile = IntakeNode.getBaseCatalog()

    def openCatalog(self, catalog_path: List[str], **kwargs ) -> YAMLFilesCatalog:
        cat_node: CatalogNode = CatalogNode( catalog_path, **kwargs )
        cat_file = IntakeNode.getCatalogFilePath( catalog_path, **kwargs )
        catalog: YAMLFilesCatalog = intake.open_catalog( cat_file, driver="yaml_files_cat" )
        catalog.discover()
        return catalog