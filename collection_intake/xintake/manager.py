import intake, os, pprint
from intake.config import conf as iconf
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog, Catalog
from collection_intake.xintake.base import IntakeNode, pp, str_dict
from collection_intake.xintake.catalog import CatalogNode
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional

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

    @classmethod
    def saveCatalog(cls, catalog: Catalog ) -> str:
        cat_path = catalog.path
        catalog.save( cat_path )
        IntakeNode.patch_yaml( cat_path )
        return cat_path

collections = CollectionsManager()



