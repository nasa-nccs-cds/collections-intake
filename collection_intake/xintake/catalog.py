import intake, os, pprint, warnings
from intake.config import conf as iconf
from intake.catalog.local import YAMLFileCatalog, Catalog
from collection_intake.xintake.base import IntakeNode, pp, str_dict
from collection_intake.xintake.collection import DataCollection
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional, Union

class CatalogNode(IntakeNode):

    def __init__( self, path_nodes: List[str], **kwargs ):
        IntakeNode.__init__(self, path_nodes, **kwargs)

    @classmethod
    def getCatalogBase(cls, **kwargs) -> "CatalogNode":
        return CatalogNode([], **kwargs)

    def _initializeCatalog(self, **kwargs):
        file_uri: str = self.catURI
        file_exists = os.path.isfile( file_uri )
        self._catalog = intake.open_catalog( file_uri, driver="yaml_file_cat", autoreload=file_exists, name=self.name, **kwargs )
        if file_exists: self._catalog.discover()
        self.save()

    def addCatalogNode(self, name: str, **kwargs ) -> "CatalogNode":
        catNode: CatalogNode =  CatalogNode(self._path_nodes + [name], **kwargs)
        self._catalog.add( catNode.catalog )
        print( f"Add Catalog: {self.catPath}" )
        return catNode

    def addDataCollection(self, name: str, **kwargs ) -> "DataCollection":
        collection: DataCollection =  DataCollection( self._path_nodes + [name], **kwargs )
        self._catalog.add( collection.catalog )
        print( f"Adding DataCollection: {self.catPath}" )
        return collection