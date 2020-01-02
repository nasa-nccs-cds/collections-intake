import intake, os, pprint, warnings
from intake.config import conf as iconf
from intake.catalog.local import YAMLFileCatalog, Catalog
from collection_intake.xintake.base import IntakeNode, pp, str_dict
from collection_intake.xintake.collection import DataCollectionGenerator
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional, Union

class CatalogNode(IntakeNode):

    def __init__( self, path_nodes: List[str], **kwargs ):
        IntakeNode.__init__(self, path_nodes, **kwargs)

    @classmethod
    def getCatalogBase(cls, **kwargs) -> "CatalogNode":
        return CatalogNode([], **kwargs)

    def addCatalogNode(self, name: str, **kwargs ) -> "CatalogNode":
        catNode: CatalogNode =  CatalogNode( self._path_nodes + [name], **kwargs )
        self._catalog.add( catNode.catalog )
        print( f"Add Catalog: {self.catPath}" )
        return catNode

    def addDataCollection(self, name: str, **kwargs ) -> DataCollectionGenerator:
        collection: DataCollectionGenerator =  DataCollectionGenerator(self._path_nodes + [name], **kwargs)
        self._catalog.add( collection.catalog )
        print( f"Adding DataCollectionGenerator: {self.catPath}" )
        return collection

    @classmethod
    def open(cls, catalog_path: str ) -> "CatalogNode":
        catalog_path: List[str] = catalog_path.split("/")
        return CatalogNode( catalog_path )

    @property
    def sources(self) -> List[str]:
        return list( self.catalog._entries.keys() )

    def getSource( self, id: str ):
        return self.catalog._entries.get( id, None )