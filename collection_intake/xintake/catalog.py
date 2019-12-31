import intake, os, pprint, warnings
from intake.config import conf as iconf
from intake.catalog.local import Catalog
from intake.catalog.local import YAMLFileCatalog
from collection_intake.xintake.base import Grouping, pp, str_dict
from glob import glob
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional, Union

def globs( fglobs: Union[str,List[str]] ) -> List[str]:
    fglobList = fglobs if isinstance(fglobs, list) else [fglobs]
    fileList = []
    for filesGlob in fglobList:
        fileList.extend( glob( filesGlob ) )
    return fileList

class CatalogNode(Grouping):

    def __init__( self, path_nodes: List[str], **kwargs ):
        Grouping.__init__(self, path_nodes, **kwargs)

    def _newCatalog( self, cat_uri: str, **kwargs ) -> Catalog:
        file_exists = os.path.isfile( cat_uri )
        catalog: YAMLFileCatalog = intake.open_catalog( cat_uri, driver="yaml_file_cat", autoreload=file_exists )
        if file_exists: catalog.discover()
        return catalog

    def addSubGroup(self, name: str, **kwargs ) -> "Grouping":
        subGroup: Grouping =  Grouping( self._path_nodes + [name], **kwargs )
        self._catalog.add( subGroup.catalog )
        print( f"Added Catalog: {self.catPath}" )
        return subGroup