import intake, os, pprint, warnings
from intake.config import conf as iconf
from intake.catalog.local import YAMLFileCatalog
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import xarray as xa
pp = pprint.PrettyPrinter(depth=4).pprint
warnings.simplefilter(action='ignore', category=FutureWarning)

class Grouping:

    def __init__( self, name: str = "root", **kwargs ):
        self.catalog: Optional[YAMLFileCatalog] = None
        self.name = name
        self.description = kwargs.get( "description", "" )
        self.metadata = kwargs.get( "metadata", {} )

    def printMetadata(self):
        pp( self.metadata )

    def getCatalogFilePath( self, **kwargs ):
        collection = kwargs.get( 'collection' )
        root_dir = kwargs.get( "path", self.getCatalogsPath() )
        if self.name == "root":
            cat_dir = root_dir
        else:
            path_nodes = [ root_dir, collection, self.name ] if collection else  [ root_dir, self.name ]
            cat_dir = os.path.join( *path_nodes )
        catalog_file = os.path.join( cat_dir, "catalog.yaml")
        os.makedirs( cat_dir, exist_ok=True )
        return catalog_file

    def getCatalogsPath( self ):
        catalog_paths = iconf.get( "catalog_path" )
        if catalog_paths is None:
            ilDataDir = os.environ.get('ILDATA')
            assert ilDataDir is not None, "Must set the ILDATA environment variable to define the data directory"
            catalog_path = os.path.join( ilDataDir, "collections", "intake_IL" )
        else:
            catalog_path = catalog_paths[0]
        os.makedirs( catalog_path, exist_ok=True )
        return catalog_path

    def close(self):
        if self.catalog:  self.catalog.close()

    def __del__(self):
        self.close()