import intake, os, pprint
from intake.config import conf as iconf
from intake.catalog.local import YAMLFileCatalog
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import xarray as xa
pp = pprint.PrettyPrinter(depth=4).pprint

class Grouping:

    def __init__( self, name: str, **kwargs ):
        self.name = name
        self.description = kwargs.get( "description", "" )
        self.metadata = kwargs.get( "metadata", {} )
        self.catalog: Optional[YAMLFileCatalog] = None

    def printMetadata(self):
        pp( self.metadata )

    def getCatalogFilePath( self, **kwargs ):
        collection = kwargs.get( 'collection' )
        root = kwargs.get( "path", self.getCatalogsPath() )
        path_nodes = [ root, collection, self.name ] if collection else  [ root, self.name ]
        agg_dir = os.path.join( *path_nodes )
        agg_catalog_file = os.path.join( agg_dir, "catalog.yaml")
        os.makedirs( agg_dir, exist_ok=True )
        return agg_catalog_file

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