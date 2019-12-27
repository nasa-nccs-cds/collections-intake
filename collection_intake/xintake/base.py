import intake, os, pprint, warnings
from intake.config import conf as iconf
from intake.catalog.local import Catalog
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import xarray as xa
pp = pprint.PrettyPrinter(depth=4).pprint
warnings.simplefilter(action='ignore', category=FutureWarning)
def str_dict( x: Dict ) -> Dict: return { str(key): str(value) for key,value in x.items() }

class Grouping:

    def __init__( self, name: str = "root", **kwargs ):
        self.catalog: Optional[Catalog] = None
        self.name = name
        self.description = kwargs.get( "description", "" )
        self.metadata = kwargs.get( "metadata", {} )

    def printMetadata(self):
        pp( self.metadata )

    @classmethod
    def getCatalogFilePath( cls, path_nodes: List[str], **kwargs ):
        root_dir = kwargs.get( "base", cls.getCatalogsPath() )
        name = kwargs.get( "name", "catalog" )
        cat_dir = os.path.join( root_dir, *path_nodes )
        catalog_file = os.path.join( cat_dir, f"{name}.yaml" )
        os.makedirs( cat_dir, exist_ok=True )
        return catalog_file

    @classmethod
    def getCatalogsPath( cls ):
        catalog_paths = iconf.get( "catalog_path" )
        if catalog_paths is None:
            ilDataDir = os.environ.get('ILDATA')
            assert ilDataDir is not None, "Must set the ILDATA environment variable to define the data directory"
            catalog_path = os.path.join( ilDataDir, "collections", "intake_IL" )
        else:
            catalog_path = catalog_paths[0]
        os.makedirs( catalog_path, exist_ok=True )
        return catalog_path

    @classmethod
    def getBaseCatalog(cls, catalog_path = None ) :
        cats_dir = cls.getCatalogsPath()
        cat_path = catalog_path.append( "catalog.yaml" ) if catalog_path else [ "catalog.yaml" ]
        return os.path.join( cats_dir,  *cat_path )

    def close(self):
        if self.catalog:  self.catalog.close()

    def __del__(self):
        self.close()