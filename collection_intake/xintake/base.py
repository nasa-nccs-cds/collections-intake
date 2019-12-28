import intake, os, pprint, warnings
from intake.config import conf as iconf
from intake.catalog.local import Catalog
from intake.catalog.local import YAMLFileCatalog
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import xarray as xa
pp = pprint.PrettyPrinter(depth=4).pprint
warnings.simplefilter(action='ignore', category=FutureWarning)
def str_dict( x: Dict ) -> Dict: return { str(key): str(value) for key,value in x.items() }

class Grouping:

    def __init__( self, path_nodes: List[str], **kwargs ):
        self._catalog: Optional[Catalog] = None
        self.path_nodes = path_nodes
        self._driver = kwargs.get( "driver", "yaml_file_cat" )
        self._catalog: Catalog = self.initCatalog(**kwargs)

    def printMetadata(self):
        pp( self._catalog.metadata )

    @property
    def catalog(self) -> Catalog:
        return self._catalog

    def addSubGroup(self, pathNode: str, **kwargs ) -> "Grouping":
        subGroup: Grouping =  Grouping( self.path_nodes + pathNode, **kwargs )
        self._addToCatalog( subGroup )
        return subGroup

    def _addToCatalog(self, subGroup: "Grouping" ):
        if self._driver == "yaml_file_cat":
            yaml_file_cat: YAMLFileCatalog = self._catalog
            yaml_file_cat.add( subGroup.catalog )
        else:
            raise Exception( f"Unrecognized driver: {self._driver}")


    def initCatalog(self, **kwargs ) -> Catalog:
        cat_uri = self.getCatalogURI(**kwargs)
        if os.path.isfile( cat_uri ):
            catalog: Catalog = intake.open_catalog(cat_uri, driver=self._driver)
            catalog.discover()
        else:
            from intake.source import registry
            catalog: Catalog = registry[self._driver]( cat_uri, **kwargs )
        description = kwargs.get( "description", None )
        metadata = kwargs.get( "metadata", None )
        name = kwargs.get( "name", None )
        if description: catalog.description = description
        if metadata: catalog.metadata = metadata
        if name: catalog.name = name
        catalog.save( cat_uri )
        return catalog

    def getCatalogURI ( self, **kwargs ):
        root_dir = kwargs.get( "base", self.getcatalogsPath() )
        name = kwargs.get( "name", self.name )
        cat_dir = os.path.join( root_dir, *self.path_nodes )
        catalog_file = os.path.join( cat_dir, f"{name}.yaml" )
        os.makedirs( cat_dir, exist_ok=True )
        return catalog_file

    @classmethod
    def getcatalogsPath( cls ) -> str:
        catalog_paths = iconf.get( "catalog_path"  )
        if catalog_paths is None:
            ilDataDir = os.environ.get('ILDATA')
            assert ilDataDir is not None, "Must set the ILDATA environment variable to define the data directory"
            catalog_path = os.path.join( ilDataDir, "collections", "intake_IL" )
        else:
            catalog_path = catalog_paths[0]
        os.makedirs( catalog_path, exist_ok=True )
        return catalog_path

    @classmethod
    def getBasecatalog(cls, catalog_path: List[str] = None ) :
        cats_dir = cls.getcatalogsPath()
        cat_file_path = [ cats_dir ]
        if catalog_path is not None:
            if isinstance(catalog_path, list):
                cat_file_path.extend( catalog_path )
            else:
                cat_file_path.append( catalog_path )
        return os.path.join(  *cat_file_path, "catalog.yaml"  )

    def close(self):
        if self._catalog:  self._catalog.close()

    def __del__(self):
        self.close()