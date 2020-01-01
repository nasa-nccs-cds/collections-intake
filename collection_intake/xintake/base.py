import intake, os, pprint, warnings, abc
from abc import ABC, abstractmethod
from intake.config import conf as iconf
from intake.catalog.local import Catalog
from intake.catalog.local import YAMLFileCatalog
from intake.source.base import DataSource
from glob import glob
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional, Union
pp = pprint.PrettyPrinter(depth=4).pprint
warnings.simplefilter(action='ignore', category=FutureWarning)
def str_dict( x: Dict ) -> Dict: return { str(key): str(value) for key,value in x.items() }

def globs( fglobs: Union[str,List[str]] ) -> List[str]:
    fglobList = fglobs if isinstance(fglobs, list) else [fglobs]
    fileList = []
    for filesGlob in fglobList:
        fileList.extend( glob( filesGlob ) )
    return fileList

class IntakeNode(ABC):
    __metaclass__ = abc.ABCMeta
    ReloadableDrivers = [ 'rasterio' ]

    def __init__( self, path_nodes: List[str], **kwargs ):
        self._catalog: Optional[Catalog] = None
        self._path_nodes: List[str] = path_nodes
        self._initializeCatalog( **kwargs )

    def printMetadata(self):
        pp( self._catalog.metadata )

    @property
    def name(self) -> str:
        return self._path_nodes[-1] if self._path_nodes else "root"

    @property
    def catPath(self) -> str:
        return "/".join(self._path_nodes)

    @property
    def catDir ( self ) -> str:
        cat_dir = os.path.join( self.getIntakeURI(), *self._path_nodes )
        os.makedirs(cat_dir, exist_ok=True)
        return cat_dir

    @property
    def catalog(self) -> Catalog:
        return self._catalog

    def _initializeCatalog(self, **kwargs):
        file_uri: str = self.catURI
        file_exists = os.path.isfile( file_uri )
        self._catalog = intake.open_catalog( file_uri, driver="yaml_file_cat", autoreload=file_exists, name=self.name, **kwargs )
        if file_exists: self._catalog.discover()
        self.save()

    @property
    def catURI (self) -> str:
        return  os.path.join( self.catDir, "catalog.yaml" )

    @classmethod
    def getIntakeURI(cls) -> str:
        catalog_paths = iconf.get( "catalog_path"  )
        if catalog_paths:
            catalog_path = catalog_paths[0]
        else:
            ilDataDir = os.environ.get('ILDATA')
            assert ilDataDir is not None, "Must set the ILDATA environment variable to define the data directory"
            catalog_path = os.path.join( ilDataDir, "collections", "intake" )
        os.makedirs( catalog_path, exist_ok=True )
        return catalog_path

    @classmethod
    def getCatalogURI(cls, catalog_path: Union[str,List[str]] = None ) :
        cats_uri = cls.getIntakeURI()
        cat_file_path = [ cats_uri ]
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

    def save( self, **kwargs ):
        self._catalog.force_reload()
        catUri = kwargs.get( 'catalog_uri', self.catURI )
        print( f"    %%%% -->  Catalog {self.name} saving to: {catUri}")
        self._catalog.save(catUri)

    @classmethod
    def setSourceAttr( cls, dataSource: DataSource, key: str, value: str):
        attr_value = dataSource.metadata.get(value[1:], "") if value.startswith('@') else value
        setattr( dataSource, key, str(attr_value) )