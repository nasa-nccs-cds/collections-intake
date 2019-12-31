import intake, os, pprint, warnings, abc
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

class Grouping:
    __metaclass__ = abc.ABCMeta
    ReloadableDrivers = [ 'rasterio' ]

    def __init__( self, path_nodes: List[str], **kwargs ):
        self._catalog: Optional[Catalog] = None
        self._path_nodes: List[str] = path_nodes
        self._initCatalog(**kwargs)

    @classmethod
    def getCatalogBase(cls, **kwargs) -> "Grouping":
        return Grouping( [], **kwargs )

    def printMetadata(self):
        pp( self._catalog.metadata )

    @property
    def name(self) -> str:
        return self._path_nodes[-1] if self._path_nodes else "root"

    @property
    def catPath(self) -> str:
        return "/".join(self._path_nodes)

    @property
    def catalog(self) -> Catalog:
        return self._catalog

    @abc.abstractmethod
    def _newCatalog( self, cat_uri: str, **kwargs ) -> Catalog:
        raise NotImplemented( "Grouping._newCatalog" )

    def _initCatalog(self, **kwargs):
        cat_uri = self.getURI(**kwargs)
        self._catalog = self._newCatalog( cat_uri )
        description = kwargs.get( "description", None )
        metadata = kwargs.get( "metadata", None )
        if description: self._catalog.description = description
        if metadata: self._catalog.metadata = metadata
        self._catalog.name = self.name
        self.save( cat_uri, **kwargs )

    def getURI ( self, **kwargs ):
        base_uri = kwargs.get( "base", self.getIntakeURI())
        source_name = kwargs.get( "source" )
        cat_dir = os.path.join( base_uri, *self._path_nodes )
        cat_file_name = f"catalog-{source_name}.yaml" if source_name else f"catalog.yaml"
        catalog_uri = os.path.join( cat_dir, cat_file_name )
        os.makedirs( cat_dir, exist_ok=True )
        return catalog_uri

    def getSourceUri(self, name:str, **kwargs ) -> str:
        return self.getURI( source=name, **kwargs)

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

    def save( self, cat_uri=None,  **kwargs ):
        catUri = cat_uri if cat_uri else self.getURI(**kwargs)
        print( f"    %%%% -->  Catalog {self.name} saving to: {catUri}")
        self._catalog.save(catUri)

    @classmethod
    def setSourceAttr( cls, dataSource: DataSource, key: str, value: str):
        attr_value = dataSource.metadata.get(value[1:], "") if value.startswith('@') else value
        setattr( dataSource, key, str(attr_value) )