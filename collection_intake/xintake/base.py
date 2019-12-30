import intake, os, pprint, warnings
from intake.config import conf as iconf
from intake.catalog.local import Catalog
from intake.catalog.local import YAMLFileCatalog
from intake.source.base import DataSource
from glob import glob
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional, Union
import xarray as xa
pp = pprint.PrettyPrinter(depth=4).pprint
warnings.simplefilter(action='ignore', category=FutureWarning)
def str_dict( x: Dict ) -> Dict: return { str(key): str(value) for key,value in x.items() }

def globs( globList: List[str] ) -> List[str]:
    fileList = []
    for filesGlob in globList:
        fileList.extend( glob( filesGlob ) )
    return fileList

class Grouping:

    ReloadableDrivers = [ 'rasterio' ]

    def __init__( self, path_nodes: List[str], **kwargs ):
        self._catalog: Optional[Catalog] = None
        self._path_nodes: List[str] = path_nodes
        self._catalog_driver = kwargs.get("driver", "yaml_file_cat")
        self._catalog: YAMLFileCatalog = self.initCatalog(**kwargs)

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

    def addSubGroup(self, name: str, **kwargs ) -> "Grouping":
        subGroup: Grouping =  Grouping( self._path_nodes + [name], **kwargs )
        self._addToCatalog( subGroup.catalog )
        print( f"Added Catalog: {self.catPath}" )
        return subGroup

    def _addToCatalog(self, source: DataSource, **kwargs ):
        if self._catalog_driver == "yaml_file_cat":
            yaml_file_cat: YAMLFileCatalog = self._catalog
            yaml_file_cat.add( source )
        else:
            raise Exception(f"Unrecognized driver: {self._catalog_driver}")
        if kwargs.get('save',False):
            self._catalog.save( self.getURI(**kwargs) )

    def initCatalog(self, **kwargs ) -> YAMLFileCatalog:
        cat_uri = self.getURI(**kwargs)
        catalog: YAMLFileCatalog = intake.open_catalog( cat_uri, driver="yaml_file_cat", autoreload=False )
        if os.path.isfile( cat_uri ): catalog.discover()
        description = kwargs.get( "description", None )
        metadata = kwargs.get( "metadata", None )
        if description: catalog.description = description
        if metadata: catalog.metadata = metadata
        catalog.name = self.name
        catalog.save( cat_uri )
        return catalog

    def initCatalog1(self, **kwargs ) -> YAMLFileCatalog:
        cat_uri = self.getURI(**kwargs)
        if os.path.isfile( cat_uri ):
            catalog: YAMLFileCatalog = intake.open_catalog( cat_uri, driver="yaml_file_cat", autoreload=False )
            catalog.discover()
        else:
            catalog: YAMLFileCatalog = YAMLFileCatalog( cat_uri, autoreload=False )
        description = kwargs.get( "description", None )
        metadata = kwargs.get( "metadata", None )
        if description: catalog.description = description
        if metadata: catalog.metadata = metadata
        catalog.name = self.name
        catalog.save( cat_uri )
        return catalog

    def getURI ( self, **kwargs ):
        base_uri = kwargs.get( "base", self.getIntakeURI())
        cat_dir = os.path.join( base_uri, *self._path_nodes )
        catalog_uri = os.path.join( cat_dir, f"catalog.yaml" )
        os.makedirs( cat_dir, exist_ok=True )
        return catalog_uri

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

    def addDataSources( self, source_file_globs: Union[str,List[str]], **kwargs ):
        files = source_file_globs if isinstance(source_file_globs,list) else [ source_file_globs ]
        aggFilesList: List[Union[str,List[str]]] = [ globs(files) ] if "concat_dim" in kwargs else globs(files)
        for aggFiles in aggFilesList:
            try:
                dataSource = self._createDataSource( aggFiles, **kwargs )
                self._initDataSource( dataSource, **kwargs )
                print(f"Adding DataSource to catalog {self.name}: {aggFiles}")
                self._addToCatalog(dataSource)
            except Exception as err:
                print( f" ** Skipped loading the data file(s) {aggFiles}:\n     --> Due to Error: {err} ")
        self._catalog.save(self.getURI( **kwargs ) )

    def _createDataSource(self, files: Union[str,List[str]], **kwargs) -> DataSource:
        from intake.source import registry
        driver = kwargs.get("driver", "netcdf")
        if driver in self.ReloadableDrivers: kwargs['force_reload'] = False
        dataSource = registry[ driver ]( files, **kwargs )
        return dataSource

    def _initDataSource(self, dataSource: DataSource, **kwargs ):
        dataSource.discover()
        attrs = kwargs.get("attrs", {})
        for key, value in attrs.items(): self.setSourceAttr( dataSource, key, value)
        dataSource.name = self.name
        dataSource.metadata = str_dict( dataSource.metadata )

    @classmethod
    def setSourceAttr( cls, dataSource: DataSource, key: str, value: str):
        attr_value = dataSource.metadata.get(value[1:], "") if value.startswith('@') else value
        setattr( dataSource, key, str(attr_value) )