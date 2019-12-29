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

class Grouping:

    def __init__( self, path_nodes: List[str], **kwargs ):
        self._catalog: Optional[Catalog] = None
        self._path_nodes: List[str] = path_nodes
        self._files: List[str] = kwargs.get('files',None)
        self._driver = kwargs.get( "driver", "yaml_file_cat" )
        self._source = kwargs.get("driver", "yaml_file_cat")
        self._catalog: Catalog = self.initCatalog(**kwargs)

    def printMetadata(self):
        pp( self._catalog.metadata )

    @property
    def name(self) -> str:
        return self._path_nodes[-1]

    @property
    def catalog(self) -> Catalog:
        return self._catalog

    def addSubGroup(self, name: str, **kwargs ) -> "Grouping":
        subGroup: Grouping =  Grouping( self._path_nodes + name, **kwargs )
        self._addToCatalog( subGroup.catalog )
        return subGroup

    def _addToCatalog(self, source: DataSource ):
        if self._driver == "yaml_file_cat":
            yaml_file_cat: YAMLFileCatalog = self._catalog
            yaml_file_cat.add( source )
        else:
            raise Exception( f"Unrecognized driver: {self._driver}")

    def initCatalog(self, **kwargs ) -> Catalog:
        cat_uri = self.getURI(**kwargs)
        if os.path.isfile( cat_uri ):
            catalog: Catalog = intake.open_catalog(cat_uri, driver=self._driver)
            catalog.discover()
        else:
            from intake.source import registry
            catalog: Catalog = registry[self._driver]( cat_uri, **kwargs )
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
        if catalog_paths is None:
            ilDataDir = os.environ.get('ILDATA')
            assert ilDataDir is not None, "Must set the ILDATA environment variable to define the data directory"
            catalog_path = os.path.join( ilDataDir, "collections", "intake" )
        else:
            catalog_path = catalog_paths[0]
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

    def addFileCollection(self, name: str, files: Union[str,List[str]], **kwargs ) -> "Grouping":
        fileList = files if isinstance(files,list) else [ files ]
        subGroup: Grouping =  Grouping( self._path_nodes + name, files=fileList, **kwargs )
        subGroup.addDataSources( **kwargs )
        self._addToCatalog( subGroup.catalog )
        return subGroup

    @property
    def fileList(self):
        fileList = []
        for filesGlob in self._files:
            fileList.extend( glob( filesGlob ) )
        return fileList

    def addDataSources( self, **kwargs ):
        cdim = kwargs.get( "concat_dim" )
        if cdim:
            datasource: DataSource = self.getAggregationDataSource( cdim, **kwargs )
            self._addToCatalog( datasource )
        else:
            for file in self.fileList:
                datasource: DataSource = self.getFileDataSource( file, **kwargs )
                self._addToCatalog( datasource )
        self._catalog.save( self.getURI( **kwargs ) )

    def getFileDataSource(self, filePath: str, **kwargs ) -> DataSource:
        dataSource = intake.open_netcdf( filePath )
        if dataSource is not None: self.initDataSource( dataSource, **kwargs )
        return dataSource

    def getAggregationDataSource(self, concat_dim: str, **kwargs) -> DataSource:
        dataSource = intake.open_netcdf(self.fileList, concat_dim=concat_dim)
        if dataSource is not None: self.initDataSource( dataSource, **kwargs )
        return dataSource

    def initDataSource(self, dataSource: DataSource, **kwargs ):
        dataSource.discover()
        attrs = kwargs.get("attrs", {})
        for key, value in attrs.items(): self.setSourceAttr( dataSource, key, value)
        dataSource.name = self.name
        dataSource.metadata = str_dict( dataSource.metadata )

    def setSourceAttr( self, dataSource: DataSource, key: str, value: str):
        attr_value = dataSource.metadata.get(value[1:], "") if value.startswith('@') else value
        setattr( dataSource, key, str(attr_value) )