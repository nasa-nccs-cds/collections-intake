import intake, os, pprint, warnings, traceback
from intake.config import conf as iconf
from intake.catalog.local import YAMLFilesCatalog, Catalog
from collection_intake.xintake.base import IntakeNode, pp, str_dict
from intake.source.base import DataSource
from glob import glob
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional, Union, Callable

def globs( fglobs: Union[str,List[str]] ) -> List[str]:
    fglobList = fglobs if isinstance(fglobs, list) else [fglobs]
    fileList = []
    for filesGlob in fglobList:
        fileList.extend( glob( filesGlob ) )
    return fileList

def summary( fileList: Union[str,List[str]] ) -> str:
   return f"{fileList[0]}...{fileList[-1]}" if (fileList and isinstance(fileList, list)) else str(fileList)

def isList(  fileList: Union[str,List[str]] ) -> bool:
    return len( globs( fileList ) ) > 1

def get_source_name( collection: str, index: int, fname: str ):
    return f"{collection}-{index}"

class DataCollectionGenerator(IntakeNode):

    def __init__( self, path_nodes: List[str], **kwargs ):
        IntakeNode.__init__( self, path_nodes, **kwargs )

    @property
    def sourcesDir(self) -> str:
        sdir = os.path.join(self.catDir, "sources" )
        os.makedirs( sdir, exist_ok=True )
        return sdir

    @property
    def sourcesUri(self) -> str:
        return os.path.join( self.sourcesDir, "*.yaml" )

    def sourceUri(self, name: str ) -> str:
        return os.path.join( self.sourcesDir, f"{name}.yaml" )

    def _initializeCatalog(self, **kwargs):
        self._sources_catalog = intake.open_catalog( self.sourcesDir, driver="yaml_files_cat", name=self.name, **kwargs )
        super(DataCollectionGenerator, self)._initializeCatalog(**kwargs)

    def addAggregation( self, name: str, fileList: Union[str,List[str]], **kwargs ):
        try:
            do_save = kwargs.pop( 'save', True )
            print(f"Adding Data Aggregation to catalog {self.name}:{name} ->  {summary(fileList)}")
            self._createDataSource( name, fileList, **kwargs )
            if do_save: self.save()
        except Exception as err:
            print( f" ** Skipped loading the data file(s) {fileList}:\n     --> Due to Error: {err}:\n{traceback.format_exc()}")

    def addAggregations(self, sources: Dict[str,List], **kwargs):
        for (source_name, fileGlobs) in sources.items():
            self.addAggregation( source_name, fileGlobs, save = False, **kwargs )
        self.save()

    def addFileCollection( self, fileGlobs: Union[str,List[str]], **kwargs ):
        fileList = globs(fileGlobs)
        do_save = kwargs.pop('save', True)
        get_name:  Callable[[str,int,str],str] = kwargs.pop('get_name', get_source_name )
        for iFile, filePath in enumerate(fileList):
            try:
                source_name = get_name(self.name, iFile, filePath)
                if isList(filePath): print(f"Adding source collection to catalog {source_name} -> {summary(filePath)}")
                else:                print(f"Adding source file to catalog {source_name} -> {filePath}")
                self._createDataSource( source_name, filePath, **kwargs )
            except Exception as err:
                print( f" ** Skipped loading the data file {filePath}:\n     --> Due to Error: {err} ")
        if do_save: self.save()

    def _createDataSource(self, name: str, files: Union[str,List[str]], **kwargs) -> DataSource:
        from intake.source import registry
        driver = kwargs.pop("driver", "netcdf")
        dataSource = registry[ driver ]( files,  **kwargs )
        dataSource.name = name
        dataSource.discover()
        attrs = kwargs.get( "attrs", {} )
        for key, value in attrs.items(): self.setSourceAttr( dataSource, key, value)
        dataSource.metadata = str_dict( dataSource.metadata )
        source_file_uri = self.sourceUri( dataSource.name )
        dataSource.metadata['cat_file'] = source_file_uri
        with open(source_file_uri, 'w') as f:
            yaml = dataSource.yaml()
            print(f"Writing dataSource {dataSource.name} to {source_file_uri}")
            f.write(yaml)
        return dataSource

    def save( self, **kwargs ):
        self._sources_catalog.force_reload()
        catUri = kwargs.get( 'catalog_uri', self.catURI )
        print( f"    %%%% -->  Catalog {self.name} saving to: {catUri}")
        self._sources_catalog.save(catUri)
        self.patch_yaml( catUri )

