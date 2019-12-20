import intake, os, pprint
from intake_xarray.netcdf import NetCDFSource
from intake.catalog.local import YAMLFileCatalog
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import xarray as xa
from intake.source.base import DataSource
pp = pprint.PrettyPrinter(depth=4).pprint

class Aggregation:

    def __init__( self, name: str, **kwargs ):
        self.name = name
        self.files = kwargs.get( "files", None )
        self.collection = kwargs.get( "collection", self.name )
        self.dataSource: Optional[DataSource] = None
        self.catalog: Optional[YAMLFileCatalog] = None
        self.openDataSource( **kwargs )

    def printMetadata(self, **kwargs):
        self.openDataSource( **kwargs )
        pp( self.dataSource.metadata )

    def getCatalogFilePath( self, **kwargs ):
        agg_dir = os.path.join( kwargs.get( "path", self.getCatalogsPath() ), self.collection, self.name )
        agg_catalog_file = os.path.join( agg_dir, "catalog.yaml")
        os.makedirs( agg_dir, exist_ok=True )
        return agg_catalog_file

    def openDataSource( self, **kwargs ):
        if (self.dataSource is None) and kwargs.get( 'open', True ):
            cdim = kwargs.get( "concat_dim", "time")
            self.dataSource: NetCDFSource = intake.open_netcdf( self.files, concat_dim=cdim )
            self.dataSource.discover()
            attrs = kwargs.get("attrs",{})
            for key,value in attrs.items(): self.setSourceAttr( key, value  )
            self.dataSource.name = kwargs.get( 'name', self.name )

    def setSourceAttr( self, key: str, value: str):
        attr_value = self.dataSource.metadata.get(value[1:], "") if value.startswith('@') else value
        setattr( self.dataSource, key, attr_value)

    def writeCatalogFile(self, **kwargs):
        self.openDataSource( **kwargs )
        catalog_file = self.getCatalogFilePath( **kwargs )

        with open( catalog_file, 'w' ) as f:
            yaml =  self.dataSource.yaml()
            print( f"\nWriting aggregation {self.name} to {catalog_file}:\n\n{yaml}")
            f.write( yaml )

    def openFromCatalog(self, **kwargs) -> xa.Dataset:
        if self.catalog is None:
            cat_file = self.getCatalogFilePath( **kwargs )
            print( f"Opening aggregation from file {cat_file}" )
            self.catalog: YAMLFileCatalog = intake.open_catalog( cat_file, driver="yaml_file_cat")
            self.catalog.discover()
        ds: xa.Dataset = getattr( self.catalog, self.name ).to_dask()
        return ds

    def getCatalogsPath( self ):
        ilDataDir = os.environ.get('ILDATA')
        assert ilDataDir is not None, "Must set the ILDATA environment variable to define the data directory"
        return os.path.join(ilDataDir, "collections" )

    def close(self):
        if self.dataSource: self.dataSource.close()
        if self.catalog:    self.catalog.close()

    def __del__(self):
        self.close()

if __name__ == "__main__":
    files_glob = '/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA2/6hr/*.nc4'
    agg = Aggregation( "merra2-6hr", collection="merra2", files=files_glob )
    agg.writeCatalogFile()

    agg1 =  Aggregation( "merra2-6hr", collection="merra2" )
    ds = agg1.openFromCatalog()
    print(f"variable QV: dims: {ds.QV.dims}, shape: {ds.QV.shape}, chunks = {ds.QV.chunks}")