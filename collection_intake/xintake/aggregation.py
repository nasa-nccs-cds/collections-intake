import intake, os, pprint
from intake_xarray.netcdf import NetCDFSource
from intake.config import conf as iconf
from intake.catalog.local import YAMLFileCatalog
from collection_intake.xintake.base import Grouping, pp, str_dict
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import xarray as xa
from intake.source.base import DataSource

class Aggregation(Grouping):

    def __init__( self, name: str, **kwargs ):
        Grouping.__init__( self, name, **kwargs )
        self.files = kwargs.get( "files", None )
        self.collection = kwargs.get( "collection", self.name )
        self.dataSource: Optional[DataSource] = None
        self.openDataSource( **kwargs )

    def printMetadata(self, **kwargs):
        self.openDataSource( **kwargs )
        pp( self.dataSource.metadata )

    def getDataSource(self, **kwargs ) -> DataSource:
        cdim = kwargs.get("concat_dim", "time")
        datasource = intake.open_netcdf( self.files, concat_dim=cdim )
        datasource.discover()
        return datasource

    def openDataSource( self, **kwargs ):
        if (self.dataSource is None) and kwargs.get( 'open', True ):
            self.dataSource: DataSource = self.getDataSource( **kwargs )
            if self.dataSource is not None:
                attrs = kwargs.get("attrs",{})
                for key,value in attrs.items(): self.setSourceAttr( key, value  )
                self.dataSource.name = f"{kwargs.get('name', self.name)}"
                self.dataSource.metadata = str_dict( self.dataSource.metadata )

    def getMetadata(self) -> Dict:
        return self.dataSource.metadata

    def setSourceAttr( self, key: str, value: str):
        attr_value = self.dataSource.metadata.get(value[1:], "") if value.startswith('@') else value
        setattr( self.dataSource, key, str(attr_value) )

    def getCatalogFilePath( self, **kwargs ):
        return Grouping.getCatalogFilePath( self, collection=self.collection, **kwargs )

    def writeCatalogFile(self, **kwargs) -> Optional[str]:
        self.openDataSource( **kwargs )
        if self.dataSource is None: return None
        catalog_file = self.getCatalogFilePath( **kwargs )

        with open( catalog_file, 'w' ) as f:
            yaml =  self.dataSource.yaml()
            print( f"Writing aggregation {self.name} to {catalog_file}")
            f.write( yaml )
        return catalog_file

    def openFromCatalog(self, **kwargs) -> xa.Dataset:
        if self.catalog is None:
            cat_file = self.getCatalogFilePath( **kwargs )
            print( f"Opening aggregation from file {cat_file}" )
            self.catalog: YAMLFileCatalog = intake.open_catalog( cat_file, driver="yaml_file_cat")
            self.catalog.discover()
        ds: xa.Dataset = getattr( self.catalog, self.name ).to_dask()
        return ds

    def close(self):
        Grouping.close(self)
        if self.dataSource: self.dataSource.close()

if __name__ == "__main__":
    files_glob = '/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA2/6hr/*.nc4'
    agg = Aggregation( "merra2-6hr", collection="merra2", files=files_glob )
    agg.writeCatalogFile()

    agg1 =  Aggregation( "merra2-6hr", collection="merra2" )
    ds = agg1.openFromCatalog()
    print(f"variable QV: dims: {ds.QV.dims}, shape: {ds.QV.shape}, chunks = {ds.QV.chunks}")