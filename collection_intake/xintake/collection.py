import intake, os, pprint
from intake.catalog.local import YAMLFileCatalog, YAMLFilesCatalog, Catalog
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from collection_intake.xintake.aggregation import Aggregation
from collection_intake.xintake.base import Grouping, pp, str_dict
from intake.source.base import DataSource
from intake import open_catalog
import xarray as xa
from glob import glob

class Collection(Grouping):

    def __init__( self, name: str = "root", **kwargs ):
        Grouping.__init__( self, name, **kwargs )

    def generate(self, **kwargs ):
        cat_nodes = kwargs.pop("cat_nodes", [] if self.name == "root" else [ self.name ] )
        catalog_file = Grouping.getCatalogFilePath( cat_nodes, **kwargs )
        cat_items = kwargs.get( 'cats' )
        if not cat_items:
            cdir = os.path.dirname( catalog_file )
            cat_items = glob(f"{cdir}/*/catalog.yaml")
        print( f"Opening collection {self.name} with items:\n" ); pp( cat_items )
        catalogs: YAMLFilesCatalog = intake.open_catalog( cat_items, driver="yaml_files_cat" )
        catalogs.name = self.name
        catalogs.description = self.description
        catalogs.metadata = str_dict(self.metadata)

        with open( catalog_file, 'w' ) as f:
            yaml =  catalogs.yaml()
            print( f"\nWriting collection {self.name} to {catalog_file}")
            f.write( yaml )
        return catalog_file

    def getCatalog(self, **kwargs ) -> YAMLFileCatalog:
        if self.catalog is None:
            cat_nodes = kwargs.pop("cat_nodes", [] if self.name == "root" else [self.name])
            cdir = Grouping.getCatalogFilePath( cat_nodes, **kwargs )
            cat_file = os.path.join( cdir, "catalog.json")
            print( f"Opening collection from file {cat_file}" )
            self.catalog = intake.open_catalog( cat_file, driver="yaml_file_cat")
            self.catalog.discover()
        return self.catalog

    def open( self, **kwargs ) -> xa.Dataset:
        data_source: YAMLFileCatalog = self.getCatalog( **kwargs )
        agg = kwargs.get( "agg", self.name )
        ds: xa.Dataset = data_source.__getattr__(self.name).__getattr__(agg).to_dask()
        return ds

class SourceCollection(Grouping):

    def __init__( self, name: str = "root", **kwargs ):
        Grouping.__init__( self, name, **kwargs )

    def generate(self, aggs: Dict[str,DataSource], **kwargs ):
        cat_nodes = kwargs.pop("cat_nodes", [])
        catalog_file = Grouping.getCatalogFilePath( cat_nodes, **kwargs )
        print( f"Opening collection {self.name} with aggs:\n" ); pp( aggs.keys() )
        catalog: Catalog = Catalog.from_dict( aggs, name=self.name, description=self.description, metadata=str_dict(self.metadata)  )

        with open( catalog_file, 'w' ) as f:
            yaml =  catalog.yaml()
            print( f"\nWriting collection {self.name} to {catalog_file}")
            f.write( yaml )

    def getCatalog(self, **kwargs ) -> YAMLFileCatalog:
        if self.catalog is None:
            cat_nodes = kwargs.pop("cat_nodes", [])
            cdir = Grouping.getCatalogFilePath( cat_nodes, **kwargs )
            cat_file = os.path.join( cdir, "catalog.json")
            print( f"Opening collection from file {cat_file}" )
            self.catalog = intake.open_catalog( cat_file, driver="yaml_file_cat")
            self.catalog.discover()
        return self.catalog

    def open( self, **kwargs ) -> xa.Dataset:
        data_source: YAMLFileCatalog = self.getCatalog( **kwargs )
        agg = kwargs.get( "agg", self.name )
        ds: xa.Dataset = data_source.__getattr__(self.name).__getattr__(agg).to_dask()
        return ds

