import intake, os
from intake.catalog.local import YAMLFileCatalog
from collection_intake.xintake.base import Grouping
from intake import open_catalog
import xarray as xa
from glob import glob

class Collection(Grouping):

    def __init__( self, name: str = "root", **kwargs ):
        Grouping.__init__( self, name, **kwargs )

    def generate(self, **kwargs ):
        cdir = self.getCatalogFilePath(**kwargs)
        catalog_file = os.path.join(cdir, "catalog.json")
        cat_items = kwargs.get( 'cats' )
        if cat_items is None:
            cat_items = glob(f"{cdir}/*/catalog.yaml")
        print( f"Opening collection {self.name} with items: {cat_items}")
        collection = open_catalog( cat_items )
        collection.name = self.name
        collection.description = self.description
        collection.metadata = self.metadata

        with open( catalog_file, 'w' ) as f:
            yaml =  collection.yaml()
            print( f"\nWriting collection {self.name} to {catalog_file}")
            f.write( yaml )

    def getCatalog(self, **kwargs ) -> YAMLFileCatalog:
        if self._catalog is None:
            cdir = self.getCatalogFilePath( **kwargs )
            cat_file = os.path.join( cdir, "catalog.json")
            print( f"Opening collection from file {cat_file}" )
            self._catalog = intake.open_catalog( cat_file, driver="yaml_file_cat")
            self._catalog.discover()
        return self._catalog

    def open( self, **kwargs ) -> xa.Dataset:
        data_source: YAMLFileCatalog = self.getCatalog( **kwargs )
        agg = kwargs.get( "agg", self.name )
        ds: xa.Dataset = data_source.__getattr__(self.name).__getattr__(agg).to_dask()
        return ds

