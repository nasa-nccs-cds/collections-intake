import intake, os
from collection_intake.xintake.collection import Collection
import xarray as xa
from glob import glob

collection = Collection( "merra2" )
collection.generate( )
