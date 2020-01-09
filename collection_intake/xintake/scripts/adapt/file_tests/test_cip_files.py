import os, xarray, time, math
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from collection_intake.util.fileTest import FileTester

base_dirs = "/nfs4m/css/curated01/create-ip/data/reanalysis/*"
suffix = ".nc"

fileTester = FileTester( "aviris", suffix )

fileTester.search( base_dirs )


