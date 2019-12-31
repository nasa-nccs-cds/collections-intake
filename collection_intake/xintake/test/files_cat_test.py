import intake, os
from intake.catalog.local import YAMLFilesCatalog
from typing import Dict

cat_items = "/att/pubrepo/ILAB/data/collections/intake/reanalysis/MERRA/cip_merra2_mon/*.yaml"
catalogs: YAMLFilesCatalog = intake.open_catalog( cat_items, driver="yaml_files_cat" )
catalogs.name = 'cip_merra2_mon'
catalogs.metadata = dict( description="MERRA2 monthly means reprocessed for CreateIP" )
catalogs.save( "/tmp/files_cat_test.yaml")