from glob import glob
from collection_intake.xintake.base import Grouping, pp, str_dict
import os, intake
print( f"Intake drivers: {list(intake.registry)}" )

collection_name = "cip_merra2_mon"
collection_root = "/nfs4m/css/curated01/create-ip/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/mon"
agg_dirs = glob( f"{collection_root}/*/*" )

base_cat = Grouping.getCatalogBase()
reanalysis_cat = base_cat.addSubGroup( "reanalysis", description="NCCS Reanalysis collections" )
MERRA_cat = reanalysis_cat.addSubGroup( "MERRA", description="MERRA collections" )
cip_merra2_mon_cat = MERRA_cat.addSubGroup( "cip_merra2_mon", description="MERRA2 monthly means reprocessed for CreateIP" )

for agg_dir in agg_dirs:
    print( f"Adding cip_merra2_mon aggregation for data path {agg_dir}")
    cip_merra2_mon_cat.addDataSources( f"{agg_dir}/*.nc", driver="netcdf", concat_dim="time" )

