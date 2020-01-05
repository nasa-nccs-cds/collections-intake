from glob import glob
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.collection import DataCollectionGenerator
import os, intake
from os import path

print( f"Intake drivers: {list(intake.registry)}" )

collection_name = "cip_merra2_mon"
collection_root = "/nfs4m/css/curated01/create-ip/data/reanalysis"

base_cat: CatalogNode = CatalogNode.getCatalogBase()
cReanalysis = base_cat.addCatalogNode( "reanalysis", description="NCCS Reanalysis collections" )
cCip = cReanalysis.addCatalogNode( "createIP", description="Reprocessed reanalyses for CreateIP" )

ecmwf_file_path1 = "ECMWF/IFS-Cy31r2/ERA-Interim"
cECMWF = cCip.addCatalogNodes( ecmwf_file_path1  )
for dset_dir in glob( f"{collection_root}/{ecmwf_file_path1}/*" ):
    dset_name = path.basename(dset_dir)
    print( f"Adding dataset {dset_name} for path {dset_dir}")
    dset_node: DataCollectionGenerator = cECMWF.addDataCollection( dset_name  )
    for col_path in glob( "dset_dir/*" ):
        col_name  = path.basename(col_path)
        for agg_path in glob( "dset_dir/*/*" ):
            agg_name  = f"{col_path}-{path.basename(agg_path)}"
            dset_node.addAggregation( agg_name, f"{agg_path}/*.nc", driver="netcdf", concat_dim="time", chunks={})


