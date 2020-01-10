from glob import glob
from collection_intake.xintake.catalog import CatalogNode
from collection_intake.xintake.collection import DataCollectionGenerator
import os, intake, xarray

collection_specs0 = [
    { "cip_merra2": "create-ip/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/*" },
    { "cip_merra2_asm": "create-ip/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/*" },
    { "merra2": "/merra2/data/*" }
]

collection_specs1 = [
    { "noaa_ncep_cfsr": "create-ip/data/reanalysis/NOAA-NCEP/CFSR/*" },
    { "noaa_ncep_mom3": "create-ip/data/reanalysis/NOAA-NCEP/MOM3/*" },
    { "noaa_gfdl_mom4": "create-ip/data/reanalysis/NOAA-GFDL/MOM4/ECDAv31/*"},
    { "noaa_esrl+cires": "create-ip/data/reanalysis/NOAA-ESRLandCIRES/ensda-v351/20CRv2c/*"},
    { "ecmwf_ifs_erai": "create-ip/data/reanalysis/ECMWF/IFS-Cy31r2/ERA-Interim/*"},
    { "ecmwf_ifs_era5": "create-ip/data/reanalysis/ECMWF/IFS-Cy41r2/ERA5/*"},
    { "ecmwf_ifs_cera": "create-ip/data/reanalysis/ECMWF/IFS-Cy41r2/CERA-20C/*"},
    { "ecmwf_nemo_oras4":    "create-ip/data/reanalysis/ECMWF/NEMOv3/ORAS4/*"},
    { "ecmwf_nemo_orap5":    "create-ip/data/reanalysis/ECMWF/NEMOv34+LIM2/ORAP5/*"},
    { "jma_jra35":      "create-ip/data/reanalysis/JMA/JRA-25/*"},
    { "jma_jra55":      "create-ip/data/reanalysis/JMA/JRA-55/*"},
    { "jma_jra55_mdl_iso": "create-ip/data/reanalysis/JMA/JRA-55-mdl-iso/*"},
    { "cmcc_nemo+lim2": "create-ip/data/reanalysis/CMCC/NEMOv32+LIM2/C-GLORSv5/*"},
    { "iap-ua_ccsm-cam_era40": "create-ip/data/reanalysis/IAP-UA/CCSM-CAM/ERA40-CRUTS3-10/*"},
    { "iap-ua_ccsm_era40": "create-ip/data/reanalysis/IAP-UA/CCSM-CAM/ERA40-CRUTS3-10/*"},
    { "iap-ua_ncep-gom_cruts3": "create-ip/data/reanalysis/IAP-UA/NCEP-Global-Operational-Model/NCEP-NCAR-CRUTS3-10/*"},
    { "uh-mitgcm_gecco2": "create-ip/data/reanalysis/University-Hamburg/MITgcm/GECCO2/*"}
]

collection_specs2 = [
    { "merra2": "/merra2/data/*" }
]

base_dirs = glob("/nfs4m/css/curated01/merra2/data/*")
errors_file = f"/tmp/bad_files-merra2.csv"
suffix = ".nc4"

lines = []
def test_files( base_dir: str ):
    print( f"Walking file system from {base_dir}")
    for root, dirs, files in os.walk( base_dir ):
        if len(files) > 0:
           for fname in files:
               if fname.endswith(suffix):
                   file_path = os.path.join(root, fname)
                   try:
                       ds = xarray.open_dataset(file_path)
                   except Exception as err:
                       print(f"{err}")
                       lines.append( f"{err}" )

collection_name = "cip_merra2_mon"
collection_root = "/nfs4m/css/curated01/create-ip/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/mon"
agg_dirs = glob( f"{collection_root}/*/*" )

base_cat: CatalogNode = CatalogNode.getCatalogBase()
reanalysis_cat = base_cat.addCatalogNode( "reanalysis", description="NCCS Reanalysis collections" )
MERRA_cat = reanalysis_cat.addCatalogNode( "MERRA", description="MERRA collections" )
cip_merra2_mon: DataCollectionGenerator = MERRA_cat.addDataCollection( "cip_merra2_mon", description="MERRA2 monthly means reprocessed for CreateIP" )

for agg_dir in agg_dirs:
    print( f"Adding aggregation data source to collection cip_merra2_mon for data path {agg_dir}")
    dsname = os.path.basename(agg_dir)
    cip_merra2_mon.addAggregation( dsname, f"{agg_dir}/*.nc", driver="netcdf", concat_dim="time", chunks={} )

