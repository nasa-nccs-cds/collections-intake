import xarray as xa
aviris_file = "/att/pubrepo/ABoVE/archived_data/ORNL/ABoVE_Airborne_AVIRIS_NG/data/ang20180819t010027/ang20180819t010027_rdn_v2r2/ang20180819t010027_rdn_v2r2_img"
result = xa.open_rasterio( aviris_file )
print( result.shape )