import xarray as xa
from collection_intake.xintake.base import Grouping, pp, str_dict

aviris_file = "/att/pubrepo/ABoVE/archived_data/ORNL/ABoVE_Airborne_AVIRIS_NG//data/ang20180815t222106/ang20180815t222106_rdn_v2r2/ang20180815t222106_rdn_v2r2_img"
result: xa.DataArray = xa.open_rasterio( aviris_file )
print( f"shape = {result.shape}" )
print( f"dims = {result.dims}" )
print( f"attrs: " )
pp(result.attrs)