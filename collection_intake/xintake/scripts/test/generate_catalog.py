import intake
from intake_xarray.netcdf import NetCDFSource

print( list(intake.registry) )

cat_source: NetCDFSource = intake.open_netcdf( '/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA2/6hr/*.nc4', concat_dim="time" )
cat_source.discover()

with open( "./catalog_local.yaml", 'w' ) as f:
    f.write( cat_source.yaml() )
