import intake
files_path = "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/2005/JAN/*.nc"
datasource = intake.open_netcdf( files_path )