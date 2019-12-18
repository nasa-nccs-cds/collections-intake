from collection_intake.xintake.aggregation import Aggregation
import xarray as xa

if __name__ == "__main__":
    files_glob = '/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA2/6hr/*.nc4'
    agg = Aggregation( "merra2-6hr", collection="merra2", files=files_glob )
    agg.generate()

    agg1 =  Aggregation( "merra2-6hr", collection="merra2" )
    ds: xa.Dataset = agg1.open()
    print(f"variable QV: dims: {ds.QV.dims}, shape: {ds.QV.shape}, chunks = {ds.QV.chunks}")