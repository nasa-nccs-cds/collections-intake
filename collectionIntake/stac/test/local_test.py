from collectionIntake.stac.scan import FileScanner

dataPath = "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA2/6hr/"
cid = "merra2-6hr"

scanner = FileScanner( cid, path=dataPath, ext="nc4", mp="False" )
scanner.writeSTAC()