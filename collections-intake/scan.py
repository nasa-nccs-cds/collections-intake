from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from netCDF4 import Dataset, num2date, Variable
from functools import total_ordering
import os, glob, yaml, datetime, argparse, math, time
from satstac import Item, Collection
import multiprocessing as mp
from multiprocessing import Pool

def m2s( params: Dict ) -> str:
    return ",".join( [ f"{key}:{value}" for key,value in params.items() ] )

@total_ordering
class FileRec:
    def __init__(self, path ):
        self.path = path
        dataset = Dataset(path)
        self.units = "minutes since 1970-01-01T00:00:00Z"
        self.relPath = None
        self.base_date = datetime.datetime(1970,1,1,1,1,1)
        self.calendar = dataset.calendar if hasattr(dataset, 'calendar') else "standard"
        self.vars_list = list(dataset.variables.keys())
        self.vars_list.sort()
        self.varsKey = ",".join(self.vars_list)
        time_var: Variable = dataset.variables["time"]
        time_data = time_var[:]
        if len(time_data) > 1:
            dt = time_data[1] - time_data[0]
            self.start_date = num2date(time_data[0], time_var.units, self.calendar)
            self.end_date = num2date(time_data[-1] + dt, time_var.units, self.calendar)
        else:
            self.start_date = num2date(time_data[0], time_var.units, self.calendar)
            self.end_date = self.start_date
        self.start_time_value = self.getTimeValue( self.start_date )
        self.end_time_value = self.getTimeValue( self.end_date )
        self.size = len(time_data)

    def getTimeValue( self, date ) -> int:
        offset = date - self.base_date
        return offset.days * 24 * 60 + int(round(offset.seconds/60))

    def setBase(self, base: str ):
        self.relPath = self.path[len(base):]

    def toSTAC(self) -> Item:
        metadata = dict( id = os.path.basename(self.path),
                         time_range = [ self.start_time_value, self.end_date ],
                         date_range=[self.start_date, self.end_time_value ],
                         n_time_steps = self.size,
                         time_units = self.units,
                         variables = self.vars_list,
                         calendar = self.calendar,
                         base_date = self.base_date )
        item =  Item( data=metadata, filename = self.path )
        return item

    def __eq__(self, other: "FileRec") -> bool:
        return self.start_time_value == other.start_time_value

    def __ne__(self, other: "FileRec") -> bool:
        return not (self.start_time_value == other.start_time_value)

    def __lt__(self, other: "FileRec"):
        return self.start_time_value < other.start_time_value

    def __str__(self):
        return  f"fREC -[{self.start_date}]-  -[{self.end_date}]-  {self.path} "

    @staticmethod
    def time( frec: "FileRec" ):
        return frec.start_time_value


class  FileScanner:

    def __init__(self, collectionId: str, **kwargs ):
        self.aggs = {}
        self.basePath = None
        self.varPaths = {}
        self.collectionId = collectionId
        self.collectionDir = self.createCollectionDir( collectionId )
        print( f"Running FileScanner with args: {kwargs}")
        self.scan( **kwargs )

    def createCollectionDir(self, collectionId: str ):
        ilDataDir = os.environ.get('ILDATA')
        assert ilDataDir is not None, "Must set the ILDATA environment variable to define the data directory"
        collectionDir = os.path.join( ilDataDir, "collections", collectionId )
        os.makedirs( collectionDir, exist_ok=True )
        return collectionDir

    def __str__(self):
        aggs = [ f"---> {varId}:\n{agg}" for varId,agg in self.aggs.items() ]
        return "\n".join( aggs )

    def toSTAC(self, **kwargs ) -> Collection:
        collection = Collection.create( id = self.collectionId, root='https://hpda.{self.collectionId}', **kwargs )
        collection_root_path = os.path.join( self.collectionDir, "root.json" )
        collection.save( collection_root_path )
        for agg in self.aggs.values():
            collection.add_collection( agg.toSTAC() )
            for fileRec in collection.fileRecs:
                collection.add_item( fileRec.toSTAC() )
        return collection

    def processPaths(self, paths: List[str], **kwargs ):
        nproc = 2*mp.cpu_count()
        par = kwargs.get("mp","t").lower().startswith("t")
        t0 = time.time()
        chunksize = math.ceil( len(paths) / nproc )
        if par:
            print( f"Processing {len(paths)} p[aths with {nproc} processes.")
            with Pool(processes=nproc) as pool:
                frecList = pool.map(FileRec, paths, chunksize)
        else:
            frecList = [ FileRec(path) for path in paths ]

        t1 = time.time()
        print(f" Completed file scan in {t1 - t0} seconds")
        for frec in frecList:
            self.varPaths.setdefault(frec.varsKey, []).append(frec)
        for varKey, frecList in self.varPaths.items():
            frecList.sort()
            base = os.path.commonprefix([os.path.dirname(frec.path) for frec in frecList])
            size = 0
            for frec in frecList:
                size += frec.size
                frec.setBase(base)
            agg = Aggregation(base, frecList, size)
            self.aggs[varKey] = agg
        print(" Completed generating aggregations in " + str(time.time() - t1) + " seconds")

    def scan( self, **kwargs ):
        glob1 =  kwargs.get( "glob" )
        globsArg =  kwargs.get( "globs" )
        globs: List[str] = [] if globsArg is None else globsArg.split(",")
        if glob1 is not None: globs.append(glob1)
        path = kwargs.get("path",None)
        if path is not None:
            self.basePath = path
            ext = kwargs.get("ext", None)
            if ext[0] == ".": ext = ext[1:]
            glob2 = path + "/**" if ext is None else path + "/**/*." + ext
            globs.append(glob2)
        if len(globs) > 0:
            print( "Scanning globs:" + str(globs) )
            paths = glob.glob( *globs, recursive=True)
            self.basePath = os.path.dirname(os.path.commonprefix(paths))
            self.processPaths( paths, **kwargs )
        else: raise Exception( "No files found")

    def write( self, collectionsDir: str ):
        baseDir = os.path.expanduser(collectionsDir)
        os.makedirs( baseDir, exist_ok=True )
        collectionsFile = f"{baseDir}/{self.collectionId}.csv"
        print(" Writing collection file: " + collectionsFile )
        lines = []
        with open( collectionsFile, 'w' ) as f:
            for aggId,agg in self.aggs.items():
                aggFile = f"{baseDir}/{agg.getId(self.collectionId)}.ag1"
                print(" Writing agg File: " + aggFile)
                agg.write( aggFile )
                lines.append(f"# title, {self.collectionId}\n")
                lines.append(f"# dir, {baseDir}\n")
                lines.append(f"# format, ag1\n")
                for var in agg.vars:
                    relPath = os.path.splitext( os.path.relpath(aggFile, baseDir) )[0]
                    lines.append( f"{var}, {relPath}\n")
            f.writelines(lines)

    def writeSTAC(self):
        collection_file = os.path.join( self.collectionDir, f"{self.collectionId}.json" )
        self.toSTAC().save( collection_file )

class Aggregation:

    def __init__(self, base: str, frecList: List[FileRec], nTs: int ):

        self.nFiles = len(frecList)
        self.fileRecs = frecList
        self.base = base
        self.nTs = nTs
        self.vars = set()
        self.nc_dims = None
        self.nc_coords = None
        self.paths = self.partition()
        self.lines = self.process()

    def toSTAC(self) -> Collection:
        dataset = Dataset(self.filePath())
        dims = { name: dim.size for name, dim in dataset.dimensions.items() }
        metadata = dict( id = os.path.dirname(self.base),
                         description="",
                         extent="",
                         properties=dict( nFiles = self.nFiles, nTs= self.nTs, paths=self.paths, vars=self.vars, dims=dims ),
                         )
        collection = Collection( data=metadata, filename = self.base )
        return collection

    def varKey(self):
        return "-".join(list(self.vars)[:4])

    def getId(self, collectionId: str ):
        return collectionId + "--" + self.varKey()

    def __str__(self):
        return f"FileScanner[{self.nFiles}]: \n\tbase = {self.base}\n\tpaths = {self.paths}\n\t"

    def partition( self ):
        paths = {}
        for frec in self.fileRecs:
            basePath = os.path.dirname(frec.relPath)
            fileList = paths.setdefault(basePath,[])
            fileList.append( frec.relPath[len(basePath):].strip("/") )
        return paths

    def filePath(self, index: int = 0 ) -> str:
        return f"{self.base}/{self.fileRecs[index].relPath}"

    def yaml(self):
        collection = {}
        dataset = Dataset( self.filePath() )
        dims = { name: dim.size for name, dim in dataset.dimensions.items() }
        vars = {name: [ dim.name for dim in var.get_dims() ] for name, var in dataset.variables.items()}
        collection["files"] = self.paths
        collection["dimensions"] = dims
        collection["variables"] = vars
        collection["parameters"] =  dict( basePath=self.base, nFiles=self.nFiles )
        return yaml.dump(collection)

    def write(self, filePath: str ):
        with open( filePath, 'w' ) as f:
            f.writelines( self.lines )

    @staticmethod
    def attr( ncobj, aname: str, default: str = "" ) -> str:
        return ncobj.getncattr(aname) if aname in ncobj.ncattrs() else default

    def process(self):
        dataset: Dataset = Dataset(self.filePath())
        calendar: str = self.attr( dataset, "calendar", "standard" )
        self.nc_dims: List[str] = [dim for dim in dataset.dimensions]
        nc_vars: List[str] = [var for var in dataset.variables]
        lines: List[str] = []
        resolution = dict()
        lines.append(f'P; base.path; {self.base}\n')
        lines.append(f'P; num.files; {self.nFiles}\n')
        lines.append(f'P; time.nrows; {self.nTs}\n')
        lines.append(f'P; time.start; {self.fileRecs[0].start_time_value}\n')
        lines.append(f'P; time.end; {self.fileRecs[-1].end_time_value}\n')
        lines.append(f'P; time.calendar; {calendar}\n')
        self.nc_coords = { vname: dataset.variables[vname] for vname in self.nc_dims }
        for attr_name in dataset.ncattrs():
            lines.append(f'P; {attr_name}; {dataset.getncattr(attr_name)}\n')
        for vname, coord in self.nc_coords.items():
            units = self.attr(coord, "units")
            lvname = vname.lower()
            if lvname == "time":
                start_value = self.fileRecs[0].start_time_value
                end_value = self.fileRecs[-1].end_time_value
                lines.append(f'C; {vname}; {self.nTs}\n')
                lines.append(f'A; {vname}; {vname}; T; {str(self.nTs)}; minutes since 1970-01-01T00:00:00Z; {start_value}; {end_value}\n')
                resolution[lvname] = 1.0 if coord.shape[0] < 2 else abs((end_value - start_value)/(self.nTs-1))
            else:
                lines.append(f'C; {vname}; {coord.shape[0]}\n')
                cdata = coord[:]
                ctype = "?"
                shape = [ str(s) for s in coord.shape ]
                resolution[lvname] = 1.0 if coord.shape[0] < 2 else abs( (cdata[-1]-cdata[0])/(coord.shape[0]-1) )
                if   lvname.startswith("lat"): ctype = "Y"
                elif lvname.startswith("lon"): ctype = "X"
                elif lvname.startswith("lev") or lvname.startswith("plev"): ctype = "Z"
                lines.append(f'A; {vname}; {vname}; {ctype}; {",".join(shape)}; {units}; {cdata[0]}; {cdata[-1]}\n')
        for vname in nc_vars:
            if vname not in self.nc_dims:
                self.vars.add(vname)
                var: Variable = dataset.variables[vname]
                long_name = self.attr( var, "long_name", vname )
                comments = self.attr(var, "comments" )
                units = self.attr(var, "units")
                dims = var.dimensions
                shape = [ str(self.nTs) if dims[iDim] == "time" else str(var.shape[iDim]) for iDim in range(len(dims)) ]
                lines.append(f'V; {vname}; {long_name}; {long_name}; {comments}; {",".join(shape)}; {m2s(resolution)}; {" ".join(dims)}; {units}\n')
        for frec in self.fileRecs:
            lines.append(f'F; {frec.start_time_value}; {frec.size}; {frec.relPath}\n')
        return lines

if __name__ == "__main__":
    parser = argparse.ArgumentParser( description='Scan the file system to create a collection', prog="cscan" )
    parser.add_argument('collectionName', help='A name for the collection')
    parser.add_argument('-path', help='The top level (input) data directory containing all files for the collection')
    parser.add_argument('-ext', help="The file extension for all files in the collection (used only with '-path', default: nc)", default="nc")
    parser.add_argument('-globs', help='A comma-separated list of unix file system globs for selecting files in the collection')
    parser.add_argument('-glob', help='A single unix file system glob for selecting files in the collection')
    parser.add_argument('-mp', help='Use multiprocessing (true/false)', default="true")
    parser.add_argument('-stac', help='Use STAC formalism', default="false")
    args = parser.parse_args()
    scanner = FileScanner( args.collectionName, **vars(args) )
    ilDataDir = os.environ.get('ILDATA')
    assert ilDataDir is not None, "Must set the ILDATA environment variable to define the data directory"
    collectionsDir = os.path.join( ilDataDir, "collections" )
    if args.stac:
        scanner.writeSTAC()
    else:
        scanner.write( collectionsDir )




