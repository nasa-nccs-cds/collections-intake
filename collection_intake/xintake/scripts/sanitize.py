import os
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional

class PatchFiles:

    def __init__(self, base_dir: str ):
        self._base_dir = base_dir

    def _patch( self, file_path: str, changes: Dict ):
        new_lines = []
        print(f"    %%%% -->  PATCHING {file_path}")
        with open(file_path, 'r') as f:
            for line in f.readlines():
                for fromTxt, toTxt in changes.items():
                    line = line.replace( fromTxt, toTxt )
                new_lines.append( line )
        with open(file_path, 'w') as f:
            f.writelines(new_lines)


    def patchFiles( self, file_suffix: str, replacements: Dict ):
        print( f"Walking file system from {self._base_dir}")
        for root, dirs, files in os.walk( self._base_dir ):
            for file in files:
                if file.endswith( file_suffix ):
                    self._patch( os.path.join(root,file), replacements )


if __name__ == "__main__":
    patcher = PatchFiles( "/att/pubrepo/ILAB/data/collections/agg")
    changes = { 'dass/dassnsd/data01/cldra/data/pubrepo': 'nfs4m/css/curated01',  'CREATE-IP': 'create-ip' }

    patcher.patchFiles( ".ag1", changes )