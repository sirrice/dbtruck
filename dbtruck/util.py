import sys
import datetime
import math
import logging


class GlobalLogger(object):
    def __init__(self):
        self._logger = None
    
    def __call__(self, fname='./log.txt', flevel=logging.DEBUG, plevel=logging.WARNING):
        if self._logger:
            # set levels
            return self._logger

        self._logger = logging.getLogger()
        self._logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(lineno)s - %(levelname)s - %(message)s')

        fh = logging.FileHandler(fname)
        fh.setLevel(flevel)
        fh.setFormatter(formatter)
        self._logger.addHandler(fh)

        ph = logging.StreamHandler(sys.stdout)
        ph.setLevel(plevel)
        ph.setFormatter(formatter)
        self._logger.addHandler(ph)
        return self._logger

get_logger = GlobalLogger()



def block_iter(l, nblocks=2):
    """
    partitions l into nblocks blocks and returns generator over each block
    @param l list
    @param nblocks number of blocks to partition l into
    """
    blocksize = int(math.ceil(len(l) / float(nblocks)))
    i = 0
    while i < len(l):
        yield l[i:i+blocksize]
        i += blocksize      
    
