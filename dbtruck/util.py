import datetime
import math

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
    
