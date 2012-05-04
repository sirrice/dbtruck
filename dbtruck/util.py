import sys
import datetime
import math
import logging


class GlobalLogger(object):
    def __init__(self):
        self._logger = None
        self.formatter = logging.Formatter('%(asctime)s - %(lineno)s - %(levelname)s - %(message)s')
    
    def __call__(self, fname=None, flevel=None, plevel=None):
        if self._logger:
            # set levels
            if fname:
                fh = filter(lambda h: h.get_name() == 'filehandler',
                           self._logger.handlers)[0]
                self._logger.removeHandler(fh)

                fh = logging.FileHandler(fname)
                fh.setLevel(logging.DEBUG)
                fh.setFormatter(self.formatter)
                fh.set_name('filehandler')
                self._logger.addHandler(fh)
                
            for handler in self._logger.handlers:
                if plevel and handler.get_name() == 'stdhandler':
                    handler.setLevel(plevel)
                if flevel and handler.get_name() == 'filehandler':
                    handler.setLevel(flevel)
            self._logger
            return self._logger

        fname = fname or './log.txt'                
        flevel = flevel or logging.DEBUG
        plevel = plevel or logging.WARNING
        
        self._logger = logging.getLogger()
        self._logger.setLevel(logging.DEBUG)


        fh = logging.FileHandler(fname)
        fh.setLevel(flevel)
        fh.setFormatter(self.formatter)
        fh.set_name('filehandler')
        self._logger.addHandler(fh)

        ph = logging.StreamHandler(sys.stdout)
        ph.setLevel(plevel)
        ph.setFormatter(self.formatter)
        ph.set_name('stdhandler')
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
    
