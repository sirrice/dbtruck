import re

from collections import Counter

from dbtruck.infertypes import *
from dbtruck.util import get_logger, to_utf

re_space = re.compile('\s+')
re_nonascii = re.compile('[^\w\s]')
re_nonasciistart = re.compile('^[^\w]')
_log = get_logger()

class DataIterator(object):
    """
    Wrapper around a file reader.
    Intantiated with an iter_func function that returns a fresh tuple iterator over inputs.
    Provides analysis for type and header detection
    """
    def __init__(self, iter_func, **kwargs):
        self.iter_func = iter_func
        self.fname = None
        self.file_index = 0  # keeps track of which table in th file this object refers to
        self.header = None
        self.header_inferred = False
        self.add_id_col = False
        self.types = None
        self.__dict__.update(kwargs)


    def infer_metadata(self):
        if not self.types:
            self.types = infer_col_types(self)

        self.infer_header()
        _log.info( 'types:\t%s', ' '.join(map(str, self.types)) )
        _log.info( 'headers:\t%s', ' '.join(self.header))        

    def infer_header(self):
        """
        validate and infer a header for this tuple iterator.
        If it all fails, just generate a header 
        """

        # if all things fail, we can always make up headers!
        try:
            self.validate_header()

            if not self.header:
                self.infer_header_row()

            self.clean_header()
        except:
            pass

        # if we didn't find a header row, then lets default to
        # generating one
        if not self.header:
            self.header_inferred = False
            self.header = ['attr%d' % i for i in xrange(len(self.types))]

        # ensure proper length by manufacturing extra header columns
        if len(self.header) < len(self.types):
            for i in xrange(len(self.types) - len(self.header)):
                self.header.append('attr%d' % i)

        self.header = self.header[:len(self.types)]


        # we _always_ need an ID column
        if 'id' not in self.header:
            self.header.append('id')
            self.add_id_col = True
            self.types.append(int)


    def infer_header_row(self):
        "analyze first row in iterator and check if it looks like a header"

        types = self.types
        
        try:
            header = self().next()
            header = [s.strip() for s in header]
            _log.info("checking header: %s", header)
            htypes = map(get_type, header)
            matches = sum([ht == t and ht != None and t != str for ht, t in zip(htypes, types)])
            _log.info("matches: %s", matches)

            if matches > 0:
                return 

            if max(map(len, header)) > 100:
                _.log.warn("header colname longer than 100: %s", max(map(len, header)))
                return 

            # lots of more complex analysis goes HERE
            self.header = header
            self.header_inferred = True
        except:
            return



    def clean_header(self):
        """
        Given a header row, makes it database friendly e.g.,
        1) remove spaces and weird characters
        2) shorten to a reasonable length
        """
        header = self.header
        if not header:
            return

        newheader = []
        timesseen = Counter()
        attridx = 0
        for value in header:
            try:
                ret = re_nonasciistart.sub('', re_space.sub('_', re_nonascii.sub('', value.strip()).strip()))
                ret = to_utf(ret)
                if not ret:
                    ret = 'attr%d' % attridx
                if re.match('\d+', ret):
                    ret = 'n_%s' % ret
            except:
                _log.info('clean_header\t%s', value)
                ret = 'attr%d' % attridx
            attridx += 1
            ret = ret.lower()
            if timesseen[ret] > 0:
                newheader.append('%s_%d' % (ret, timesseen[ret]))
            elif timesseen[ret] > 3:
                break
            else:
                newheader.append(ret)
            timesseen[ret] += 1

        # XXX: ensure that header doesn't have overlapping values
        if len(set(newheader)) < len(newheader):
            _log.info("duplicate elements in header\t%s", str(newheader))
            self.header = None
        else:
            self.header = newheader


    def validate_header(self):
        """
        Make sure header at least has the same number of elements as the most
        popular row, otherwise disqualify
        """
        if self.header:
            c = Counter()
            for idx, row in enumerate(self()):
                c[len(row)] += 1
                if idx > 1000:
                    break
            if c:
                ncols = c.most_common(1)[0][0]
                if len(self.header) != ncols:
                    self.header = None
                    _log.info("""invalidating self.header because length %d doesn't
                                 match most popular row length %d""",
                                 len(self.header),
                                 ncols)
        

    def __call__(self):
        return self.iter_func()

    def __iter__(self):
        return self.iter_func()
