

class DataIterator(object):
    def __init__(self, iter_func, **kwargs):
        self.iter_func = iter_func
        self.fname = None
        self.file_index = 0  # keeps track of which table in th file this object refers to
        self.header = None
        self.types = None
        self.__dict__.update(kwargs)

    def __call__(self):
        return self.iter_func()

    def __iter__(self):
        return self.iter_func()
