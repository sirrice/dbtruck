import pickle

from sqlalchemy import *
from sqlalchemy.orm import *
import sqlalchemy.types as types
from sqlalchemy.ext.declarative import *

from database import Base, db_session
from location import *        

class Metadata(Base):
    __tablename__ = '__dbtruck_metadata__'
    id = Column(Integer, primary_key=True)
    tablename =  Column(String(50))
    state = Column(Integer, default=0)    
    maxid = Column(Integer, default=0)
    hasloc = Column(Boolean, default=False)
    done = Column(Boolean, default=False)

    # table can be in following states
    # 0) shadow columns not created
    # 1) shadow cols created but not populated
    # 2) columns analyzed, but shadow cols not populated
    # 3) shadow cols populated but needs geocoding
    # 4) shadow cols geocoded partially (up to maxid)
    # 5) shadow cols geocoded completely (or didn't need it)

    
    def __init__(self, tablename, maxid=None, tested=None, hasloc=None, done=None):
        self.tablename = tablename
        self.maxid = maxid
        self.tested = tested
        self.hasloc = hasloc
        self.done = done

    def minimum_precision(self, db):
        attrs = ['shape', 'latlon', 'address', 'city', 'state', 'country', 'query', 'description']
        loctypes = set([anno.loctype for anno in self.annotations])
        loctypes = filter(lambda a: a in loctypes, attrs)
        
            
    @staticmethod
    def load_from_tablename(db, tablename):
        try:
            tablemd = db_session.query(Metadata).filter(Metadata.tablename == tablename).all()[0]
        except:
            # add row to metadata table
            tablemd = Metadata(tablename)
            db_session.add(tablemd)
            db_session.commit()
        return tablemd

class Annotation(Base):
    """
    An annotation either describes
    0) a column as as location type, and a function to extract it
    1) a value as a location type
    whether or not this was assigned by user or automatically

    annotypes:
     - column defined: 0
     - value defined: 1
    """
    __tablename__ = '__dbtruck_annotations__'
    id = Column(Integer, primary_key=True)
    annotype = Column(Integer, default=0)
    name = Column(String) # either a column name, or a string value
    loctype = Column(String) # or USERINPUT
    user_set = Column(Boolean, default=False)
    _extractor = Column(String)
    md_id = Column(Integer, ForeignKey('__dbtruck_metadata__.id'))
    md = relationship(Metadata,
                      primaryjoin=md_id == Metadata.id,
                      backref=backref('annotations', order_by=id))

    @synonym_for("_extractor")
    def extractor(self):
        exec('f = %s' % self._extractor)
        return f

    USERINPUT = '_userinput_' # if user input location annotations, use it

    def __init__(self, name, loctype, extractor, md, annotype=0, user_set = False):
        self.name = name
        self.loctype = loctype
        self._extractor = extractor
        self.md_id = md.id
        self.annotype = annotype
        self.user_set = user_set


class CorrelationPair(Base):
    __tablename__ = '__dbtruck_corrpair__'
    id = Column(Integer, primary_key=True)
    corr = Column(Float)
    radius = Column(Float, default=0)
    statname = Column(String)
    
    table1 = Column(String)
    col1 = Column(String)
    agg1 = Column(String, default=None)
    
    table2 = Column(String)
    col2 = Column(String)
    agg2 = Column(String, default=None)
    
    md1_id = Column(Integer, ForeignKey('__dbtruck_metadata__.id'))
    md2_id = Column(Integer, ForeignKey('__dbtruck_metadata__.id'))
    md1 = relationship(Metadata,
                      primaryjoin=md1_id == Metadata.id,
                      backref=backref('corrpairs1', order_by=id))
    md2 = relationship(Metadata,
                      primaryjoin=md2_id == Metadata.id,
                      backref=backref('corrpairs2', order_by=id))

    def __init__(self, corr, r, statname, t1, col1, agg1, t2, col2, agg2, md1, md2):
        self.corr = corr
        self.radius = r
        self.statname = statname
        
        self.table1 = t1
        self.col1 = col1
        self.agg1 = agg1
        
        self.table2 = t2
        self.col2 = col2
        self.agg2 = agg2
        
        self.md1_id = md1.id
        self.md2_id = md2.id
        self.md1 = md1
        self.md2 = md2

    def __unicode__(self):
        left = '%s.%s' % (self.md1.tablename,self.col1)
        if self.agg1:
            left = '%s(%s)' % (self.agg1, left)
        right = '%s.%s' % (self.md2.tablename,self.col2)
        if self.agg2:
            right = '%s(%s)' % (self.agg2, right)
        return ' '.join([left, right, str(self.corr)])

    def __str__(self):
        return self.__unicode__()


if __name__ == '__main__':
    pass
