'''
Created on May 5, 2015

@author: Corentin Berger
'''

import DIRAC
from types import StringTypes
import json
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC      import gLogger, S_ERROR

from DIRAC.DataManagementSystem.DB.DataLoggingDB    import DataLoggingDB
from DIRAC.DataManagementSystem.private.DataLoggingDecoder import DataLoggingDecoder




class DataLoggingHandler( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    """ initialize handler """
    try:
      cls.__dataLoggingDB = DataLoggingDB()
    except RuntimeError, error:
      gLogger.exception( error )
      return S_ERROR( error )

    # create tables for empty db
    return cls.__dataLoggingDB.createTables()


  types_insertSequence = [StringTypes]
  @classmethod
  def export_insertSequence( cls, sequenceJSON ):
    print "After receiving %s" % sequenceJSON
    try :
      sequence = json.loads( sequenceJSON, cls = DataLoggingDecoder )
      res = cls.__dataLoggingDB.putSequence( sequence )
    except :
      raise
    return res


  types_getSequenceOnFile = [StringTypes]
  @classmethod
  def export_getSequenceOnFile( cls, fileName ):
    try :
      res = cls.__dataLoggingDB.getSequenceOnFile( fileName )
    except :
      raise
    return res


  types_getMethodCallOnFile = [StringTypes]
  @classmethod
  def export_getMethodCallOnFile( cls, fileName ):
    try :
      res = cls.__dataLoggingDB.getMethodCallOnFile( fileName )
    except :
      raise
    return res
