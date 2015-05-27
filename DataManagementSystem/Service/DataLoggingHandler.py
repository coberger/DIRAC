'''
Created on May 5, 2015

@author: Corentin Berger
'''

import DIRAC
from types import StringTypes
import json
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC      import gLogger

from DIRAC.DataManagementSystem.DB.DataLoggingDB    import DataLoggingDB
from DIRAC.DataManagementSystem.private.DataLoggingDecoder import DataLoggingDecoder




class DataLoggingHandler( RequestHandler ):

  types_insertSequence = [StringTypes]
  def export_insertSequence( self, sequenceJSON ):
    # print "After receiving %s" % sequenceJSON
    sequence = json.loads( sequenceJSON, cls = DataLoggingDecoder )
    db = DataLoggingDB()
    db.createTables()
    res = db.putSequence( sequence )
    if not res["OK"]:
      gLogger.error( ' error export_insertSequence', res['Message'] )
      DIRAC.exit( -1 )
    return res


  types_getSequenceOnFile = [StringTypes]
  def export_getSequenceOnFile( self, fileName ):
    db = DataLoggingDB()
    res = db.getSequenceOnFile( fileName )
    return res


  types_getMethodCallOnFile = [StringTypes]
  def export_getMethodCallOnFile( self, fileName ):
    db = DataLoggingDB()
    res = db.getMethodCallOnFile( fileName )
    return res

  def export_createTables( self ):
    db = DataLoggingDB()
    res = db.createTables()
    return res
