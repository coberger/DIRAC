'''
Created on May 5, 2015

@author: Corentin Berger
'''

from types import StringTypes, NoneType, StringType, UnicodeType
import json
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC      import gLogger, S_ERROR, S_OK

from DIRAC.DataManagementSystem.DB.DataLoggingDB    import DataLoggingDB
from DIRAC.DataManagementSystem.private.DLDecoder import DLDecoder

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
    try :
      sequence = json.loads( sequenceJSON, cls = DLDecoder )
      res = cls.__dataLoggingDB.putSequence( sequence )
    except :
      raise
    return res


  types_getSequenceOnFile = [StringTypes]
  @classmethod
  def export_getSequenceOnFile( cls, fileName ):
    try :
      res = cls.__dataLoggingDB.getSequenceOnFile( fileName )
      sequences = []
      if res["OK"]:
        seqs = res["Value"]
        for seq in seqs :
          sequences.append( seq.toJSON()["Value"] )
    except :
      raise
    return S_OK( sequences )

  types_getSequenceByID = [StringTypes]
  @classmethod
  def export_getSequenceByID( cls, IDSeq ):
    try :
      res = cls.__dataLoggingDB.getSequenceByID( IDSeq )
      sequences = []
      if res["OK"]:
        seqs = res["Value"]
        for seq in seqs :
          sequences.append( seq.toJSON()["Value"] )
    except :
      raise
    return S_OK( sequences )


  types_getMethodCallOnFile = [StringTypes, ( list( StringTypes ) + [NoneType] ), ( list( StringTypes ) + [NoneType] )]
  @classmethod
  def export_getMethodCallOnFile( cls, fileName, before, after ):
    try :
      res = cls.__dataLoggingDB.getMethodCallOnFile( fileName, before, after )
      methodCalls = []
      if res["OK"]:
        calls = res["Value"]
        for call in calls :
          methodCalls.append( call.toJSON()["Value"] )
    except :
      raise
    return S_OK( methodCalls )


  types_getMethodCallByName = [StringTypes, ( list( StringTypes ) + [NoneType] ), ( list( StringTypes ) + [NoneType] )]
  @classmethod
  def export_getMethodCallByName( cls, methodName, before, after ):
    try :
      res = cls.__dataLoggingDB.getMethodCallByName( methodName, before, after )
      methodCalls = []
      if res["OK"]:
        calls = res["Value"]
        for call in calls :
          methodCalls.append( call.toJSON()["Value"] )
    except :
      raise
    return S_OK( methodCalls )
