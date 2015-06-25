'''
Created on May 5, 2015

@author: Corentin Berger
'''
import json

from types import StringTypes, NoneType, IntType
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC      import S_OK, gConfig, gLogger, S_ERROR

from DIRAC.DataManagementSystem.DB.DataLoggingDB    import DataLoggingDB

class DataLoggingHandler( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    """ initialize handler """
    csSection = PathFinder.getServiceSection( 'DataManagement/DataLogging' )
    cls.maxSequence = gConfig.getValue( '%s/MaxSequence' % csSection, 100 )
    cls.maxTime = gConfig.getValue( '%s/MaxTime' % csSection, 3600 )
    try:
      cls.__dataLoggingDB = DataLoggingDB()
      cls.__dataLoggingDB.createTables()
    except RuntimeError, error:
      gLogger.exception( error )
      return S_ERROR( error )
    gThreadScheduler.setMinValidPeriod( 10 )
    gThreadScheduler.addPeriodicTask( 10, cls.moveSequences )
    gThreadScheduler.addPeriodicTask( 10800, cls.cleanStaledSequencesStatus )
    return S_OK()


  @classmethod
  def moveSequences( cls ):
    res = cls.__dataLoggingDB.moveSequences( cls.maxSequence )
    return res

  @classmethod
  def cleanStaledSequencesStatus( cls ):
    res = cls.__dataLoggingDB.cleanStaledSequencesStatus( cls.maxTime )
    return res

  types_insertCompressedSequence = [StringTypes]
  @classmethod
  def export_insertCompressedSequence( cls, sequenceCompress ):
    res = cls.__dataLoggingDB.insertCompressedSequence( sequenceCompress )
    return res


  types_getSequenceOnFile = [StringTypes]
  @classmethod
  def export_getSequenceOnFile( cls, fileName ):
    res = cls.__dataLoggingDB.getSequenceOnFile( fileName )
    sequences = []
    if res["OK"]:
      seqs = res["Value"]
      for seq in seqs :
        sequences.append( seq.toJSON()["Value"] )
    return S_OK( sequences )


  types_getSequenceByID = [StringTypes]
  @classmethod
  def export_getSequenceByID( cls, IDSeq ):
    res = cls.__dataLoggingDB.getSequenceByID( IDSeq )
    sequences = []
    if res["OK"]:
      seqs = res["Value"]
      for seq in seqs :
        sequences.append( seq.toJSON()["Value"] )
    return S_OK( sequences )


  types_getMethodCallOnFile = [StringTypes, ( list( StringTypes ) + [NoneType] ), ( list( StringTypes ) + [NoneType] )]
  @classmethod
  def export_getMethodCallOnFile( cls, fileName, before, after ):
    res = cls.__dataLoggingDB.getMethodCallOnFile( fileName, before, after )
    methodCalls = []
    if res["OK"]:
      calls = res["Value"]
      for call in calls :
        methodCalls.append( call.toJSON()["Value"] )
    return S_OK( methodCalls )


  types_getMethodCallByName = [StringTypes, ( list( StringTypes ) + [NoneType] ), ( list( StringTypes ) + [NoneType] )]
  @classmethod
  def export_getMethodCallByName( cls, methodName, before, after ):
    res = cls.__dataLoggingDB.getMethodCallByName( methodName, before, after )
    methodCalls = []
    if res["OK"]:
      calls = res["Value"]
      for call in calls :
        methodCalls.append( call.toJSON()["Value"] )
    return S_OK( methodCalls )
