'''
Created on May 5, 2015

@author: Corentin Berger
'''
import zlib
import json

from types import StringTypes, NoneType, BooleanType

from DIRAC      import S_OK, gConfig, gLogger, S_ERROR

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.DataManagementSystem.private.DLDecoder import DLDecoder
from DIRAC.DataManagementSystem.DB.DataLoggingDB    import DataLoggingDB

class DataLoggingHandler( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    """ initialize handler
    """
    csSection = PathFinder.getServiceSection( 'DataManagement/DataLogging' )
    # maxSequenceToMove is the maximum number of sequences that we can move in the moveSequences method
    cls.maxSequenceToMove = gConfig.getValue( '%s/MaxSequenceToMove' % csSection, 100 )
    # expirationTime is the maximum time for a Compressed Sequence to has its status at Ongoing, the time is in minutes
    cls.expirationTime = gConfig.getValue( '%s/ExpirationTime' % csSection, 3600 )
    # period between each call of moveSequences method, in second
    cls.moveSequencesPeriod = gConfig.getValue( '%s/MoveSequencesPeriod' % csSection, 100 )
    # period between each call of cleanExpiredCompressedSequence method, in second
    cls.cleanExpiredPeriod = gConfig.getValue( '%s/CleanExpiredPeriod' % csSection, 3600 )

    try:
      cls.__dataLoggingDB = DataLoggingDB()
      cls.__dataLoggingDB.createTables()
    except RuntimeError, error:
      gLogger.exception( error )
      return S_ERROR( error )
    # we set the minimum Valid period at 10 seconds
    gThreadScheduler.setMinValidPeriod( 10 )
    # method moveSequences will be call each 10 seconds
    gThreadScheduler.addPeriodicTask( cls.moveSequencesPeriod, cls.moveSequences )
    # method cleanExpiredCompressedSequence will be call each 10800 seconds or 3 hours
    gThreadScheduler.addPeriodicTask( cls.cleanExpiredPeriod, cls.cleanExpiredCompressedSequence )
    return S_OK()


  @classmethod
  def moveSequences( cls ):
    """ this method call the moveSequences method of DataLoggingDB"""
    res = cls.__dataLoggingDB.moveSequences( cls.maxSequenceToMove )
    return res

  @classmethod
  def cleanExpiredCompressedSequence( cls ):
    """ this method call the cleanStaledSequencesStatus method of DataLoggingDB"""
    res = cls.__dataLoggingDB.cleanExpiredCompressedSequence( cls.expirationTime )
    return res

  types_insertSequence = [StringTypes, BooleanType]
  @classmethod
  def export_insertSequence( cls, sequenceCompressed, directInsert = False ):
    """
      this method call the insertSequenceDirectly method of DataLoggingDB if directInsert = True
      else call insertCompressedSequence method of DataLoggingDB

      :param sequence, the sequence to insert
      :param directInsert, a boolean, if we want to insert directly as a DLSequence and not a DLCompressedSequence
    """
    if directInsert :
      sequenceJSON = zlib.decompress( sequenceCompressed )
      sequence = json.loads( sequenceJSON , cls = DLDecoder )
      res = cls.__dataLoggingDB.insertSequenceDirectly( sequence )
    else :
      res = cls.__dataLoggingDB.insertCompressedSequence( sequenceCompressed )
    return res


  types_getSequenceOnFile = [StringTypes, ( list( StringTypes ) + [NoneType] ), ( list( StringTypes ) + [NoneType] )]
  @classmethod
  def export_getSequenceOnFile( cls, fileName, before = None, after = None, status = None ):
    """
      this method call the getSequenceOnFile method of DataLoggingDB

      :param fileName, name of a file
      :param before, a date
      :param after, a date

      :return sequences, a list of sequence
    """
    res = cls.__dataLoggingDB.getSequenceOnFile( fileName, before, after, status )
    if not res["OK"]:
      return res
    sequences = [seq.toJSON()['Value'] for seq in res['Value']]
    return S_OK( sequences )


  types_getSequenceByID = [StringTypes]
  @classmethod
  def export_getSequenceByID( cls, IDSeq ):
    """
      this method call the getSequenceByID method of DataLoggingDB

      :param IDSeq, ID of the sequence

      :return sequences, a list of sequence
    """
    res = cls.__dataLoggingDB.getSequenceByID( IDSeq )
    if not res["OK"]:
      return res
    sequences = [seq.toJSON()['Value'] for seq in res['Value']]
    return S_OK( sequences )

  types_getSequenceByCaller = [StringTypes, ( list( StringTypes ) + [NoneType] ), ( list( StringTypes ) + [NoneType] )]
  @classmethod
  def export_getSequenceByCaller( cls, callerName, before = None, after = None, status = None ):
    """
      this method call the getSequenceByCaller method of DataLoggingDB

      :param callerName, name of a caller
      :param before, a date
      :param after, a date

      :return sequences, a list of sequence
    """
    res = cls.__dataLoggingDB.getSequenceByCaller( callerName, before, after, status )
    if not res["OK"]:
      return res
    sequences = [seq.toJSON()['Value'] for seq in res['Value']]
    return S_OK( sequences )


  types_getMethodCallOnFile = [StringTypes, ( list( StringTypes ) + [NoneType] ), ( list( StringTypes ) + [NoneType] )]
  @classmethod
  def export_getMethodCallOnFile( cls, fileName, before = None, after = None, status = None ):
    """
      this method call the getMethodCallOnFile method of DataLoggingDB

      :param fileName, name of the file
      :param before, a date
      :param after, a date

      :return methodCalls, a list of method call
    """
    res = cls.__dataLoggingDB.getMethodCallOnFile( fileName, before, after, status )
    if not res["OK"]:
      return res
    methodCalls = [call.toJSON()['Value'] for call in res['Value']]
    return S_OK( methodCalls )


  types_getMethodCallByName = [StringTypes, ( list( StringTypes ) + [NoneType] ), ( list( StringTypes ) + [NoneType] )]
  @classmethod
  def export_getMethodCallByName( cls, methodName, before = None, after = None, status = None ):
    """
      this method call the getMethodCallByName method of DataLoggingDB

      :param name, name of the method
      :param before, a date
      :param after, a date

      :return methodCalls, a list of method call
    """
    res = cls.__dataLoggingDB.getMethodCallByName( methodName, before, after, status )
    if not res["OK"]:
      return res
    methodCalls = [call.toJSON()['Value'] for call in res['Value']]
    return S_OK( methodCalls )
