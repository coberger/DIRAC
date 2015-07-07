'''
Created on May 5, 2015

@author: Corentin Berger
'''
import json
import zlib
from DIRAC.Core.Base.Client               import Client
from DIRAC.DataManagementSystem.private.DLDecoder import DLDecoder
from DIRAC import S_OK,gLogger

class DataLoggingClient( Client ):

  def __init__( self, url = None, **kwargs ):
    Client.__init__( self, **kwargs )
    self.setServer( "DataManagement/DataLogging" )
    self.dataLoggingManager = self._getRPC()


  def insertSequence( self, sequence, directInsert = False ):
    """
      This insert a sequence into DataLoggingDB database

      :param sequence, the sequence to insert
      :param directInsert, a boolean, if we want to insert directly as a DLSequence and not a DLCompressedSequence
    """
    sequenceJSON = sequence.toJSON()
    if not sequenceJSON["OK"]:
      gLogger.error( sequenceJSON['Message'] )
      return sequenceJSON
    sequenceJSON = sequenceJSON['Value']
    seq = zlib.compress( sequenceJSON )
    res = self.dataLoggingManager.insertSequence( seq, directInsert )
    return res

  def getSequenceOnFile( self, fileName, before = None, after = None ):
    """
      This select all Sequence about a file

      :param fileName, name of a file
      :param before, a date
      :param after, a date

      :return sequences, a list of sequence
    """
    res = self.dataLoggingManager.getSequenceOnFile( fileName, before, after )
    if not res["OK"]:
      return res
    sequences = [json.loads( seq, cls = DLDecoder ) for seq in res['Value']]

    return S_OK( sequences )

  def getSequenceByID( self, IDSeq ):
    """
      This select all Sequence about an ID

      :param IDSeq, ID of the sequence

      :return sequences, a list of sequence
    """
    res = self.dataLoggingManager.getSequenceByID( IDSeq )
    if not res["OK"]:
      return res
    sequences = [json.loads( seq, cls = DLDecoder ) for seq in res['Value']]
    return S_OK( sequences )

  def getSequenceByCaller( self, callerName, before = None, after = None ):
    """
      This select all Sequence about an ID

      :param callerName, name of a caller
      :param before, a date
      :param after, a date

      :return sequences, a list of sequence
    """
    print callerName
    res = self.dataLoggingManager.getSequenceByCaller( callerName, before, after )
    if not res["OK"]:
      return res
    sequences = [json.loads( seq, cls = DLDecoder ) for seq in res['Value']]
    return S_OK( sequences )

  def getMethodCallOnFile( self, fileName, before = None, after = None ):
    """
      This select all method call about a file, you can precise a date before, a date after and both to make a between

      :param fileName, name of the file
      :param before, a date
      :param after, a date

      :return methodCalls, a list of method call
    """
    res = self.dataLoggingManager.getMethodCallOnFile( fileName, before, after )
    if not res["OK"]:
      return res
    methodCalls = [json.loads( call, cls = DLDecoder ) for call in res['Value']]
    return S_OK( methodCalls )

  def getMethodCallByName( self, name, before = None, after = None ):
    """
      This select all method call about a specific method name, you can precise a date before, a date after and both to make a between

      :param name, name of the method
      :param before, a date
      :param after, a date

      :return methodCalls, a list of method call
    """
    res = self.dataLoggingManager.getMethodCallByName( name, before, after )
    if not res["OK"]:
      return res
    methodCalls = [json.loads( call, cls = DLDecoder ) for call in res['Value']]
    return S_OK( methodCalls )
