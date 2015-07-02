'''
Created on May 5, 2015

@author: Corentin Berger
'''
import json
import zlib
from DIRAC.Core.Base.Client               import Client
from DIRAC.DataManagementSystem.private.DLDecoder import DLDecoder
from DIRAC import S_OK

class DataLoggingClient( Client ):

  def __init__( self, url = None, **kwargs ):
    Client.__init__( self, **kwargs )
    self.setServer( "DataManagement/DataLogging" )
    self.dataLoggingManager = self._getRPC()


  def insertSequence( self, sequence, directInsert = False ):
    """
      This insert a sequence into DataLoggingDB database
      :param sequence, the sequence to insert
    """
    sequenceJSON = sequence.toJSON()
    if not sequenceJSON["OK"]:
      return sequenceJSON
    sequenceJSON = sequenceJSON['Value']
    seq = zlib.compress( sequenceJSON )
    res = self.dataLoggingManager.insertSequence( seq, directInsert )

    return res

  def getSequenceOnFile( self, fileName ):
    """
      This select all Sequence about a file
      :param fileName, name of the file
    """
    res = self.dataLoggingManager.getSequenceOnFile( fileName )
    sequences = []
    if not res["OK"]:
      return res
    seqs = res["Value"]
    for seq in seqs :
      res = json.loads( seq, cls = DLDecoder )
      sequences.append( res )
    return S_OK( sequences )

  def getSequenceByID( self, IDSeq ):
    """
      This select all Sequence about an ID
      :param IDSeq, ID of the sequence
    """
    res = self.dataLoggingManager.getSequenceByID( IDSeq )
    sequences = []
    if not res["OK"]:
      return res
    seqs = res["Value"]
    for seq in seqs :
      res = json.loads( seq, cls = DLDecoder )
      sequences.append( res )
    return S_OK( sequences )

  def getMethodCallOnFile( self, fileName, before, after ):
    """
      This select all method call about a file, you can precise a date before, a date after and both to make a between
      :param fileName, name of the file
      :param before, a date
      :param after, a date
    """
    res = self.dataLoggingManager.getMethodCallOnFile( fileName, before, after )
    methodCalls = []
    if not res["OK"]:
      return res
    calls = res["Value"]
    for call in calls :
      res = json.loads( call, cls = DLDecoder )
      methodCalls.append( res )
    return S_OK( methodCalls )

  def getMethodCallByName( self, name, before, after ):
    """
      This select all method call about a specific method name, you can precise a date before, a date after and both to make a between
      :param name, name of the method
      :param before, a date
      :param after, a date
    """
    res = self.dataLoggingManager.getMethodCallByName( name, before, after )
    methodCalls = []
    if not res["OK"]:
      return res
    calls = res["Value"]
    for call in calls :
      res = json.loads( call, cls = DLDecoder )
      methodCalls.append( res )
    return S_OK( methodCalls )
