'''
Created on May 5, 2015

@author: Corentin Berger
'''
import json, time
import zlib
from DIRAC.Core.Base.Client               import Client
from DIRAC.ConfigurationSystem.Client     import PathFinder
from DIRAC.Core.DISET.RPCClient           import RPCClient
from DIRAC.DataManagementSystem.Client.DLException import DLException
from DIRAC.DataManagementSystem.private.DLDecoder import DLDecoder
from DIRAC import S_OK

class DataLoggingClient( Client ):

  def __init__( self, url = None, **kwargs ):
    Client.__init__( self, **kwargs )
    self.setServer( "DataManagement/DataLogging" )
    self.dataLoggingManager = self._getRPC()


  def insertSequence( self, sequence ):
    sequenceJSON = sequence.toJSON()
    if not sequenceJSON["OK"]:
      raise Exception( 'DataLoggingClient.insertSequence bad sequenceJSON' )
    sequenceJSON = sequenceJSON['Value']
    seq = zlib.compress( sequenceJSON )
    res = self.dataLoggingManager.insertCompressedSequence( seq )

    return res

  def getSequenceOnFile( self, fileName ):
    res = self.dataLoggingManager.getSequenceOnFile( fileName )
    sequences = []
    if res["OK"]:
      seqs = res["Value"]
      for seq in seqs :
        res = json.loads( seq, cls = DLDecoder )
        sequences.append( res )
    return S_OK( sequences )

  def getSequenceByID( self, IDSeq ):
    res = self.dataLoggingManager.getSequenceByID( IDSeq )
    sequences = []
    if res["OK"]:
      seqs = res["Value"]
      for seq in seqs :
        res = json.loads( seq, cls = DLDecoder )
        sequences.append( res )
    return S_OK( sequences )
  getSequenceByID

  def getMethodCallOnFile( self, fileName, before, after ):
    res = self.dataLoggingManager.getMethodCallOnFile( fileName, before, after )
    methodCalls = []
    if res["OK"]:
      calls = res["Value"]
      for call in calls :
        res = json.loads( call, cls = DLDecoder )
        methodCalls.append( res )
    return S_OK( methodCalls )

  def getMethodCallByName( self, name, before, after ):
    res = self.dataLoggingManager.getMethodCallByName( name, before, after )
    methodCalls = []
    if res["OK"]:
      calls = res["Value"]
      for call in calls :
        res = json.loads( call, cls = DLDecoder )
        methodCalls.append( res )
    return S_OK( methodCalls )
