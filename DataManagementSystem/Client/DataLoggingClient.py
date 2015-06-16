'''
Created on May 5, 2015

@author: Corentin Berger
'''
import json
import zlib
from DIRAC.Core.Base.Client               import Client
from DIRAC.ConfigurationSystem.Client     import PathFinder
from DIRAC.Core.DISET.RPCClient           import RPCClient
from DIRAC.DataManagementSystem.Client.DLException import DLException
from DIRAC.DataManagementSystem.private.DLDecoder import DLDecoder
from DIRAC import S_OK

class DataLoggingClient( Client ):

  def __init__( self ):
    Client.__init__( self )
    self.setServer( "DataManagement/DataLogging" )
    url = PathFinder.getServiceURL( "DataManagement/DataLogging" )
    if not url:
      raise RuntimeError( "CS option DataManagement/DataLogging URL is not set!" )
    self.dataLoggingManager = RPCClient( url )

  def insertSequence( self, sequence ):
    sequenceJSON = sequence.toJSON()
    if not sequenceJSON["OK"]:
      raise Exception( 'Client.insertSequence bad sequenceJSON' )
    sequenceJSON = sequenceJSON["Value"]
    seq = zlib.compress( sequenceJSON )
    res = self.dataLoggingManager.insertSequence( seq )
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
