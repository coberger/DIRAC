'''
Created on May 5, 2015

@author: Corentin Berger
'''
from DIRAC.Core.Base.Client               import Client
from DIRAC.ConfigurationSystem.Client     import PathFinder
from DIRAC.Core.DISET.RPCClient           import RPCClient
from DIRAC.DataManagementSystem.Client.DataLoggingException import DataLoggingException


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
      raise DataLoggingException( 'DataLoggingClient.insertSequence bad sequenceJSON' )
    sequenceJSON = sequenceJSON["Value"]
    # print "Before sending %s" % sequenceJSON
    try:
      res = self.dataLoggingManager.insertSequence( sequenceJSON )
    except :
      raise
    return res

  def getSequenceOnFile( self, fileName ):
    res = self.dataLoggingManager.getSequenceOnFile( fileName )
    return res

  def getMethodCallOnFile(self, fileName):
    res = self.dataLoggingManager.getMethodCallOnFile( fileName )
    return res



