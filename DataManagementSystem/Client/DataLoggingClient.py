'''
Created on May 5, 2015

@author: Corentin Berger
'''
from DIRAC.Core.Base.Client               import Client
from DIRAC.ConfigurationSystem.Client     import PathFinder
from DIRAC.Core.DISET.RPCClient           import RPCClient

from DIRAC import gLogger

class DataLoggingClient( Client ):

  def __init__( self ):
    Client.__init__( self )
    self.setServer( "DataManagement/DataLogging" )

    url = PathFinder.getServiceURL( "DataManagement/DataLogging" )
    if not url:
      raise RuntimeError( "CS option DataManagement/DataLogging URL is not set!" )
    self.testManager = RPCClient( url )

  def insertSequence( self, sequence ):
    sequenceJSON = sequence.toJSON()
    # print "BEFORE SENDING %s" % sequenceJSON['Value']
    if not sequenceJSON["OK"]:
      return sequenceJSON
    sequenceJSON = sequenceJSON["Value"]
    res = self.testManager.insertSequence( sequenceJSON )
    # gLogger.error( 'res = ', res )
    return res

