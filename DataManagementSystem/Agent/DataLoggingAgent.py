'''
Created on Jun 19, 2015

@author: Corentin Berger
'''
from DIRAC import S_OK, gLogger

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.DB.DataLoggingDB    import DataLoggingDB

# # agent's name
AGENT_NAME = 'DataManagement/DataLoggingAgent'

########################################################################
class DataLoggingAgent( AgentModule ):
  def initialize( self ):
    self.maxSequenceToMove = self.am_getOption( "MaxSequenceToMove", 200 )
    self.__dataLoggingDB = DataLoggingDB( '/tmp/agentInsertion.txt', '/tmp/agentBetween.txt' )
    return S_OK()

  def execute( self ):
    """ this method call the moveSequences method of DataLoggingDB"""
    res = self.__dataLoggingDB.moveSequences( self.maxSequenceToMove )
    if not res['OK']:
      gLogger.error( 'DataLoggingAgent, error %s' % res['Message'] )
    return S_OK()