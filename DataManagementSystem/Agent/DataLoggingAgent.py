'''
Created on Jun 19, 2015

@author: Corentin Berger
'''


from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.Client.test.mockDirac import ClientB

# # agent's name
AGENT_NAME = 'DataManagement/DataLoggingAgent'

########################################################################
class DataLoggingAgent( AgentModule ):
  def initialize( self ):
    pass

  def execute( self ):
    self.test_agentName()

  def test_agentName(self):
    client = ClientB()
    client.doSomething()
