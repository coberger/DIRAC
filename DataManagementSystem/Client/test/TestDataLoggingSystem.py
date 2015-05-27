'''
Created on May 22, 2015

@author: Corentin Berger
'''
import unittest


from DIRAC.DataManagementSystem.Client.test.mockDirac import ClientA, ClientB, ClientC
from DIRAC.DataManagementSystem.Client.DataLoggingClient   import DataLoggingClient

class DataLoggingArgumentsTestCase( unittest.TestCase ):
  pass

class ClientACase ( DataLoggingArgumentsTestCase ):
  def test_insertion_equal( self ):
    client = ClientA( ['A', 'B', 'C', 'D'] )
    client.doSomething()
    dlc = DataLoggingClient()
    dlc.getSequenceOnFile( 'A' )


class ClientBCase ( DataLoggingArgumentsTestCase ):
  pass



if __name__ == "__main__":

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientACase )

  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
