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
    dlc = DataLoggingClient()
    dlc.dropTables()
    dlc.createTables()
    A = '1 replicateAndRegister A Successful ,1 addReplica A Successful ,1 putFile A Successful ,2 getFileSize A Successful'
    B = '1 replicateAndRegister B Failed ,1 putFile B Failed ,2 getFileSize B Failed'
    C = '1 putFile C Successful ,1 replicateAndRegister C Failed ,1 addReplica C Failed ,2 getFileSize C Successful'
    D = '1 replicateAndRegister D Failed ,1 putFile D Failed ,2 getFileSize D Failed'
    client = ClientA( ['A', 'B', 'C', 'D'] )
    client.doSomething()

    res = dlc.getSequenceOnFile( 'A' )['Value']
    self.assert_( res, A )
    res = dlc.getSequenceOnFile( 'B' )['Value']
    self.assert_( res, B )
    res = dlc.getSequenceOnFile( 'C' )['Value']
    self.assert_( res, C )
    res = dlc.getSequenceOnFile( 'D' )['Value']
    self.assert_( res, D)


class ClientBCase ( DataLoggingArgumentsTestCase ):
  def test_insertion_equal( self ):
    dlc = DataLoggingClient()
    dlc.dropTables()
    dlc.createTables()
    A = '1 putAndRegister A Successful ,1 addFile A Successful ,1 putFile A Successful ,2 getFileSize A Successful'
    B = '1 putAndRegister B Failed ,1 putFile B Failed'
    C = '1 putFile C Successful ,1 putAndRegister C Failed ,1 addFile C Failed'
    D = '1 putAndRegister D Failed ,1 putFile D Failed'
    client = ClientB()
    client.doSomething()

    res = dlc.getSequenceOnFile( 'A' )['Value']
    self.assert_( res, A )
    res = dlc.getSequenceOnFile( 'B' )['Value']
    self.assert_( res, B )
    res = dlc.getSequenceOnFile( 'C' )['Value']
    self.assert_( res, C )
    res = dlc.getSequenceOnFile( 'D' )['Value']
    self.assert_( res, D )


class ClientCCase ( DataLoggingArgumentsTestCase ):
  def test_insertion_equal( self ):
    dlc = DataLoggingClient()
    dlc.dropTables()
    dlc.createTables()
    A = '1 putAndRegister A Successful ,1 addFile A Successful ,1 putFile A Successful ,2 getFileSize A Successful'
    B = '1 putAndRegister B Failed ,1 putFile B Failed'
    C = '1 putFile C Successful ,1 putAndRegister C Failed ,1 addFile C Failed'
    D = '1 putAndRegister D Failed ,1 putFile D Failed'
    client = ClientC()
    client.doSomething()

    res = dlc.getSequenceOnFile( 'A' )['Value']
    self.assert_( res, A )
    res = dlc.getSequenceOnFile( 'B' )['Value']
    self.assert_( res, B )
    res = dlc.getSequenceOnFile( 'C' )['Value']
    self.assert_( res, C )
    res = dlc.getSequenceOnFile( 'D' )['Value']
    self.assert_( res, D )


if __name__ == "__main__":

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientACase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ClientBCase ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
