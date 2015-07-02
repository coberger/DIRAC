'''
Created on May 22, 2015

@author: Corentin Berger
'''
import unittest


from DIRAC.DataManagementSystem.Client.test.mockDirac import ClientA, ClientB, ClientC
from DIRAC.DataManagementSystem.Client.DataLoggingClient   import DataLoggingClient
from time import sleep

dlc = DataLoggingClient()


class DataLoggingArgumentsTestCase( unittest.TestCase ):
  pass

class ClientACase ( DataLoggingArgumentsTestCase ):
  def setUp( self ):
    self.dlc = DataLoggingClient()

  def test_insertion_equal( self ):

    client = ClientA( ['/data/file1', '/data/file2', '/data/file3', '/data/file4'] )
    client.doSomething()

    sequenceOne = self.dlc.getSequenceByID( '1' )['Value'][0]
    sequenceTwo = self.dlc.getSequenceByID( '2' )['Value'][0]

    self.assertEqual( len( sequenceOne.methodCalls ), 5 )
    self.assertEqual( len( sequenceTwo.methodCalls ), 1 )

    self.assertEqual( sequenceOne.caller.name, 'DIRAC.DataManagementSystem.Client.test.mockDirac.ClientA.doSomething' )
    self.assertEqual( sequenceOne.methodCalls[0].name.name, 'TestDataManager.replicateAndRegister' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[0].status.name, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[1].status.name, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[2].status.name, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[3].status.name, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[1].name.name, 'TestFileCatalog.addReplica' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[0].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[1].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[0].status.name, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[1].status.name, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[2].name.name, 'TestFileCatalog.getFileSize' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[0].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[1].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[0].status.name, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[1].status.name, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[3].name.name, 'TestStorageElement.putFile' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[0].status.name, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[1].status.name, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[2].status.name, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[3].status.name, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[4].name.name, 'TestStorageElement.getFileSize' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[0].status.name, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[1].status.name, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[2].status.name, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[3].status.name, 'Failed' )

    self.assertEqual( sequenceTwo.caller.name, 'DIRAC.DataManagementSystem.Client.test.mockDirac.ClientA.doSomething' )
    self.assertEqual( sequenceTwo.methodCalls[0].name.name, 'TestStorageElement.getFileSize' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[0].status.name, 'Successful' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[1].status.name, 'Failed' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[2].status.name, 'Successful' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[3].status.name, 'Failed' )




class ClientBCase ( DataLoggingArgumentsTestCase ):
  def setUp( self ):
    self.dlc = DataLoggingClient()

  def test_insertion_equal( self ):
    client = ClientB()
    client.doSomething()

    sequenceOne = self.dlc.getSequenceByID( '3' )['Value'][0]
    sequenceTwo = self.dlc.getSequenceByID( '4' )['Value'][0]

    self.assertEqual( len( sequenceOne.methodCalls ), 4 )
    self.assertEqual( len( sequenceTwo.methodCalls ), 1 )

    self.assertEqual( sequenceTwo.caller.name, 'DIRAC.DataManagementSystem.Client.test.mockDirac.ClientB.doSomething' )
    self.assertEqual( sequenceOne.methodCalls[0].name.name, 'TestDataManager.putAndRegister' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[0].status.name, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[1].status.name, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[2].status.name, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[3].status.name, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[1].name.name, 'TestFileCatalog.addFile' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[0].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[1].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[0].status.name, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[1].status.name, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[2].name.name, 'TestStorageElement.putFile' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[0].status.name, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[1].status.name, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[2].status.name, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[3].status.name, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[3].name.name, 'TestStorageElement.getFileSize' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[0].status.name, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[1].status.name, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[2].status.name, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[3].status.name, 'Failed' )

    self.assertEqual( sequenceTwo.caller.name, 'DIRAC.DataManagementSystem.Client.test.mockDirac.ClientB.doSomething' )
    self.assertEqual( sequenceTwo.methodCalls[0].name.name, 'TestFileCatalog.getFileSize' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[0].file.name, '/data/file3' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[0].status.name, 'Successful' )


class ClientCCase ( DataLoggingArgumentsTestCase ):
  def setUp( self ):
    pass

  def test_no_exception( self ):
    client = ClientC()

if __name__ == "__main__":

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientACase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ClientBCase ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ClientCCase ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
