'''
Created on May 22, 2015

@author: Corentin Berger
'''
import unittest


from DIRAC.DataManagementSystem.Client.test.mockDirac import ClientA, ClientB, ClientC, ClientD
from DIRAC.DataManagementSystem.Client.DataLoggingClient   import DataLoggingClient

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
    self.assertEqual( sequenceOne.methodCalls[0].actions[0].fileDL.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[1].fileDL.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[2].fileDL.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[3].fileDL.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[0].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[1].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[2].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[3].status, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[1].name.name, 'TestFileCatalog.addReplica' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[0].fileDL.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[1].fileDL.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[0].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[1].status, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[2].name.name, 'TestFileCatalog.getFileSize' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[0].fileDL.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[1].fileDL.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[0].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[1].status, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[3].name.name, 'TestStorageElement.putFile' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[0].fileDL.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[1].fileDL.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[2].fileDL.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[3].fileDL.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[0].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[1].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[2].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[3].status, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[4].name.name, 'TestStorageElement.getFileSize' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[0].fileDL.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[1].fileDL.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[2].fileDL.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[3].fileDL.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[0].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[1].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[2].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[3].status, 'Failed' )

    self.assertEqual( sequenceTwo.caller.name, 'DIRAC.DataManagementSystem.Client.test.mockDirac.ClientA.doSomething' )
    self.assertEqual( sequenceTwo.methodCalls[0].name.name, 'TestStorageElement.getFileSize' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[0].fileDL.name, '/data/file1' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[1].fileDL.name, '/data/file2' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[2].fileDL.name, '/data/file3' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[3].fileDL.name, '/data/file4' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[0].status, 'Successful' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[1].status, 'Failed' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[2].status, 'Successful' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[3].status, 'Failed' )




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

    self.assertEqual( sequenceOne.caller.name, 'DIRAC.DataManagementSystem.Client.test.mockDirac.ClientB.doSomething' )
    self.assertEqual( sequenceOne.methodCalls[0].name.name, 'TestDataManager.putAndRegister' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[0].fileDL.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[1].fileDL.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[2].fileDL.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[3].fileDL.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[0].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[1].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[2].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[3].status, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[1].name.name, 'TestFileCatalog.addFile' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[0].fileDL.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[1].fileDL.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[0].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[1].status, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[2].name.name, 'TestStorageElement.putFile' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[0].fileDL.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[1].fileDL.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[2].fileDL.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[3].fileDL.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[0].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[1].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[2].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[3].status, 'Failed' )

    self.assertEqual( sequenceOne.methodCalls[3].name.name, 'TestStorageElement.getFileSize' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[0].fileDL.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[1].fileDL.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[2].fileDL.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[3].fileDL.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[0].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[1].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[2].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[3].status, 'Failed' )

    self.assertEqual( sequenceTwo.caller.name, 'DIRAC.DataManagementSystem.Client.test.mockDirac.ClientB.doSomething' )
    self.assertEqual( sequenceTwo.methodCalls[0].name.name, 'TestFileCatalog.getFileSize' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[0].fileDL.name, '/data/file3' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[0].status, 'Successful' )


class ClientCCase ( DataLoggingArgumentsTestCase ):
  # this client raise an exception from the decorator, the result should be unchanged by the exception
  def setUp( self ):
    pass

  def test_no_exception( self ):
    client = ClientC()
    res = client.doSomething()
    self.assertEqual( res['Value'], 5 )

class ClientDCase ( DataLoggingArgumentsTestCase ):
  # this client raise an exception from the decorator, the result should be unchanged by the exception
  def setUp( self ):
    self.dlc = DataLoggingClient()

  def test_no_exception( self ):
    client = ClientD()
    with self.assertRaises( Exception ):
      client.doSomething()

    sequence = self.dlc.getSequenceByID( '6' )['Value'][0]

    self.assertEqual( len( sequence.methodCalls ), 4 )

    self.assertEqual( sequence.caller.name, 'DIRAC.DataManagementSystem.Client.test.mockDirac.ClientD.doSomething' )
    self.assertEqual( sequence.methodCalls[0].name.name, 'TestDataManager.putAndRegister' )
    self.assertEqual( sequence.methodCalls[0].actions[0].fileDL.name, '/data/file1' )
    self.assertEqual( sequence.methodCalls[0].actions[1].fileDL.name, '/data/file2' )
    self.assertEqual( sequence.methodCalls[0].actions[2].fileDL.name, '/data/file3' )
    self.assertEqual( sequence.methodCalls[0].actions[3].fileDL.name, '/data/file4' )
    self.assertEqual( sequence.methodCalls[0].actions[0].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[0].actions[1].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[0].actions[2].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[0].actions[3].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[0].actions[0].errorMessage, 'addFile exception' )
    self.assertEqual( sequence.methodCalls[0].actions[1].errorMessage, 'addFile exception' )
    self.assertEqual( sequence.methodCalls[0].actions[2].errorMessage, 'addFile exception' )
    self.assertEqual( sequence.methodCalls[0].actions[3].errorMessage, 'addFile exception' )


    self.assertEqual( sequence.methodCalls[1].name.name, 'TestFileCatalog.addFile' )
    self.assertEqual( sequence.methodCalls[1].actions[0].fileDL.name, '/data/file3' )
    self.assertEqual( sequence.methodCalls[1].actions[1].fileDL.name, '/data/file1' )
    self.assertEqual( sequence.methodCalls[1].actions[0].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[1].actions[1].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[1].actions[0].errorMessage, 'addFile exception' )
    self.assertEqual( sequence.methodCalls[1].actions[1].errorMessage, 'addFile exception' )

    self.assertEqual( sequence.methodCalls[2].name.name, 'TestStorageElement.putFile' )
    self.assertEqual( sequence.methodCalls[2].actions[0].fileDL.name, '/data/file1' )
    self.assertEqual( sequence.methodCalls[2].actions[1].fileDL.name, '/data/file2' )
    self.assertEqual( sequence.methodCalls[2].actions[2].fileDL.name, '/data/file3' )
    self.assertEqual( sequence.methodCalls[2].actions[3].fileDL.name, '/data/file4' )
    self.assertEqual( sequence.methodCalls[2].actions[0].status, 'Successful' )
    self.assertEqual( sequence.methodCalls[2].actions[1].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[2].actions[2].status, 'Successful' )
    self.assertEqual( sequence.methodCalls[2].actions[3].status, 'Failed' )

    self.assertEqual( sequence.methodCalls[3].name.name, 'TestStorageElement.getFileSize' )
    self.assertEqual( sequence.methodCalls[3].actions[0].fileDL.name, '/data/file1' )
    self.assertEqual( sequence.methodCalls[3].actions[1].fileDL.name, '/data/file2' )
    self.assertEqual( sequence.methodCalls[3].actions[2].fileDL.name, '/data/file3' )
    self.assertEqual( sequence.methodCalls[3].actions[3].fileDL.name, '/data/file4' )
    self.assertEqual( sequence.methodCalls[3].actions[0].status, 'Successful' )
    self.assertEqual( sequence.methodCalls[3].actions[1].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[3].actions[2].status, 'Successful' )
    self.assertEqual( sequence.methodCalls[3].actions[3].status, 'Failed' )


if __name__ == "__main__":

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientACase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ClientBCase ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ClientCCase ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ClientDCase ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
