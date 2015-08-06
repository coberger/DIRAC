'''
Created on May 20, 2015

@author: Corentin Berger
'''
import unittest

from DIRAC.DataManagementSystem.Client.DataLogging.DLUtilities import extractArgs, extractArgsExecuteFC, \
                    extractArgsExecuteSE, extractTupleArgs


"""
this is what we need to test if the system parses correctly arguments from a method call
argsDict* is a dictionnary like the dictionnary with all arguments passed to the decorator
arguments* is like *args when we call a decorate method
there is 5 different case :
  Default is when the call oh a decorate method is like method('lfn1','localPath',protocol ='myProtocol')
  ExecuteFC is when the call is about a method from FileCatalog
  SetReplicaProblematic is when the call is about SetReplicaProblematic method from FileCatalog
  ExecuteSE is when the call is about a method from StorageElement
"""
argsDictDefault = {}
argsDictDefault['argsPosition'] = [ 'files', 'localPath', ('targetSE', 'dstSE') ]
argumentsDefault = ( ['/lhcb/sub/file1.data', '/lhcb/sub/file2.data', '/lhcb/sub/file3.data'], '/local/path/', 'destSE' )
argumentsDefaultWhenOptionnal = ( ['/lhcb/sub/file1.data', '/lhcb/sub/file2.data', '/lhcb/sub/file3.data'], )
argumentsDefaultoptionnal = {'localPath' :'/local/path/', 'dstSE':'destSE'}

argsDictTuple = {}
argsDictTuple['argsPosition'] = ( ['tuple' ] )
argsDictTuple['tupleArgsPosition'] = ['files', 'physicalFile', 'fileSize', 'targetSE', 'fileGuid', 'checksum' ]
argumentsTuple = [( ( '/lhcb/sub/file1.data', '/local/file1.data', 150, 'destinationSE', 40, 108524789 ),
                 ( '/lhcb/sub/file2.data', '/local/file2.data', 7855, 'TargetSE', 14, 155 ) )]


argsDictExecuteFC = {}
lfnsFC = {'/lhcb/sub/file1.data':{'PFN':'PFN1', 'Size':1, 'SE':'se1', 'GUID':100, 'Checksum':10},
          '/lhcb/sub/file2.data':{'PFN':'PFN2', 'Size':2, 'SE':'se2', 'GUID':200, 'Checksum':20},
          '/lhcb/sub/file3.data':{'PFN':'PFN3', 'Size':3, 'SE':'se3', 'GUID':300, 'Checksum': 30 }}
argumentsExecuteFC = ( 'self', lfnsFC )
argsDictExecuteFC['call'] = 'addFile'
argsDictExecuteFC['methods_to_log'] = ['addFile']
argsDictExecuteFC['methods_to_log_arguments'] = {
              'addFile' :
                {'Arguments' : ['self', 'files'],
                 'type' : 'dict',
                 'valueType' : 'dict',
                 'dictKeys' : { 'PFN':'PFN', 'Size':'Size', 'targetSE':'SE', 'GUID':'GUID', 'Checksum':'Checksum'} }
              }

argsDictSetReplicaProblematic = {}
lfnsSetReplicaProblematic = {'/lhcb/sub/file2.data': {'S20': 'P20'}, \
                            '/lhcb/sub/file3.data': {'S30': 'P30', 'S31': 'P31', 'S32': 'P32'}, \
                            '/lhcb/sub/file1.data': {'S10': 'P10', 'S11': 'P11'}}
argumentsSetReplicaProblematic = ( 'self', lfnsSetReplicaProblematic )
argsDictSetReplicaProblematic['call'] = 'setReplicaProblematic'
argsDictSetReplicaProblematic['methods_to_log'] = ['setReplicaProblematic']
argsDictSetReplicaProblematic['methods_to_log_arguments'] = {
              'setReplicaProblematic' :
                {'Arguments' : ['self', 'files'],
                 'specialFunction' : 'setReplicaProblematic' }
              }


argsDictExecuteSE = {}
lfnsSE = {'/lhcb/sub/file1.data':'src_file1', '/lhcb/sub/file2.data':'src_file2', '/lhcb/sub/file3.data':'src_file3' }
argumentsExecuteSE = ( 'self', lfnsSE )
argsDictExecuteSE['methodName'] = 'putFile'
argsDictExecuteSE['name'] = 'targetSE1'
argsDictExecuteSE['methods_to_log'] = ['putFile']
argsDictExecuteSE['methods_to_log_arguments'] = {
              'putFile' :
                {'Arguments' : ['self', 'files'],
                 'type' : 'dict',
                 'valueType' : 'str',
                 'valueName' : 'src_file'},
              }


def callFunction( function , argsDict, *args, **kwargs ):
  ret = function( argsDict, *args, **kwargs )
  return ret


class DataLoggingArgumentsParsingTestCase( unittest.TestCase ):
  pass


class DefaultCase ( DataLoggingArgumentsParsingTestCase ):

  def test_DictEqual( self ):
    ok = [{'file': '/lhcb/sub/file1.data', 'targetSE': 'destSE', 'extra': 'localPath = /local/path/', 'srcSE': None}, \
          {'file': '/lhcb/sub/file2.data', 'targetSE': 'destSE', 'extra': 'localPath = /local/path/', 'srcSE': None}, \
          {'file': '/lhcb/sub/file3.data', 'targetSE': 'destSE', 'extra': 'localPath = /local/path/', 'srcSE': None}]

    ret = extractArgs( argsDictDefault , *argumentsDefault )['Value']
    for x in range( len( ret ) ):
      self.assertEqual( ret[x]['file'], ok[x]['file'] )
      self.assertEqual( ret[x]['targetSE'], ok[x]['targetSE'] )
      self.assertEqual( ret[x]['srcSE'], ok[x]['srcSE'] )
      self.assertEqual( ret[x]['extra'], ok[x]['extra'] )

    ret = extractArgs( argsDictDefault , *argumentsDefaultWhenOptionnal, **argumentsDefaultoptionnal )['Value']
    for x in range( len( ret ) ):
      self.assertEqual( ret[x]['file'], ok[x]['file'] )
      self.assertEqual( ret[x]['targetSE'], ok[x]['targetSE'] )
      self.assertEqual( ret[x]['srcSE'], ok[x]['srcSE'] )
      self.assertEqual( ret[x]['extra'], ok[x]['extra'] )

  def test_Error( self ):
    ret = extractArgsExecuteSE( argsDictDefault, *argumentsDefault )
    self.assertEqual( ret['OK'], False )
    ret = extractTupleArgs ( argsDictDefault , *argumentsDefault )
    self.assertEqual( ret['OK'], False )



class TupleCase ( DataLoggingArgumentsParsingTestCase ):

  def test_DictEqual( self ):
    ok = [{'file': '/lhcb/sub/file1.data', 'targetSE': 'destinationSE', \
              'extra': 'physicalFile = /local/file1.data,fileSize = 150,fileGuid = 40,checksum = 108524789', 'srcSE': None}, \
          {'file': '/lhcb/sub/file2.data', 'targetSE': 'TargetSE', \
             'extra': 'physicalFile = /local/file2.data,fileSize = 7855,fileGuid = 14,checksum = 155', 'srcSE': None}]
    ret = extractTupleArgs( argsDictTuple, *argumentsTuple )['Value']
    for x in range( len( ret ) ):
      self.assertEqual( ret[x]['file'], ok[x]['file'] )
      self.assertEqual( ret[x]['targetSE'], ok[x]['targetSE'] )
      self.assertEqual( ret[x]['srcSE'], ok[x]['srcSE'] )
      self.assertEqual( ret[x]['extra'], ok[x]['extra'] )

  def test_Error( self ):
    ret = extractArgsExecuteSE( argsDictTuple , *argumentsTuple )
    self.assertEqual( ret['OK'], False )

  def test_TupleAsNone( self ):
    ret = extractTupleArgs( argsDictTuple , [None] )
    self.assertEqual( ret['OK'], False )

class ExecuteFCCase ( DataLoggingArgumentsParsingTestCase ):
  def test_DictEqual( self ):
    ok = [{'files': None, 'targetSE': 'se2', 'extra': 'Size = 2,GUID = 200,Checksum = 20,PFN = PFN2', 'file': '/lhcb/sub/file2.data', 'srcSE': None}, \
          {'files': None, 'targetSE': 'se1', 'extra': 'Size = 1,GUID = 100,Checksum = 10,PFN = PFN1', 'file': '/lhcb/sub/file1.data', 'srcSE': None}, \
          {'files': None, 'targetSE': 'se3', 'extra': 'Size = 3,GUID = 300,Checksum = 30,PFN = PFN3', 'file': '/lhcb/sub/file3.data', 'srcSE': None}]
    ret = extractArgsExecuteFC( argsDictExecuteFC , *argumentsExecuteFC )['Value']
    for x in range( len( ret ) ):
      self.assertEqual( ret[x]['file'], ok[x]['file'] )
      self.assertEqual( ret[x]['targetSE'], ok[x]['targetSE'] )
      self.assertEqual( ret[x]['srcSE'], ok[x]['srcSE'] )
      self.assertEqual( ret[x]['extra'], ok[x]['extra'] )

  def test_Error( self ):
    ret = extractTupleArgs( argsDictExecuteFC, *argumentsExecuteFC )
    self.assertEqual( ret['OK'], False )

class SetReplicaProblematicCase( DataLoggingArgumentsParsingTestCase ):

  def test_DictEqual( self ):
    ok = [{'file': '/lhcb/sub/file2.data', 'targetSE': 'S20', 'extra': 'PFN = P20', 'srcSE': None}, \
          {'file': '/lhcb/sub/file1.data', 'targetSE': 'S11', 'extra': 'PFN = P11', 'srcSE': None}, \
          {'file': '/lhcb/sub/file1.data', 'targetSE': 'S10', 'extra': 'PFN = P10', 'srcSE': None}, \
          {'file': '/lhcb/sub/file3.data', 'targetSE': 'S31', 'extra': 'PFN = P31', 'srcSE': None}, \
          {'file': '/lhcb/sub/file3.data', 'targetSE': 'S30', 'extra': 'PFN = P30', 'srcSE': None}, \
          {'file': '/lhcb/sub/file3.data', 'targetSE': 'S32', 'extra': 'PFN = P32', 'srcSE': None}]
    ret = extractArgsExecuteFC( argsDictSetReplicaProblematic, *argumentsSetReplicaProblematic )['Value']
    for x in range( len( ret ) ):
      self.assertEqual( ret[x]['file'], ok[x]['file'] )
      self.assertEqual( ret[x]['targetSE'], ok[x]['targetSE'] )
      self.assertEqual( ret[x]['srcSE'], ok[x]['srcSE'] )
      self.assertEqual( ret[x]['extra'], ok[x]['extra'] )

class ExecuteSECase ( DataLoggingArgumentsParsingTestCase ):
  def test_DictEqual( self ):
    ok = [{'file': '/lhcb/sub/file2.data', 'targetSE': 'targetSE1', 'extra': 'src_file = src_file2', 'srcSE': None}, \
          {'file': '/lhcb/sub/file1.data', 'targetSE': 'targetSE1', 'extra': 'src_file = src_file1', 'srcSE': None}, \
          {'file': '/lhcb/sub/file3.data', 'targetSE': 'targetSE1', 'extra': 'src_file = src_file3', 'srcSE': None}]
    ret = extractArgsExecuteSE( argsDictExecuteSE , *argumentsExecuteSE )['Value']
    for x in range( len( ret ) ):
      self.assertEqual( ret[x]['file'], ok[x]['file'] )
      self.assertEqual( ret[x]['targetSE'], ok[x]['targetSE'] )
      self.assertEqual( ret[x]['srcSE'], ok[x]['srcSE'] )
      self.assertEqual( ret[x]['extra'], ok[x]['extra'] )

  def test_Error( self ):
    ret = extractTupleArgs( argsDictExecuteSE, *argumentsExecuteSE )
    self.assertEqual( ret['OK'], False )
    ret = extractArgs( argsDictExecuteSE , *argumentsExecuteSE )
    self.assertEqual( ret['OK'], False )



if __name__ == "__main__":

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( DefaultCase )

  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ExecuteFCCase ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ExecuteSECase ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TupleCase ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SetReplicaProblematicCase ) )

  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
