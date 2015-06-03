'''
Created on May 20, 2015

@author: Corentin Berger
'''
import unittest

from DIRAC.DataManagementSystem.Client.DataLoggingDecorator import funcDict
from DIRAC.DataManagementSystem.Client.DataLoggingException import DataLoggingException

argsDictDefault = {}
argsDictDefault['argsPosition'] = [ 'files', 'localPath', 'targetSE' ]
argumentsDefault = ( [18, 19, 20], '/local/path/', 'destSE' )

argsDictTuple = {}
argsDictTuple['argsPosition'] = ( ['tuple' ] )
argsDictTuple['tupleArgsPosition'] = ['files', 'physicalFile', 'fileSize', 'targetSE', 'fileGuid', 'checksum' ]
argumentsTuple = [( ( 'M', 'destUrl', 150, 'destinationSE', 40, 108524789 ),
                 ( 'TITI', 'targetURL', 7855, 'TargetSE', 14, 155 ) )]


argsDictExecuteFC = {}
lfnsFC = {'lfn1':{'PFN':'PFN1', 'Size':'1', 'SE':'se1', 'GUID':'1', 'Checksum':'1'},
        'lfn2':{'PFN':'PFN2', 'Size':'2', 'SE':'se2', 'GUID':'2', 'Checksum':'2'},
        'lfn3':{'PFN':'PFN3', 'Size':'3', 'SE':'se3', 'GUID':'3', 'Checksum':'3'}  }
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
lfnsSetReplicaProblematic = {'L2': {'S20': 'P20'}, 'L3': {'S30': 'P30', 'S31': 'P31', 'S32': 'P32'}, 'L1': {'S10': 'P10', 'S11': 'P11'}}
argumentsSetReplicaProblematic = ( 'self', lfnsSetReplicaProblematic )
argsDictSetReplicaProblematic['call'] = 'setReplicaProblematic'
argsDictSetReplicaProblematic['methods_to_log'] = ['setReplicaProblematic']
argsDictSetReplicaProblematic['methods_to_log_arguments'] = {
              'setReplicaProblematic' :
                {'Arguments' : ['self', 'files'],
                 'specialFunction' : 'setReplicaProblematic' }
              }


def callFunction( function , argsDict, *args, **kwargs ):
  try :
    ret = function( argsDict, *args, **kwargs )
  except Exception as e:
    raise DataLoggingException( e )

  return ret


class DataLoggingArgumentsParsingTestCase( unittest.TestCase ):
  pass


class DefaultCase ( DataLoggingArgumentsParsingTestCase ):

  def test_DictEqual( self ):
    ok = [{'files': 18, 'targetSE': 'destSE', 'blob': 'localPath = /local/path/', 'srcSE': None}, {'files': 19, 'targetSE': 'destSE', 'blob': 'localPath = /local/path/', 'srcSE': None}, {'files': 20, 'targetSE': 'destSE', 'blob': 'localPath = /local/path/', 'srcSE': None}]
    getArgs = funcDict['normal']
    ret = callFunction( getArgs, dict( argsDictDefault ), *argumentsDefault )
    self.assertEqual( ret, ok )

  def test_ExceptionRaise( self ):
    getArgs = funcDict['executeSE']
    self.assertRaises( DataLoggingException, callFunction, getArgs, dict( argsDictDefault ), *argumentsDefault )
    getArgs = funcDict['tuple']
    self.assertRaises( DataLoggingException, callFunction, getArgs, dict( argsDictDefault ), *argumentsDefault )



class TupleCase ( DataLoggingArgumentsParsingTestCase ):

  def test_DictEqual( self ):
    ok = [{'files': 'M', 'targetSE': 'destinationSE', 'blob': 'physicalFile = destUrl,fileSize = 150,fileGuid = 40,checksum = 108524789', 'srcSE': None}, {'files': 'TITI', 'targetSE': 'TargetSE', 'blob': 'physicalFile = targetURL,fileSize = 7855,fileGuid = 14,checksum = 155', 'srcSE': None}]
    getArgs = funcDict['tuple']
    ret = callFunction( getArgs, dict( argsDictTuple ), *argumentsTuple )

    self.assertEqual( ret, ok )

  def test_ExceptionRaise( self ):
    getArgs = funcDict['executeSE']
    self.assertRaises( DataLoggingException, callFunction, getArgs, dict( argsDictTuple ), *argumentsTuple )

  def test_TupleAsNone( self ):
    getArgs = funcDict['tuple']
    self.assertRaises( DataLoggingException, callFunction, getArgs, dict( argsDictTuple ), [None] )


class ExecuteSECase ( DataLoggingArgumentsParsingTestCase ):
  def test_DictEqual( self ):
    ok = [{'files': 'lfn1', 'targetSE': 'se1', 'blob': 'Size = 1,GUID = 1,Checksum = 1,PFN = PFN1', 'srcSE': None}, {'files': 'lfn2', 'targetSE': 'se2', 'blob': 'Size = 2,GUID = 2,Checksum = 2,PFN = PFN2', 'srcSE': None}, {'files': 'lfn3', 'targetSE': 'se3', 'blob': 'Size = 3,GUID = 3,Checksum = 3,PFN = PFN3', 'srcSE': None}]
    getArgs = funcDict['executeFC']
    ret = callFunction( getArgs, dict( argsDictExecuteFC ), *argumentsExecuteFC )
    self.assertEqual( ret, ok )

  def test_ExceptionRaise( self ):
    getArgs = funcDict['tuple']
    self.assertRaises( DataLoggingException, callFunction, getArgs, dict( argsDictExecuteFC ), *argumentsExecuteFC )
    getArgs = funcDict['default']
    self.assertRaises( DataLoggingException, callFunction, getArgs, dict( argsDictExecuteFC ), *argumentsExecuteFC )



class SetReplicaProblematicCase( DataLoggingArgumentsParsingTestCase ):

  def test_DictEqual( self ):
    ok = [{'files': 'L2', 'targetSE': 'S20', 'blob': 'PFN = P20', 'srcSE': None}, {'files': 'L3', 'targetSE': 'S31', 'blob': 'PFN = P31', 'srcSE': None}, {'files': 'L3', 'targetSE': 'S30', 'blob': 'PFN = P30', 'srcSE': None}, {'files': 'L3', 'targetSE': 'S32', 'blob': 'PFN = P32', 'srcSE': None}, {'files': 'L1', 'targetSE': 'S11', 'blob': 'PFN = P11', 'srcSE': None}, {'files': 'L1', 'targetSE': 'S10', 'blob': 'PFN = P10', 'srcSE': None}]
    getArgs = funcDict['executeFC']
    ret = callFunction( getArgs, dict( argsDictSetReplicaProblematic ), *argumentsSetReplicaProblematic )
    self.assertEqual( ret, ok )

if __name__ == "__main__":

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( DefaultCase )

  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TupleCase ) )
  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ExecuteSECase ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SetReplicaProblematicCase ) )

  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
