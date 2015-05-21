'''
Created on May 20, 2015

@author: Corentin Berger
'''
import unittest

from DIRAC.DataManagementSystem.Client.DataLoggingDecorator import funcdict
from DIRAC.DataManagementSystem.Client.DataLoggingException import DataLoggingException

argsDictDefault = {}
argsDictDefault['argsPosition'] = [ 'files', 'localPath', 'targetSE' ]
argumentsDefault = ( [18, 19, 20], '/local/path/', 'destSE' )

argsDictTuple = {}
argsDictTuple['argsPosition'] = (['fileTuple' ])
argsDictTuple['tupleArgsPosition'] = ['files', 'physicalFile', 'fileSize', 'targetSE', 'fileGuid', 'checksum' ]
argumentsTuple = [( ( 'M', 'destUrl', 150, 'destinationSE', 40, 108524789 ),
                 ( 'TITI', 'targetURL', 7855, 'TargetSE', 14, 155 ) )]


argsDictExecute = {}
argumentsExecute = ( 'self', ['lfn3', 'lfn4'], 'titi' )
argsDictExecute['call'] = 'isFile'



def callFunction( function , argsDict, *args, **kwargs ):
  try :
    ret = function( argsDict, *args, **kwargs )
  except Exception as e:
    raise DataLoggingException( repr( e ) )

  return ret


class DataLoggingArgumentsParsingTestCase( unittest.TestCase ):
  pass


class DefaultCase ( DataLoggingArgumentsParsingTestCase ):

  def test_DictEqual( self ):
    ok = [{'files': 18, 'targetSE': 'destSE', 'blob': 'localPath = /local/path/', 'srcSE': None},
          {'files': 19, 'targetSE': 'destSE', 'blob': 'localPath = /local/path/', 'srcSE': None},
           {'files': 20, 'targetSE': 'destSE', 'blob': 'localPath = /local/path/', 'srcSE': None}]
    getArgs = funcdict['normal']
    ret = callFunction( getArgs, dict( argsDictDefault ), *argumentsDefault )

    self.assert_( ret, ok )

  def test_ExceptionRaise( self ):
    getArgs = funcdict['execute']
    self.assertRaises( DataLoggingException, callFunction, getArgs, dict( argsDictDefault ), *argumentsDefault )
    getArgs = funcdict['tuple']
    self.assertRaises( DataLoggingException, callFunction, getArgs, dict( argsDictDefault ), *argumentsDefault )



class TupleCase ( DataLoggingArgumentsParsingTestCase ):

  def test_DictEqual( self ):
    ok = [{'files': 'M', 'targetSE': 'destinationSE', 'blob': 'physicalFile = destUrl,fileSize = 150,fileGuid = 40,checksum = 108524789', 'srcSE': None},
         {'files': 'TITI', 'targetSE': 'TargetSE', 'blob': 'physicalFile = targetURL,fileSize = 7855,fileGuid = 14,checksum = 155', 'srcSE': None}]
    getArgs = funcdict['tuple']
    ret = callFunction( getArgs, dict( argsDictTuple ), *argumentsTuple )

    self.assert_( ret, ok )

  def test_ExceptionRaise( self ):
    getArgs = funcdict['execute']
    self.assertRaises( DataLoggingException, callFunction, getArgs, dict( argsDictTuple ), *argumentsTuple )
    getArgs = funcdict['default']
    self.assertRaises( DataLoggingException, callFunction, getArgs, dict( argsDictTuple ), *argumentsTuple )

  def test_TupleAsNone( self ):
    getArgs = funcdict['tuple']
    self.assertRaises( DataLoggingException, callFunction, getArgs, dict( argsDictTuple ), [None] )


class ExecuteCase ( DataLoggingArgumentsParsingTestCase ):
  def test_DictEqual( self ):
    ok = [{'files': 'lfn3', 'targetSE': None, 'blob': 'name = titi', 'srcSE': None},
           {'files': 'lfn4', 'targetSE': None, 'blob': 'name = titi', 'srcSE': None}]
    getArgs = funcdict['execute']
    ret = callFunction( getArgs, dict( argsDictExecute ), *argumentsExecute )

    self.assert_( ret, ok )

  def test_ExceptionRaise( self ):
    getArgs = funcdict['tuple']
    self.assertRaises( DataLoggingException, callFunction, getArgs, dict( argsDictExecute ), *argumentsExecute )
    getArgs = funcdict['default']
    print argsDictExecute
    self.assertRaises( DataLoggingException, callFunction, getArgs, dict( argsDictExecute ), *argumentsExecute )




if __name__ == "__main__":

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( DefaultCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TupleCase ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ExecuteCase ) )

  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
