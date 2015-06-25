'''
Created on Jun 11, 2015

@author: Corentin Berger
'''

import random, time, zlib
from threading import Thread

from DIRAC.DataManagementSystem.DB.DataLoggingDB import DataLoggingDB
from DIRAC.DataManagementSystem.Client.DLSequence import DLSequence
from DIRAC.DataManagementSystem.Client.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DLStatus import DLStatus
from DIRAC.DataManagementSystem.Client.DLStorageElement import DLStorageElement
from DIRAC.DataManagementSystem.Client.DLMethodName import DLMethodName


randomMax = 100000

dictLong = {'files': '/lhcb/data/file', 'targetSE': '/SE/Target/se',
 'blob': 'physicalFile = blablablablablabla ,fileSize = 6536589', 'srcSE': '/SE/SRC/src'}

def makeSequenceA():
  sequence = DLSequence()
  sequence.setCaller( 'longCallerName' )
  calls = []
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  sequence.popMethodCall()
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  sequence.popMethodCall()
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  sequence.popMethodCall()
  sequence.popMethodCall()

  files = []
  for x in range( 10 ):
    files.append( dictLong['files'] + str( random.randint( 0, randomMax ) ) + '.data' )

  sources = []
  for x in range( 10 ):
    sources.append( dictLong['srcSE'] + str( random.randint( 0, randomMax ) ) )

  targets = []
  for x in range( 10 ):
    targets.append( dictLong['targetSE'] + str( random.randint( 0, randomMax ) ) )

  for call in calls :
    for x in range( 5 ):
      call.addAction( DLAction( DLFile( files[x * 2] ) , DLStatus( 'Successful' ) ,
              DLStorageElement( sources[x * 2] ),
               DLStorageElement( targets[x * 2] ),
              dictLong['blob'], 'errorMessage' ) )
      call.addAction( DLAction( DLFile( files[x * 2 + 1 ] ) , DLStatus( 'Failed' ) ,
              DLStorageElement( sources[x * 2 + 1 ] ),
               DLStorageElement( targets[x * 2 + 1] ),
              dictLong['blob'], 'errorMessage' ) )
  return sequence


def makeSequenceB():
  sequence = DLSequence()
  sequence.setCaller( 'longCallerName' )
  calls = []

  files = []
  for x in range( 500 ):
    files.append( dictLong['files'] + str( random.randint( 0, randomMax ) ) + '.data' )

  sources = []
  for x in range( 500 ):
    sources.append( dictLong['srcSE'] + str( random.randint( 0, randomMax ) ) )

  targets = []
  for x in range( 500 ):
    targets.append( dictLong['targetSE'] + str( random.randint( 0, randomMax ) ) )

  for x in range( 500 ):
    calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName2' + str( random.randint( 0, randomMax ) ) )} ) )

  for x in range( 10 ):
    sequence.popMethodCall()

  for call in calls :
    for x in range( 500 ):
      call.addAction( DLAction( DLFile( files[x ] ) , DLStatus( 'Successful' ) ,
              DLStorageElement( sources[x] ),
               DLStorageElement( targets[x] ),
              dictLong['blob'], 'errorMessage' ) )
  return sequence



class SequenceA( Thread ):
  def __init__( self, nb = 10 ):
    super( SequenceA, self ).__init__()
    self.nb = nb

  def run( self ):
    db = DataLoggingDB()
    for x in range( 1000 ) :
      sequence = makeSequenceA()
      sequenceJSON = sequence.toJSON()
      if not sequenceJSON["OK"]:
        return sequenceJSON
      sequenceJSON = sequenceJSON['Value']
      seq = zlib.compress( sequenceJSON )
      res = db.insertCompressedSequence( seq )
      if not res['OK']:
        print 'res %s' % res['Message']
        return res

class SequenceB( Thread ):

  def run( self ):
    db = DataLoggingDB()
    for x in range( 1000 ) :
      sequence = makeSequenceB()
      sequenceJSON = sequence.toJSON()
      if not sequenceJSON["OK"]:
        return sequenceJSON
      sequenceJSON = sequenceJSON['Value']
      seq = zlib.compress( sequenceJSON )
      res = db.insertCompressedSequence( seq )
      if not res['OK']:
        print 'res %s' % res['Message']
        return res

begin = time.time()
insertions = []
for x in range( 10 ) :
    insertions.append( SequenceA() )
    insertions[x].start()

for x in range( 10 ) :
  insertions[x].join()


print 'end, time : %s' % time.strftime( '%M : %S', time.localtime( time.time() - begin ) )
