'''
Created on Jun 19, 2015

@author: Corentin Berger
'''
from datetime import datetime

class DLCompressedSequence( object ):

  def __init__( self, value ):
    self.value = value
    self.creationTime = datetime.now()
    self.insertionTime = None
    self.status = 'Waiting'
