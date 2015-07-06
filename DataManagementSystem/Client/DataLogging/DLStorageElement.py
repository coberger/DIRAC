'''
Created on May 18, 2015

@author: Corentin Berger
'''

from DIRAC.DataManagementSystem.private.DLJSON import DLJSON

class DLStorageElement( DLJSON ):

  attrNames = ['name']

  def __init__( self, name ):
    self.name = name

