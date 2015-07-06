'''
Created on May 4, 2015

@author: Corentin Berger
'''

from DIRAC.DataManagementSystem.private.DLJSON import DLJSON

class DLFile( DLJSON ):
  """ this is the class for data logging system which is like lfn"""
  attrNames = ['name']

  def __init__( self, name ):
    self.name = name