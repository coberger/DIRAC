'''
Created on Jun 9, 2015

@author: Corentin Berger
'''

from DIRAC.Core.Base import Script

lfn = None
IDSeq = None
fullFlag = False

Script.registerSwitch( '', 'Full', '   Print full method call' )
Script.registerSwitch( 'f:', 'LFN=', 'Name of LFN [%s]' % lfn )
Script.registerSwitch( 'i:', 'ID=', 'ID of sequence [%s]' % IDSeq )
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'USAGE:',
                                     ' %s [OPTION|CFGFILE] -l LFN -m NAME' % Script.scriptName,
                                     'ARGUMENTS:',
                                     'At least one shall be given\nLFN: AN LFN NAME \ID : A sequence ID' ] ) )

Script.parseCommandLine( ignoreErrors = False )

for switch in Script.getUnprocessedSwitches():
  if switch[0] == "f" or switch[0].lower() == "lfn":
    lfn = switch[1]
  if switch[0] == "i" or switch[0].lower() == "ID":
    IDSeq = switch[1]
  if switch[0].lower() == "full":
    fullFlag = True

from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient

def printSequence( seq, full = False ):
  seqLines = []
  seqLines.append( 'Sequence %s Caller %s' % ( seq.sequenceID, seq.caller.name ) )
  stack = list()
  stack.append( [seq.methodCalls[0], 1] )
  while len( stack ) != 0 :
    el = stack.pop()
    mc = el[0]
    cpt = el[1]
    line = ''
    for x in range( cpt ):
      line += '\t'
    line += '%s %s ' % \
    ( mc.creationTime, mc.name.name )
    seqLines.append( line )
    for action in mc.actions :
      line = ''
      for x in range( cpt + 1 ):
        line += '\t'
      if full :
        line += '\t%s%s%s%s%s%s'\
          % ( '%s' % action.status.name,
              ', file %s ' % action.file.name if action.file else '',
              ', sourceSE %s ' % action.srcSE.name if action.srcSE else '',
              ', targetSE %s ' % action.targetSE.name if action.targetSE else '',
              ', blob %s ' % action.blob if action.blob else '',
              ', errorMessage %s ' % action.messageError if action.messageError else '' )
      else :
        line += '\t%s%s%s%s'\
            % ( '%s' % action.status.name,
                ', file %s ' % action.file.name if action.file else '',
                ', sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                ', targetSE %s ' % action.targetSE.name if action.targetSE else '' )
      seqLines.append( line )

    for child in reversed( mc.children ) :
      stack.append( [child, cpt + 1] )
  return '\n'.join( seqLines )


def printSequenceLFN( seq, lfn, full = False ):
  seqLines = []
  seqLines.append( 'Sequence %s Caller %s' % ( seq.sequenceID, seq.caller.name ) )
  cpt = 1
  stack = list()
  stack.append( [seq.methodCalls[0], 1] )
  while len( stack ) != 0 :
    el = stack.pop()
    mc = el[0]
    cpt = el[1]
    base = ''
    for x in range( cpt ):
      base += '\t'
    base += '%s %s, ' % \
    ( mc.creationTime, mc.name.name )
    for action in mc.actions :
      if action.file.name == lfn:
        line = base
        if full :
          line += '%s%s%s%s%s'\
              % ( '%s' % action.status.name,
                  ', sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                  ', targetSE %s ' % action.targetSE.name if action.targetSE else '',
                  ', blob %s ' % action.blob if action.blob else '',
                  ', errorMessage %s ' % action.messageError if action.messageError else '' )
          seqLines.append( line )
        else :
          line += '%s%s%s'\
              % ( '%s' % action.status.name,
                  ', sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                  ', targetSE %s ' % action.targetSE.name if action.targetSE else '' )
          seqLines.append( line )
    for child in mc.children :
      stack.append( child )
  return '\n'.join( seqLines )


if not lfn and not IDSeq :
  print 'you should give at least one lfn or one sequence ID'
else :
  dlc = DataLoggingClient()
  if lfn :
    res = dlc.getSequenceOnFile( lfn )
    if res['OK']:
      for seq in res['Value'] :
        print printSequenceLFN( seq, lfn, full = fullFlag )
    else :
      print res['Value']
  elif id :
    res = dlc.getSequenceByID( IDSeq )
    if res['OK']:
      for seq in res['Value'] :
        print printSequence( seq, full = fullFlag )
    else :
      print res['Value']
