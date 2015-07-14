'''
Created on Jun 9, 2015

@author: Corentin Berger
'''

from DIRAC.Core.Base import Script

lfn = None
IDSeq = None
fullFlag = False
callerName = None
after = None
before = None
status = None
extra = []

Script.registerSwitch( '', 'Full', 'Print full method call' )
Script.registerSwitch( 'f:', 'File=', 'Name of LFN [%s]' % lfn )
Script.registerSwitch( 'i:', 'ID=', 'ID of sequence [%s]' % IDSeq )
Script.registerSwitch( 'n:', 'Name=', 'Name of caller [%s]' % callerName )
Script.registerSwitch( 'a:', 'After=', 'Date, format be like 1999-12-31 [%s]' % after )
Script.registerSwitch( 'b:', 'Before=', 'Date, format be like 1999-12-31 [%s]' % before )
Script.registerSwitch( 'w:', 'Status=', 'Failed, Successful or Unknown [%s]' % status )
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'USAGE:',
                                     ' %s [OPTION|CFGFILE] -l LFN -m NAME' % Script.scriptName,
                                     'ARGUMENTS:',
                                     'At least one shall be given\nLFN: AN LFN NAME \ID : A sequence ID',
                                     'You can pass some extra args but not with a shortcut example : --JobID 14500' ] ) )

Script.parseCommandLine( ignoreErrors = False )

for switch in Script.getUnprocessedSwitches():
  if switch[0] == "f" or switch[0].lower() == "file":
    lfn = switch[1]
  elif switch[0] == "i" or switch[0].lower() == "id":
    IDSeq = switch[1]
  elif switch[0] == "n" or switch[0].lower() == "name":
    callerName = switch[1]
  elif switch[0] == "a" or switch[0].lower() == "after":
    after = switch[1]
  elif switch[0] == "b" or switch[0].lower() == "before":
    before = switch[1]
  elif switch[0] == "w" or switch[0].lower() == "status":
    status = switch[1]
  elif switch[0].lower() == "full":
    fullFlag = True
  else :
    extra.append( ( switch[0], switch[1] ) )

from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient

def printSequence( seq, full = False ):
  seqLines = []
  line = 'Sequence %s Caller %s Extra : ' % ( seq.sequenceID, seq.caller.name )
  for key, value in seq.extra.items() :
    line += '%s = %s, ' % ( key, value )
  seqLines.append( line )
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
    ( mc.name.name, mc.creationTime )
    seqLines.append( line )
    for action in mc.actions :
      line = ''
      for x in range( cpt + 1 ):
        line += '\t'
      if full :
        line += '\t%s%s%s%s%s%s'\
          % ( '%s' % action.status,
              ', file %s ' % action.fileDL.name if action.fileDL else '',
              ', sourceSE %s ' % action.srcSE.name if action.srcSE else '',
              ', targetSE %s ' % action.targetSE.name if action.targetSE else '',
              ', extra %s ' % action.extra if action.extra else '',
              ', errorMessage %s ' % action.errorMessage if action.errorMessage else '' )
      else :
        line += '\t%s%s%s%s'\
            % ( '%s' % action.status,
                ', file %s ' % action.fileDL.name if action.fileDL else '',
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
    ( mc.name.name, mc.creationTime )
    for action in mc.actions :
      if action.fileDL.name == lfn:
        line = base
        if full :
          line += '%s%s%s%s%s'\
              % ( '%s' % action.status,
                  ', sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                  ', targetSE %s ' % action.targetSE.name if action.targetSE else '',
                  ', extra %s ' % action.extra if action.extra else '',
                  ', errorMessage %s ' % action.errorMessage if action.errorMessage else '' )
          seqLines.append( line )
        else :
          line += '%s%s%s'\
              % ( '%s' % action.status,
                  ', sourceSE %s ' % action.srcSE.name if action.srcSE else '',
                  ', targetSE %s ' % action.targetSE.name if action.targetSE else '' )
          seqLines.append( line )
    for child in mc.children :
      stack.append( [child, cpt + 1] )
  return '\n'.join( seqLines )


if not lfn and not IDSeq and not callerName :
  print 'you should give at least one lfn, one sequence ID or one caller name'
else :
  dlc = DataLoggingClient()
  if lfn :
    res = dlc.getSequenceOnFile( lfn, before, after, status, extra )
    if res['OK']:
      if not res['Value'] :
        print 'no sequence to print'
      else :
        for seq in res['Value'] :
          print printSequenceLFN( seq, lfn, full = fullFlag )
          print'\n'
    else :
      print res['Message']

  elif IDSeq :
    res = dlc.getSequenceByID( IDSeq )
    if res['OK']:
      if not res['Value'] :
        print 'no sequence to print'
      else :
        for seq in res['Value'] :
          print printSequence( seq, full = fullFlag )
          print'\n'
    else :
      print res['Message']

  elif callerName :
    res = dlc.getSequenceByCaller( callerName, before, after, status, extra )
    if res['OK']:
      if not res['Value'] :
        print 'no sequence to print'
      else :
        for seq in res['Value'] :
          print printSequence( seq, full = fullFlag )
          print'\n'
    else :
      print res['Message']
