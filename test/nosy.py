# Based on initial Nosy by Jeff Winkler, http://jeffwinkler.net

import glob
import os
import stat
import time 
import sys

'''
Watch for changes in all .py files, in all subdirectories. If changes, run nosetests.
'''

def checkSum():
    ''' Return a long which can be used to know if any .py files have changed.
    Only looks in the current directory. '''
    val = 0
    for f in glob.glob ('*.py'):
        stats = os.stat (f)
        val += stats [stat.ST_SIZE] + stats [stat.ST_MTIME]
    for f in glob.glob ('*.kid'):
        stats = os.stat (f)
        val += stats [stat.ST_SIZE] + stats [stat.ST_MTIME]
    return val

def checkSumRecurse():
	''' Return a long which can be used to know if any .py files have changed.
	Looks in subdirectories.
	Contributed to nosy by Kevin Dahlhausen'''
	val = 0
	for dirpath, dirs, files in os.walk("."):
		for file in [file for file in files if file[-3:]==".py"]:
			absoluteFileName = os.path.join( dirpath, file)
			stats = os.stat(absoluteFileName)
			val += stats[stat.ST_SIZE] + stats[stat.ST_MTIME]
	return val


def main(argv=None):
    """call example: nosy test_authority.py -A \'not slow and not online\'  
    Note:  Attribute flags set at the nosy command line will override those set
    in this file, it looks like.  The last -A gets precedence?
    """

    print("argv", argv)

    nosecommand = 'nosetests'
#    nosecommand += ' -A "not slow and not online" '    
#    nosecommand += ' --nocapture'
#    nosecommand += ' --with-doctest'
#    nosecommand += ' --where=tests'    
#    nosecommand += ' --config=tests/nose.config'    
    nosecommand += ' --rednose'  
    if "--no-progressive" not in argv: 
        nosecommand += ' --with-progressive'    
#    other useful flags:  -d --with-doctest --with-profile --with-isolation --nocapture
    # --decorator-file tests/nose_test_decorators.config  # for use with pinocchio decorators

    argstring = " ".join([arg for arg in argv if arg != "--no-progressive"])
    nosecommand_plus_args = nosecommand + " " + argstring

    print("\n", os.getcwd())
    print(nosecommand_plus_args)

    val=0
    check = checkSumRecurse
    while (True):
    	if check() != val:
    		val=check()
    		print("*************************")
    		print(time.ctime())
    		os.system(nosecommand_plus_args)				
    	time.sleep(1)

if __name__ == "__main__":
    main(sys.argv[1:])
