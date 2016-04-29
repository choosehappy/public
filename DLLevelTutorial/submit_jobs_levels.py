#!/usr/bin/env python
import os
import glob
import subprocess
import commands


basefile_solver='BASE-alexnet_solver_ada.prototxt'
basefile_train='BASE-alexnet_traing_32w_db.prototxt'
basefile_qsub='BASE-qsub.pbs'
workingdir='/home/axj232/code/nuclei_level/models'


f = open(basefile_solver, 'r')
template_solver_text=f.read()
f.close()


f = open(basefile_train, 'r')
template_train_text=f.read()
f.close()


f = open(basefile_qsub, 'r')
template_qsub=f.read()
f.close()



os.chdir(workingdir)

for kfoldi in xrange(1,6):
	for leveli in xrange(1,5):
		out=commands.getstatusoutput("wc -l ../test_w32_%d_%d.txt"%(kfoldi,leveli))
		numiter=int(out[1].split()[0])/128

		specific_solver_text=template_solver_text % {'kfoldi': kfoldi,'leveli' : leveli,'numiter': numiter}
		specific_train_text=template_train_text %  {'kfoldi': kfoldi,'leveli' : leveli}
		specific_qsub=template_qsub %   {'kfoldi': kfoldi,'leveli' : leveli}
		
		foutname=basefile_solver
		foutname=foutname.replace('BASE',"%d-%d" %(kfoldi,leveli))
		fout = open(foutname,'w')
		fout.write(specific_solver_text)
		fout.close()		
		
		
		foutname=basefile_train
		foutname=foutname.replace('BASE',"%d-%d" %(kfoldi,leveli))
		fout = open(foutname,'w')
		fout.write(specific_train_text)
		fout.close()		
		
		
		sp = subprocess.Popen(["qsub",""], shell=False, stdin=subprocess.PIPE)
		print sp.communicate(specific_qsub)
		sp.wait()

