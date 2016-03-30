#!/usr/bin/env python
import os
import glob
import subprocess
import commands


basefile_solver='BASE-alexnet_solver_ada.prototxt'
basefile_train='BASE-alexnet_traing_32w_db.prototxt'
basefile_qsub='BASE-qsub.pbs'
workingdir='PATH TO WHERE test_w32_XX.txt files are'


#open the template files
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

# for each of the folds, fill in the templates, save the, and submit them to HPC
for kfoldi in xrange(1,6):
	out=commands.getstatusoutput("wc -l ../test_w32_%d.txt"%kfoldi) #figure out how many testing iterations we need, this number is divided by 128, which is the testset batch size
	numiter=int(out[1].split()[0])/128

	#make the specific files
	specific_solver_text=template_solver_text % {'kfoldi': kfoldi,'numiter': numiter}
	specific_train_text=template_train_text %  {'kfoldi': kfoldi}
	specific_qsub=template_qsub %   {'kfoldi': kfoldi}
	
	#save them
	foutname=basefile_solver
	foutname=foutname.replace('BASE',str(kfoldi))
	fout = open(foutname,'w')
	fout.write(specific_solver_text)
	fout.close()		
	
	
	foutname=basefile_train
	foutname=foutname.replace('BASE',str(kfoldi))
	fout = open(foutname,'w')
	fout.write(specific_train_text)
	fout.close()		
	
	#use QSUB to submit them to HPC...can comment this out if you only want to generate files but not submit
	sp = subprocess.Popen(["qsub",""], shell=False, stdin=subprocess.PIPE)
	print sp.communicate(specific_qsub)
	sp.wait()

