#!/usr/bin/env python
#
# mega2sw_lod_start.py
# (based on mega2sw_lod_start.pl)
# Start script for genome wide SIMWALK2 calculation with sets of SNPs
# (c) 2004 Franz Rueschendorf 
# 6-Apr-2005 Adapted by John E. Landers, 
# 22-June-2009 ported to python

import os.path, os, re, shutil, commands, sys, getopt
import threading

DEBUG = False
sp = re.compile("^datain_(\d+)\..*")

def dbg(s) :
	if DEBUG :
		print >> sys.stderr, s

def usage():
	print >> sys.stderr, "Usage: %s [-nX] (where X is the number of cores herzog is allowed to use)" % sys.argv[0]

# finds out the number of cores
# creates a semaphore of the same number
# runs threads on temporary directories created
# as part of running mega2
# when thread dies a semaphore is released
def main() :
	global DEBUG
	ncores = -1

	try : 
		opts,args = getopt.getopt(sys.argv[1:], "vhn:")
	except getopt.GetoptError, err:
		print >> sys.stderr, str(err)
		usage()
		sys.exit(-1)
	for o,a in opts :
		if o == "-v" :
			DEBUG = True
		elif o == "-h" :
			usage()
			sys.exit()
		elif o == "-n" :
			try :
				ncores = int(a)
			except :
				usage()
				sys.exit(-1)


	if ncores == -1 :
		ncores = number_of_cores()
	
	dbg("[main] using %d cores..." % ncores)

	#os.system('date')
	jobs = parse_genome()
	#os.system('date')
	job_lock = threading.RLock()

	dbg("[main] %d separate runnable units..." % len(jobs))

	threads = []
	for i in range(ncores) :
		t = threading.Thread(target=simwalk_wrapper, args=(i,jobs,job_lock))
		threads.append(t)

	for t in threads :
		t.start()

def batch_rename(fl) :
	for src,dst in fl :
		try :
			os.rename(src,dst)
		except OSError, e :
			print >> sys.stderr, "could not rename %s to %s : %s" % (src,dst,e.strerror)

def batch_unlink(fl) :
	for f in fl :
		try :
			os.unlink(f)
		except OSError, e :
			print >> sys.stderr, "could unlink %s : %s" % (f, e.strerror)

def number_of_cores() :
	s,o = commands.getstatusoutput("grep \"processor\" /proc/cpuinfo | wc -l")
	if s == 0 :
		return int(o.strip())

	print >> sys.stderr, "could not assertain the number of cores... (%s, %s)" % (str(s), str(o))
	print >> sys.stderr, "(use the -n flag to set this explicitly)"
	
	sys.exit(-1)

def simwalk_wrapper(id, jobs, job_lock) :
	while True :
		job_lock.acquire()
		
		if len(jobs) == 0 :
			job_lock.release()
			sys.exit()
	
		dir_tuple = jobs.pop()
		job_lock.release()

		chrdir,tmpdir = dir_tuple
		if os.path.exists("%s/SCORE-%s_%d.ALL" % (chrdir, chrdir[1:], int(tmpdir))) :
			dbg("[simwalk_wrapper] skipping SCORE-%s_%d.ALL, results file exists..." % (chrdir[1:], int(tmpdir)))
			continue

		dbg("[simwalk_wrapper] thread %d starting simwalk" % id)
		simwalk(dir_tuple,id)

def simwalk(dir_tuple, id) :
	chrdir,tmpdir = dir_tuple

	dbg("[simwalk] running simwalk in %s/%s..." % (chrdir,tmpdir))

#	pid = os.fork()
#	if pid == 0 :
#		os.chdir(chrdir)
#		os.chdir(tmpdir)
#
#		command = "simwalk2"
#		#os.execlp("simwalk2","simwalk2")
#		#dbg("forked (%d)" % id)
#		sys.exit(os.system("simwalk2 > /dev/null 2>&1"))
#	
#	#dbg("pre-waitpid (%d)" % id)
#	try :
#		pid2, status = os.waitpid(pid,0)
#	except :
#		pass
#		#dbg("WARNING! os..waitpid exception")
#	
#	#dbg("post-waitpid (%d)" % id)
#
#	#if status != 0 :
#	#	dbg("bad return simwalk (%d)" % id)
#	#	print >> sys.stderr, "simwalk2 did not run properly : return code %d" % status
#	#	os._exit(-1)

	os.system("cd %s/%s ; simwalk2 > /dev/null 2> /dev/null ; cd -" % (chrdir,tmpdir))

	try :
		dbg("cleanup start (%d)" % id)
		os.rename("%s/%s/SCORE-%s.ALL" % (chrdir,tmpdir,chrdir[1:]), \
				"%s/%s/SCORE-%s_%d.ALL" % (chrdir,tmpdir,chrdir[1:],int(tmpdir)))

#	fl = ["datain.%s" % chr,"pedin.%s" % chr,"map.%s" % chr,"mega2_in.tmp"]
#	for f in os.listdir('.') :
#	if f.endswith(".out") or f.startswith("MEGA") or f.endswith(".sh") :
#		fl.append(f)
#	
#	batch_unlink(fl)

		shutil.copy("%s/%s/SCORE-%s_%d.ALL" % (chrdir,tmpdir,chrdir[1:],int(tmpdir)), "%s/" % chrdir)
		shutil.rmtree("%s/%s" % (chrdir,tmpdir))
		dbg("cleanup end (%d)" % id)
	except :
		dbg("[simwalk] cleanup failed (%s/%s)" % (chrdir,tmpdir))
	
	dbg("[simwalk] %s/%s complete..." % (chrdir,tmpdir))

def write_mega2_input(extension) :
	try :
		f = open("mega2_in.tmp", 'a')
		#f = open(filename, 'a')
	except :
		print >> sys.stderr, "could not open mega2_in.tmp"
		sys.exit(-1)

	print >> f, "1\n%s\n0\n1\n2\n0\n0\n0" % extension
	f.close()

def run_mega2(extension) :
	command = "mega2 < mega2_in.tmp"
	status,out = commands.getstatusoutput(command)
	if status != 0 :
		print >> sys.stderr, "mega2 did not run properly : return code %d" % status
		sys.exit(-1)

	#batch_unlink(["PEDIGREE.DAT","LOCUS.DAT","PEN.DAT","BATCH2.DAT"])
	
	batch_rename([
		("sw2_pedigree.%s" % extension, "PEDIGREE.DAT"),
		("sw2_locus.%s" % extension,	"LOCUS.DAT"),
		("sw2_pen.%s" % extension,	"PEN.DAT"),
		("sw2_batch.%s" % extension,	"BATCH2.DAT"),
		("sw2_map.%s" % extension,	"MAP.DAT")
	])

def parse_genome() :
	blocks = []
	
	for chr in map(lambda x : "%02d" % x , range(1,25)) :
		chrdir = "c%s" % chr
		if os.path.exists(chrdir) and os.path.isdir(chrdir) :
			os.chdir(chrdir)
			dbg("[parse_genome] entering %s..." % chrdir)

			write_mega2_input(chr)

			filelist = filter(lambda x : x.startswith("datain_") and x.endswith(chr), os.listdir('.'))
			set  = max(map(lambda x : int(sp.match(x).group(1)), filelist))

			# nb: this is the basic unit of concurrency
			for i in range(1, set + 1) :
				tmpdir = str(i)
				blocks.append((chrdir,tmpdir))
				
				try :
					shutil.rmtree(tmpdir) # XXX
				except :
					pass
				os.mkdir(tmpdir)
				os.chdir(tmpdir)
				#dbg("[parse_genome]\tcreated %s, entering..." % tmpdir)

				shutil.copy("../datain_%d.%s" % (i, chr), "datain.%s" % chr)
				shutil.copy("../pedin_%d.%s" % (i, chr),  "pedin.%s" % chr)
				shutil.copy("../map_%d.%s" % (i, chr), 	  "map.%s" % chr)

				shutil.copy("../mega2_in.tmp", '.')
			
				#dbg("[parse_genome] running mega2")
				run_mega2(chr)	

				os.chdir("..")

			os.chdir("..")
		else :
			dbg("[parse_genome] no chromosome %s" % chr)
	
	return blocks

if __name__ == '__main__' :
	main()

