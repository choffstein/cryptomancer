# for parallel to work, picloud must be installed:
# > sudo pip install cloud
# > picloud setup
# Make sure you get a picloud key from tech support!

import multiprocessing
import numpy
import functools
import tqdm
import time

import inspect


def _escapable_child(f, *args):
	# help prevent any forking issues with seeding the RNG
	numpy.random.seed()

	try:
		return f(*args)
		
	except KeyboardInterrupt:
		return


def aync_run(f_args, pool_size = None, process = True):
	if pool_size == None:
		try:
			pool_size = multiprocessing.cpu_count()
			
		except NotImplementedError:
			pool_size = 1

	if process:
		# processes are not subject to the GIL
		pool = multiprocessing.Pool(processes = pool_size)
			
	else:
		# threads are
		pool = multiprocessing.pool.ThreadPool(processes = pool_size)

	try:
		async_results = []
		for f, args in f_args:
			partial_f = functools.partial(_escapable_child, f)
			async_result = pool.apply_async(partial_f, args)
			async_results.append(async_result)

		pool.close()
	
	except KeyboardInterrupt:
		pool.terminate()
		raise
		
	except Exception:
		pool.terminate()
		raise
		
	finally:
		pool.join()

	return [async_result.get() for async_result in async_results]



def lmap(f, lst, pool_size = None, process = True, progress_bar = False):
	"""Parallelize a map using local processes or threads.	Works well if the function we are offloading to is a C function, like an expensive pandas or numpy function.
	   
	   f should be a read-only function -- i.e. it doesn't manipulate local data"""
	
	if pool_size == None:
		try:
			pool_size = multiprocessing.cpu_count()
			
		except NotImplementedError:
			pool_size = 1

	if process:
		# processes are not subject to the GIL
		pool = multiprocessing.Pool(processes = pool_size)
			
	else:
		# threads are
		pool = multiprocessing.pool.ThreadPool(processes = pool_size)

	try:
		partial_f = functools.partial(_escapable_child, f)
		if progress_bar:
			l = list(tqdm.tqdm(pool.starmap(partial_f, lst), total=len(lst)))
		else:
			l = pool.starmap(partial_f, lst)
		pool.close()
	
	except KeyboardInterrupt:
		pool.terminate()
		raise
		
	except Exception:
		pool.terminate()
		raise
		
	finally:
		pool.join()
	
	return l

