from paver.easy import *
import time

data_dir = "data"

def is_db_running():
	lock_file = path(data_dir)/"mongod.lock"
	if not lock_file.exists():
		return False
	try:
		lock_content = lock_file.bytes()
		if lock_content == b'':
			return False
		return int(lock_content)
	except:
		return False

def require_db_running(func):
	from functools import wraps
	import inspect
	using_pid = False
	sig = inspect.signature(func)
	params = sig.parameters.copy()
	if 'pid' in params:
		using_pid = True
		params.pop('pid')
	@wraps(func)
	def new_func(**kwargs):
		pid = is_db_running()
		if pid:
			if using_pid:
				kwargs["pid"] = pid
			func(**kwargs)
		else:
			print("mongodb is not running")
	new_func.__signature__ = sig.replace(parameters = params.values())
	#print(func.__name__, inspect.getfullargspec(func), inspect.getfullargspec(new_func))
	return new_func

def namespace(func):
	[ns, name] = func.__name__.split('_', 1)
	func.__name__ = "%s.%s" % (ns, name)
	return func

@task
@needs(["db.stop"])
@namespace
def db_clean():
	"""remove data folder"""
	path(data_dir).rmtree_p()

@task
@needs(["db.init"])
@namespace
def db_start():
	"""start a mongod instance"""
	pid = is_db_running()
	if not pid:
		sh("mongod -f mongodb.conf &")
	else:
		print("an instance is running")

@task
@require_db_running
@namespace
def db_stop(pid):
	"""stop the mongod instance"""
	sh("kill %d" % pid)

@task
@needs(["db.init"])
@require_db_running
@namespace
def db_status(pid):
	"""get the mongod instance pid"""
	print("mongodb running at %d" % pid)

@task
@needs(["db.init"])
@consume_args
@require_db_running
@namespace
def db_run(args):
	"""run scripts in mongodb"""
	for script in args:
		sh("mongo 127.0.0.1:3269 %s" % script)

@task
@namespace
def db_init():
	"""init mongodb data dir"""
	data = path(data_dir)
	if not data.exists():
		data.mkdir_p()
		call_task('db.start')
		while not is_db_running():
			time.sleep(0.2)
		time.sleep(1)
		call_task('db.run', args = [ 'initdb.js' ])
		call_task('db.stop')
		while is_db_running():
			time.sleep(0.2)
