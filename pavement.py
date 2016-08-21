from paver.easy import *

data_dir = "data"

def is_running():
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

@task
@needs(["stop"])
def clean():
	path("data").rmtree_p()

@task
@needs(["init"])
def start():
	pid = is_running()
	if not pid:
		sh("mongod -f mongodb.conf &")
	else:
		print("an instance is running")

@task
@needs(["init"])
def stop():
	pid = is_running()
	if pid:
		sh("kill %d" % pid)
	else:
		print("no instance running")

@task
@needs(["init"])
def status():
	pid = is_running()
	if pid:
		print("mongodb running at %d" % pid)
	else:
		print("mongodb is not running")

@task
def init():
	data = path(data_dir)
	if not data.exists():
		data.mkdir_p()
