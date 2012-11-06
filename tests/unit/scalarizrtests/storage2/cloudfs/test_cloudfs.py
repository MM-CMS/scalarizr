'''
Created on Sep 19, 2012

@author: Dmytro Korsakov
'''
import mock
from Queue import Empty
from os.path import basename
from subprocess import call

from nose.tools import assert_raises

from scalarizr.storage2 import cloudfs


class NTestFileTransfer(object):

	path0 = '/mnt/backups/daily0.tar.gz'
	path1 = '/mnt/backups/daily.tar.gz'
	path2 = 's3://backups/mysql/2012-09-05/'

	def make_generator(self, *args):
		def generator():
			for arg in args:
				yield arg
		return generator

	def setup(self):
		cloudfs.cloudfs = mock.MagicMock()

	def test_job_generator(self):
		# the simplest case (str str)
		obj = cloudfs.FileTransfer(src=self.path1, dst=self.path2)
		res = [job for job in obj._job_generator()]

		assert res == [(self.path1, self.path2, 0, -1)], res

		# iter str multipart
		src_gen = self.make_generator(self.path0, self.path1, self.path0)
		obj = cloudfs.FileTransfer(src=src_gen, dst=self.path2, multipart=True)
		res = [job for job in obj._job_generator()]

		assert res == [
			(self.path0, self.path2, 0, 0),
			(self.path1, self.path2, 0, 1),
			(self.path0, self.path2, 0, 2),
		], res

		# iter str not multipart
		src_gen = self.make_generator(self.path0, self.path1)
		obj = cloudfs.FileTransfer(src=src_gen, dst=self.path2)
		res = [job for job in obj._job_generator()]

		assert res == [
			(self.path0, self.path2, 0, -1),
			(self.path1, self.path2, 0, -1),
		], res

		# iter iter
		src_gen = self.make_generator(self.path0, self.path1, self.path2)
		dst_list = [self.path2, self.path2, self.path1]
		obj = cloudfs.FileTransfer(src=src_gen, dst=dst_list)
		res = [job for job in obj._job_generator()]

		assert res == [
			(self.path0, self.path2, 0, -1),
			(self.path1, self.path2, 0, -1),
			(self.path2, self.path1, 0, -1),
		], res

	def test_job_generator_with_retries(self):
		# str str, first retry fail
		obj = cloudfs.FileTransfer(src=self.path1, dst=self.path2)
		res = []
		generator = obj._job_generator()

		src, dst, retry, chunk_num = generator.next()
		res.append((src, dst, retry, chunk_num))
		obj._retries_queue.put((src, dst, retry + 1, chunk_num))
		res.append(generator.next())

		assert_raises(StopIteration, generator.next)
		assert res == [
			(self.path1, self.path2, 0, -1),
			(self.path1, self.path2, 1, -1),
		], res

		# iter str multipart
		src_gen = self.make_generator(self.path0, self.path1, self.path0)
		obj = cloudfs.FileTransfer(src=src_gen, dst=self.path2, multipart=True)
		res = []
		generator = obj._job_generator()

		res.append(generator.next())
		src, dst, retry, chunk_num = generator.next()
		res.append((src, dst, retry, chunk_num))
		obj._retries_queue.put((src, dst, retry + 1, chunk_num))
		res.append(generator.next())
		res.append(generator.next())

		assert_raises(StopIteration, generator.next)
		assert res == [
			(self.path0, self.path2, 0, 0),
			(self.path1, self.path2, 0, 1),
			(self.path1, self.path2, 1, 1),
			(self.path0, self.path2, 0, 2),
		], res

	@mock.patch("scalarizr.storage2.cloudfs.os.path.isfile")
	@mock.patch("scalarizr.storage2.cloudfs.os.path.getsize")
	def test_worker_upload(self, getsize, isfile):
		# str str
		obj = cloudfs.FileTransfer(src=self.path1, dst=self.path2)

		ret = obj.run()

		driver = cloudfs.cloudfs.return_value

		assert all([call == mock.call("s3") for call in cloudfs.cloudfs.call_args_list])
		driver.put.assert_called_once_with(self.path1, self.path2)
		assert not driver.multipart_init.called
		assert not driver.multipart_put.called
		assert not driver.get.called
		assert not driver.multipart_abort.called
		assert not driver.multipart_complete.called
		assert ret["completed"] == [
				{"src": self.path1, "dst": self.path2, "chunk_num": -1,
				 "size": getsize.return_value},
		]
		assert ret["failed"] == []

		# iter str not multipart
		cloudfs.cloudfs.reset_mock()
		src_gen = self.make_generator(self.path0, self.path1, self.path0)
		obj = cloudfs.FileTransfer(src=src_gen, dst=self.path2)

		ret = obj.run()

		driver = cloudfs.cloudfs.return_value

		assert all([call == mock.call("s3") for call in cloudfs.cloudfs.call_args_list])
		driver.put.assert_any_call(self.path0, self.path2)
		driver.put.assert_any_call(self.path1, self.path2)
		driver.put.assert_any_call(self.path0, self.path2)
		assert not driver.multipart_init.called
		assert not driver.multipart_put.called
		assert not driver.get.called
		assert not driver.multipart_abort.called
		assert not driver.multipart_complete.called
		completed = [
			{"src": self.path0, "dst": self.path2, "chunk_num": -1,
			 "size": getsize.return_value},
			{"src": self.path1, "dst": self.path2, "chunk_num": -1,
			 "size": getsize.return_value},
			{"src": self.path0, "dst": self.path2, "chunk_num": -1,
			 "size": getsize.return_value},
		]
		assert len(ret["completed"]) == len(completed)
		for job in completed:
			assert job in ret["completed"]
		assert ret["failed"] == []

		# iter iter
		cloudfs.cloudfs.reset_mock()
		src_gen = self.make_generator(self.path0, self.path1, self.path0)
		dst2 = self.path2 + 'one/'
		dst3 = self.path2 + 'two/'
		dst_gen = self.make_generator(self.path2, dst2, dst3)
		obj = cloudfs.FileTransfer(src=src_gen, dst=dst_gen)

		ret = obj.run()

		driver = cloudfs.cloudfs.return_value

		assert all([call == mock.call("s3") for call in cloudfs.cloudfs.call_args_list])
		driver.put.assert_any_call(self.path0, self.path2)
		driver.put.assert_any_call(self.path1, dst2)
		driver.put.assert_any_call(self.path0, dst3)
		assert not driver.multipart_init.called
		assert not driver.multipart_put.called
		assert not driver.get.called
		assert not driver.multipart_abort.called
		assert not driver.multipart_complete.called
		completed = [
			{"src": self.path0, "dst": self.path2, "chunk_num": -1,
			 "size": getsize.return_value},
			{"src": self.path1, "dst": dst2, "chunk_num": -1,
			 "size": getsize.return_value},
			{"src": self.path0, "dst": dst3, "chunk_num": -1,
			 "size": getsize.return_value},
		]
		assert len(ret["completed"]) == len(completed)
		for job in completed:
			assert job in ret["completed"]
		assert ret["failed"] == []

	@mock.patch("scalarizr.storage2.cloudfs.os.path.isfile")
	@mock.patch("scalarizr.storage2.cloudfs.os.path.getsize")
	def test_worker_upload_multipart(self, getsize, isfile):
		src_gen = self.make_generator(self.path0, self.path1, self.path0)
		obj = cloudfs.FileTransfer(src=src_gen, dst=self.path2, multipart=True)

		ret = obj.run()

		driver = cloudfs.cloudfs.return_value
		upload_id = driver.multipart_init.return_value

		assert all([call == mock.call("s3") for call in cloudfs.cloudfs.call_args_list])
		driver.multipart_init.assert_called_once_with(self.path2,
			getsize.return_value)
		driver.multipart_put.assert_any_call(upload_id, 0, self.path0)
		driver.multipart_put.assert_any_call(upload_id, 1, self.path1)
		driver.multipart_put.assert_any_call(upload_id, 2, self.path0)
		assert not driver.put.called
		assert not driver.get.called
		assert not driver.multipart_abort.called
		driver.multipart_complete.assert_called_once_with(upload_id)
		completed = [
			{"src": self.path0, "dst": self.path2, "chunk_num": 0,
			 "size": getsize.return_value},
			{"src": self.path1, "dst": self.path2, "chunk_num": 1,
			 "size": getsize.return_value},
			{"src": self.path0, "dst": self.path2, "chunk_num": 2,
			 "size": getsize.return_value},
		]
		assert len(ret["completed"]) == len(completed)
		for job in completed:
			assert job in ret["completed"], str(job) + ' not in ' + str(ret["completed"])
		assert ret["failed"] == []

	@mock.patch("scalarizr.storage2.cloudfs.os.path.isfile")
	@mock.patch("scalarizr.storage2.cloudfs.os.path.getsize")
	def test_worker_upload_retry(self, getsize, isfile):
		# basic retry queue interaction
		src_gen = self.make_generator(self.path0, self.path1)
		obj = cloudfs.FileTransfer(src=src_gen, dst=self.path2)
		driver = cloudfs.cloudfs.return_value
		driver.put.side_effect = Exception("test exception")
		obj._retries_queue = mock.MagicMock()
		obj._retries_queue.get_nowait.side_effect = Empty("test exc")

		ret = obj.run()

		assert len(obj._retries_queue.put.call_args_list) == 2
		obj._retries_queue.put.assert_any_call((self.path0, self.path2, 1, -1))
		obj._retries_queue.put.assert_any_call((self.path1, self.path2, 1, -1))
		assert obj._retries_queue.get_nowait.call_args_list == [mock.call()] * 10

		# iter str multipart
		cloudfs.cloudfs.reset_mock()

		def side_effect(upload_id, chunk_num, src, firsttime=[True]):
			"""
			raise exception once on src=self.path1;
			for driver.multipart_put(self._upload_id, chunk_num, src)
			"""
			if src == self.path1 and firsttime[0]:
				firsttime[0] = False
				raise Exception("test exception")

		src_gen = self.make_generator(self.path0, self.path1, self.path0)
		obj = cloudfs.FileTransfer(src=src_gen, dst=self.path2, multipart=True)
		driver = cloudfs.cloudfs.return_value
		driver.multipart_put.side_effect = side_effect

		ret = obj.run()

		upload_id = driver.multipart_init.return_value

		assert all([call == mock.call("s3") for call in cloudfs.cloudfs.call_args_list])
		driver.multipart_init.assert_called_once_with(self.path2,
			getsize.return_value)

		# check for the 4 calls 2 of which are equal
		assert len(driver.multipart_put.call_args_list) == 4
		driver.multipart_put.assert_any_call(upload_id, 0, self.path0)
		driver.multipart_put.assert_any_call(upload_id, 2, self.path0)
		driver.multipart_put.assert_any_call(upload_id, 1, self.path1)
		driver.multipart_put.call_args_list.remove(mock.call(upload_id, 1, self.path1))
		driver.multipart_put.assert_any_call(upload_id, 1, self.path1)
		assert not driver.put.called
		assert not driver.get.called
		assert not driver.multipart_abort.called
		driver.multipart_complete.assert_called_once_with(upload_id)
		completed = [
			{"src": self.path0, "dst": self.path2, "chunk_num": 0,
			 "size": getsize.return_value},
			{"src": self.path1, "dst": self.path2, "chunk_num": 1,
			 "size": getsize.return_value},
			{"src": self.path0, "dst": self.path2, "chunk_num": 2,
			 "size": getsize.return_value},
		]
		assert len(ret["completed"]) == len(completed)
		for job in completed:
			assert job in ret["completed"], str(job) + ' not in ' + str(ret["completed"])
		assert ret["failed"] == []


	@mock.patch("scalarizr.storage2.cloudfs.os.path.isfile")
	@mock.patch("scalarizr.storage2.cloudfs.os.path.getsize")
	def test_worker_assertion_error(self, getsize, isfile):
		# both remote
		isfile.return_value = False
		obj = cloudfs.FileTransfer(src=self.path2, dst=self.path2)
		obj.fire = mock.MagicMock()

		ret = obj.run()

		assert ('transfer_error', self.path2, self.path2, 0, -1) in \
			[list(x)[0][:5] for x in obj.fire.call_args_list]
		assert not ret["completed"]
		assert not ret["failed"]

		# both local
		isfile.reset_mock()
		obj = cloudfs.FileTransfer(src=self.path1, dst=self.path1)
		obj.fire = mock.MagicMock()

		ret = obj.run()

		assert ('transfer_error', self.path1, self.path1, 0, -1) in\
			[list(x)[0][:5] for x in obj.fire.call_args_list]
		assert not ret["completed"]
		assert not ret["failed"]

		# uploading non-existent file
		isfile.return_value = False
		obj = cloudfs.FileTransfer(src=self.path1, dst=self.path2)
		obj.fire = mock.MagicMock()

		ret = obj.run()

		assert ('transfer_error', self.path1, self.path2, 0, -1) in\
			[list(x)[0][:5] for x in obj.fire.call_args_list]
		assert not ret["completed"]
		assert not ret["failed"]

	@mock.patch("scalarizr.storage2.cloudfs.os.path.isfile")
	@mock.patch("scalarizr.storage2.cloudfs.os.path.getsize")
	def test_worker_download(self, getsize, isfile):
		# str str
		obj = cloudfs.FileTransfer(src=self.path2, dst=self.path1)

		ret = obj.run()

		driver = cloudfs.cloudfs.return_value

		assert all([call == mock.call("s3") for call in cloudfs.cloudfs.call_args_list])
		driver.get.assert_called_once_with(self.path2, self.path1)
		assert not driver.multipart_init.called
		assert not driver.multipart_put.called
		assert not driver.put.called
		assert not driver.multipart_abort.called
		assert not driver.multipart_complete.called
		assert ret["completed"] == [
			{"src": self.path2, "dst": self.path1, "size": getsize.return_value},
		]
		assert ret["failed"] == []

		# iter iter
		cloudfs.cloudfs.reset_mock()
		dst_gen = self.make_generator(self.path0, self.path1, self.path0)
		dst2 = self.path2 + 'one/'
		dst3 = self.path2 + 'two/'
		src_gen = self.make_generator(self.path2, dst2, dst3)
		obj = cloudfs.FileTransfer(src=src_gen, dst=dst_gen)

		ret = obj.run()

		driver = cloudfs.cloudfs.return_value

		assert all([call == mock.call("s3") for call in cloudfs.cloudfs.call_args_list])
		driver.get.assert_any_call(self.path2, self.path0)
		driver.get.assert_any_call(dst2, self.path1)
		driver.get.assert_any_call(dst3, self.path0)
		assert not driver.multipart_init.called
		assert not driver.multipart_put.called
		assert not driver.put.called
		assert not driver.multipart_abort.called
		assert not driver.multipart_complete.called
		completed = [
			{"src": self.path2, "dst": self.path0, "size": getsize.return_value},
			{"src": dst2, "dst": self.path1, "size": getsize.return_value},
			{"src": dst3, "dst": self.path0, "size": getsize.return_value},
		]
		assert len(ret["completed"]) == len(completed)
		for job in completed:
			assert job in ret["completed"]
		assert ret["failed"] == []


class TestLargeTransfer(object):

	mpoint = '/mpoint/'

	path1 = '/mnt/backups/daily.tar.gz'
	path2 = 's3://backups/mysql/2012-09-05/'  # dir

	def setup(self):
		cloudfs.cloudfs = mock.MagicMock()
		cloudfs.FileTransfer = mock.MagicMock()
		cloudfs.storage2.volume = mock.MagicMock()
		cloudfs.coreutils.remove = mock.MagicMock()
		cloudfs.tempfile = mock.MagicMock()

		self.stream = mock.MagicMock()
		self.stream.read = lambda size: 'c' * size
		def myopen(path, mode=None):
			if mode is None:
				return self.stream
			obj = mock.MagicMock()
			obj.name = path
			return obj
		cloudfs.open = myopen

	@mock.patch("scalarizr.storage2.cloudfs.os.path.isfile")
	def test_src_gen_upload(self, isfile):
		isfile.return_value = True
		cloudfs.storage2.volume.return_value.mpoint = self.mpoint

		obj = cloudfs.LargeTransfer(self.path1, self.path2,
			cloudfs.LargeTransfer.UPLOAD, gzip_it=False)
		assert cloudfs.FileTransfer.call_count == 1
		src_gen = cloudfs.FileTransfer.call_args[0][0]()

		res = []
		for src in src_gen:
			res.append(src)

		assert res == [self.mpoint + basename(self.path1) + '.000'], res




