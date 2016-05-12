import uuid
import mock

from nose.tools import assert_equals

from scalarizr.handlers.union import base


@mock.patch.object(base, '__node__', {'scripting_log_dir': '/scripting/log'})
@mock.patch('os.symlink')
def test_symlink_execute_logs_with_execution_id(symlink, *args):
	task = mock.Mock()
	task.agent_ext.log_dir = '/task/log'
	meta = {'persistent': {
		'execution_id': 'e1bf3b47',
		'script_name': 'reload apache',
		'event_name': 'BeforeHostUp',
		'role_name': 'base-ubuntu1404'}}

	base.symlink_execute_logs(task, meta)

	assert_equals(symlink.mock_calls[0], mock.call(
		'/task/log/stdout.log', 
		'/scripting/log/reload apache.BeforeHostUp.e1bf3b47-out.log'))
	assert_equals(symlink.mock_calls[1], mock.call(
		'/task/log/stderr.log', 
		'/scripting/log/reload apache.BeforeHostUp.e1bf3b47-err.log'))


@mock.patch.object(base, '__node__', {'scripting_log_dir': '/scripting/log'})
@mock.patch('os.symlink')
def test_symlink_execute_logs_before_execution_id(symlink, *args):
	task = mock.Mock()
	task.id = '3915f946'
	task.agent_ext.log_dir = '/task/log'
	meta = {'persistent': {
		'execution_id': '',
		'script_name': 'reload apache',
		'event_name': 'BeforeHostUp',
		'role_name': 'base-ubuntu1404'}}

	base.symlink_execute_logs(task, meta)

	assert_equals(symlink.mock_calls[0], mock.call(
		'/task/log/stdout.log', 
		'/scripting/log/reload apache.BeforeHostUp.base-ubuntu1404.3915f946-out.log'))
	assert_equals(symlink.mock_calls[1], mock.call(
		'/task/log/stderr.log', 
		'/scripting/log/reload apache.BeforeHostUp.base-ubuntu1404.3915f946-err.log'))


@mock.patch.object(base, '__node__', {
	'server_id': 'e1bf3b47',
	'base': {'abort_init_on_script_fail': 1}})
@mock.patch.object(base, 'set_init_result')
def test_propagate_before_host_up_error_maybe(set_init_result, *args):
	task = mock.MagicMock()
	task_data = {'state': 'failed'}
	task.__getitem__.side_effect = task_data.__getitem__
	task.exception = Exception('task error')
	meta = {'persistent': {
		'event_name': 'BeforeHostUp',
		'event_server_id': 'e1bf3b47'}}

	base.propagate_before_host_up_error_maybe(task, meta)

	set_init_result.assert_called_once_with(task.exception)

