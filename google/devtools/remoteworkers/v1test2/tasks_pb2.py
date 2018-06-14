# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: google/devtools/remoteworkers/v1test2/tasks.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.api import annotations_pb2 as google_dot_api_dot_annotations__pb2
from google.protobuf import any_pb2 as google_dot_protobuf_dot_any__pb2
from google.protobuf import field_mask_pb2 as google_dot_protobuf_dot_field__mask__pb2
from google.rpc import status_pb2 as google_dot_rpc_dot_status__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='google/devtools/remoteworkers/v1test2/tasks.proto',
  package='google.devtools.remoteworkers.v1test2',
  syntax='proto3',
  serialized_pb=_b('\n1google/devtools/remoteworkers/v1test2/tasks.proto\x12%google.devtools.remoteworkers.v1test2\x1a\x1cgoogle/api/annotations.proto\x1a\x19google/protobuf/any.proto\x1a google/protobuf/field_mask.proto\x1a\x17google/rpc/status.proto\"\xb1\x01\n\x04Task\x12\x0c\n\x04name\x18\x01 \x01(\t\x12)\n\x0b\x64\x65scription\x18\x02 \x01(\x0b\x32\x14.google.protobuf.Any\x12\x43\n\x04logs\x18\x03 \x03(\x0b\x32\x35.google.devtools.remoteworkers.v1test2.Task.LogsEntry\x1a+\n\tLogsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\x9a\x01\n\nTaskResult\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x10\n\x08\x63omplete\x18\x02 \x01(\x08\x12\"\n\x06status\x18\x03 \x01(\x0b\x32\x12.google.rpc.Status\x12$\n\x06output\x18\x04 \x01(\x0b\x32\x14.google.protobuf.Any\x12\"\n\x04meta\x18\x05 \x01(\x0b\x32\x14.google.protobuf.Any\"\x1e\n\x0eGetTaskRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\"\xab\x01\n\x17UpdateTaskResultRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x41\n\x06result\x18\x02 \x01(\x0b\x32\x31.google.devtools.remoteworkers.v1test2.TaskResult\x12/\n\x0bupdate_mask\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.FieldMask\x12\x0e\n\x06source\x18\x04 \x01(\t\"1\n\x11\x41\x64\x64TaskLogRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0e\n\x06log_id\x18\x02 \x01(\t\"$\n\x12\x41\x64\x64TaskLogResponse\x12\x0e\n\x06handle\x18\x01 \x01(\t2\x88\x04\n\x05Tasks\x12\x91\x01\n\x07GetTask\x12\x35.google.devtools.remoteworkers.v1test2.GetTaskRequest\x1a+.google.devtools.remoteworkers.v1test2.Task\"\"\x82\xd3\xe4\x93\x02\x1c\x12\x1a/v1test2/{name=**/tasks/*}\x12\xb8\x01\n\x10UpdateTaskResult\x12>.google.devtools.remoteworkers.v1test2.UpdateTaskResultRequest\x1a\x31.google.devtools.remoteworkers.v1test2.TaskResult\"1\x82\xd3\xe4\x93\x02+2!/v1test2/{name=**/tasks/*/result}:\x06result\x12\xaf\x01\n\nAddTaskLog\x12\x38.google.devtools.remoteworkers.v1test2.AddTaskLogRequest\x1a\x39.google.devtools.remoteworkers.v1test2.AddTaskLogResponse\",\x82\xd3\xe4\x93\x02&\"!/v1test2/{name=**/tasks/*}:addLog:\x01*B\xc2\x01\n)com.google.devtools.remoteworkers.v1test2B\x12RemoteWorkersTasksP\x01ZRgoogle.golang.org/genproto/googleapis/devtools/remoteworkers/v1test2;remoteworkers\xa2\x02\x02RW\xaa\x02%Google.DevTools.RemoteWorkers.V1Test2b\x06proto3')
  ,
  dependencies=[google_dot_api_dot_annotations__pb2.DESCRIPTOR,google_dot_protobuf_dot_any__pb2.DESCRIPTOR,google_dot_protobuf_dot_field__mask__pb2.DESCRIPTOR,google_dot_rpc_dot_status__pb2.DESCRIPTOR,])




_TASK_LOGSENTRY = _descriptor.Descriptor(
  name='LogsEntry',
  full_name='google.devtools.remoteworkers.v1test2.Task.LogsEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='google.devtools.remoteworkers.v1test2.Task.LogsEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='value', full_name='google.devtools.remoteworkers.v1test2.Task.LogsEntry.value', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=_descriptor._ParseOptions(descriptor_pb2.MessageOptions(), _b('8\001')),
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=343,
  serialized_end=386,
)

_TASK = _descriptor.Descriptor(
  name='Task',
  full_name='google.devtools.remoteworkers.v1test2.Task',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='google.devtools.remoteworkers.v1test2.Task.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='description', full_name='google.devtools.remoteworkers.v1test2.Task.description', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='logs', full_name='google.devtools.remoteworkers.v1test2.Task.logs', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[_TASK_LOGSENTRY, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=209,
  serialized_end=386,
)


_TASKRESULT = _descriptor.Descriptor(
  name='TaskResult',
  full_name='google.devtools.remoteworkers.v1test2.TaskResult',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='google.devtools.remoteworkers.v1test2.TaskResult.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='complete', full_name='google.devtools.remoteworkers.v1test2.TaskResult.complete', index=1,
      number=2, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='status', full_name='google.devtools.remoteworkers.v1test2.TaskResult.status', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='output', full_name='google.devtools.remoteworkers.v1test2.TaskResult.output', index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='meta', full_name='google.devtools.remoteworkers.v1test2.TaskResult.meta', index=4,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=389,
  serialized_end=543,
)


_GETTASKREQUEST = _descriptor.Descriptor(
  name='GetTaskRequest',
  full_name='google.devtools.remoteworkers.v1test2.GetTaskRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='google.devtools.remoteworkers.v1test2.GetTaskRequest.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=545,
  serialized_end=575,
)


_UPDATETASKRESULTREQUEST = _descriptor.Descriptor(
  name='UpdateTaskResultRequest',
  full_name='google.devtools.remoteworkers.v1test2.UpdateTaskResultRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='google.devtools.remoteworkers.v1test2.UpdateTaskResultRequest.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='result', full_name='google.devtools.remoteworkers.v1test2.UpdateTaskResultRequest.result', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='update_mask', full_name='google.devtools.remoteworkers.v1test2.UpdateTaskResultRequest.update_mask', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='source', full_name='google.devtools.remoteworkers.v1test2.UpdateTaskResultRequest.source', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=578,
  serialized_end=749,
)


_ADDTASKLOGREQUEST = _descriptor.Descriptor(
  name='AddTaskLogRequest',
  full_name='google.devtools.remoteworkers.v1test2.AddTaskLogRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='google.devtools.remoteworkers.v1test2.AddTaskLogRequest.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='log_id', full_name='google.devtools.remoteworkers.v1test2.AddTaskLogRequest.log_id', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=751,
  serialized_end=800,
)


_ADDTASKLOGRESPONSE = _descriptor.Descriptor(
  name='AddTaskLogResponse',
  full_name='google.devtools.remoteworkers.v1test2.AddTaskLogResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='handle', full_name='google.devtools.remoteworkers.v1test2.AddTaskLogResponse.handle', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=802,
  serialized_end=838,
)

_TASK_LOGSENTRY.containing_type = _TASK
_TASK.fields_by_name['description'].message_type = google_dot_protobuf_dot_any__pb2._ANY
_TASK.fields_by_name['logs'].message_type = _TASK_LOGSENTRY
_TASKRESULT.fields_by_name['status'].message_type = google_dot_rpc_dot_status__pb2._STATUS
_TASKRESULT.fields_by_name['output'].message_type = google_dot_protobuf_dot_any__pb2._ANY
_TASKRESULT.fields_by_name['meta'].message_type = google_dot_protobuf_dot_any__pb2._ANY
_UPDATETASKRESULTREQUEST.fields_by_name['result'].message_type = _TASKRESULT
_UPDATETASKRESULTREQUEST.fields_by_name['update_mask'].message_type = google_dot_protobuf_dot_field__mask__pb2._FIELDMASK
DESCRIPTOR.message_types_by_name['Task'] = _TASK
DESCRIPTOR.message_types_by_name['TaskResult'] = _TASKRESULT
DESCRIPTOR.message_types_by_name['GetTaskRequest'] = _GETTASKREQUEST
DESCRIPTOR.message_types_by_name['UpdateTaskResultRequest'] = _UPDATETASKRESULTREQUEST
DESCRIPTOR.message_types_by_name['AddTaskLogRequest'] = _ADDTASKLOGREQUEST
DESCRIPTOR.message_types_by_name['AddTaskLogResponse'] = _ADDTASKLOGRESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Task = _reflection.GeneratedProtocolMessageType('Task', (_message.Message,), dict(

  LogsEntry = _reflection.GeneratedProtocolMessageType('LogsEntry', (_message.Message,), dict(
    DESCRIPTOR = _TASK_LOGSENTRY,
    __module__ = 'google.devtools.remoteworkers.v1test2.tasks_pb2'
    # @@protoc_insertion_point(class_scope:google.devtools.remoteworkers.v1test2.Task.LogsEntry)
    ))
  ,
  DESCRIPTOR = _TASK,
  __module__ = 'google.devtools.remoteworkers.v1test2.tasks_pb2'
  # @@protoc_insertion_point(class_scope:google.devtools.remoteworkers.v1test2.Task)
  ))
_sym_db.RegisterMessage(Task)
_sym_db.RegisterMessage(Task.LogsEntry)

TaskResult = _reflection.GeneratedProtocolMessageType('TaskResult', (_message.Message,), dict(
  DESCRIPTOR = _TASKRESULT,
  __module__ = 'google.devtools.remoteworkers.v1test2.tasks_pb2'
  # @@protoc_insertion_point(class_scope:google.devtools.remoteworkers.v1test2.TaskResult)
  ))
_sym_db.RegisterMessage(TaskResult)

GetTaskRequest = _reflection.GeneratedProtocolMessageType('GetTaskRequest', (_message.Message,), dict(
  DESCRIPTOR = _GETTASKREQUEST,
  __module__ = 'google.devtools.remoteworkers.v1test2.tasks_pb2'
  # @@protoc_insertion_point(class_scope:google.devtools.remoteworkers.v1test2.GetTaskRequest)
  ))
_sym_db.RegisterMessage(GetTaskRequest)

UpdateTaskResultRequest = _reflection.GeneratedProtocolMessageType('UpdateTaskResultRequest', (_message.Message,), dict(
  DESCRIPTOR = _UPDATETASKRESULTREQUEST,
  __module__ = 'google.devtools.remoteworkers.v1test2.tasks_pb2'
  # @@protoc_insertion_point(class_scope:google.devtools.remoteworkers.v1test2.UpdateTaskResultRequest)
  ))
_sym_db.RegisterMessage(UpdateTaskResultRequest)

AddTaskLogRequest = _reflection.GeneratedProtocolMessageType('AddTaskLogRequest', (_message.Message,), dict(
  DESCRIPTOR = _ADDTASKLOGREQUEST,
  __module__ = 'google.devtools.remoteworkers.v1test2.tasks_pb2'
  # @@protoc_insertion_point(class_scope:google.devtools.remoteworkers.v1test2.AddTaskLogRequest)
  ))
_sym_db.RegisterMessage(AddTaskLogRequest)

AddTaskLogResponse = _reflection.GeneratedProtocolMessageType('AddTaskLogResponse', (_message.Message,), dict(
  DESCRIPTOR = _ADDTASKLOGRESPONSE,
  __module__ = 'google.devtools.remoteworkers.v1test2.tasks_pb2'
  # @@protoc_insertion_point(class_scope:google.devtools.remoteworkers.v1test2.AddTaskLogResponse)
  ))
_sym_db.RegisterMessage(AddTaskLogResponse)


DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), _b('\n)com.google.devtools.remoteworkers.v1test2B\022RemoteWorkersTasksP\001ZRgoogle.golang.org/genproto/googleapis/devtools/remoteworkers/v1test2;remoteworkers\242\002\002RW\252\002%Google.DevTools.RemoteWorkers.V1Test2'))
_TASK_LOGSENTRY.has_options = True
_TASK_LOGSENTRY._options = _descriptor._ParseOptions(descriptor_pb2.MessageOptions(), _b('8\001'))

_TASKS = _descriptor.ServiceDescriptor(
  name='Tasks',
  full_name='google.devtools.remoteworkers.v1test2.Tasks',
  file=DESCRIPTOR,
  index=0,
  options=None,
  serialized_start=841,
  serialized_end=1361,
  methods=[
  _descriptor.MethodDescriptor(
    name='GetTask',
    full_name='google.devtools.remoteworkers.v1test2.Tasks.GetTask',
    index=0,
    containing_service=None,
    input_type=_GETTASKREQUEST,
    output_type=_TASK,
    options=_descriptor._ParseOptions(descriptor_pb2.MethodOptions(), _b('\202\323\344\223\002\034\022\032/v1test2/{name=**/tasks/*}')),
  ),
  _descriptor.MethodDescriptor(
    name='UpdateTaskResult',
    full_name='google.devtools.remoteworkers.v1test2.Tasks.UpdateTaskResult',
    index=1,
    containing_service=None,
    input_type=_UPDATETASKRESULTREQUEST,
    output_type=_TASKRESULT,
    options=_descriptor._ParseOptions(descriptor_pb2.MethodOptions(), _b('\202\323\344\223\002+2!/v1test2/{name=**/tasks/*/result}:\006result')),
  ),
  _descriptor.MethodDescriptor(
    name='AddTaskLog',
    full_name='google.devtools.remoteworkers.v1test2.Tasks.AddTaskLog',
    index=2,
    containing_service=None,
    input_type=_ADDTASKLOGREQUEST,
    output_type=_ADDTASKLOGRESPONSE,
    options=_descriptor._ParseOptions(descriptor_pb2.MethodOptions(), _b('\202\323\344\223\002&\"!/v1test2/{name=**/tasks/*}:addLog:\001*')),
  ),
])
_sym_db.RegisterServiceDescriptor(_TASKS)

DESCRIPTOR.services_by_name['Tasks'] = _TASKS

# @@protoc_insertion_point(module_scope)