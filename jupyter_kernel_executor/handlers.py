import asyncio
import json
import os
from typing import Optional, Dict, List
from datetime import datetime

import tornado.web
from tornado.websocket import WebSocketHandler
from jupyter_server.base.handlers import APIHandler, JupyterHandler
from jupyter_server.utils import ensure_async
from watchfiles import awatch, Change

from jupyter_kernel_client.client import KernelWebsocketClient
from jupyter_kernel_executor.file_watcher import FileWatcher
from jupyter_kernel_executor.fileid import FileIDWrapper

class ExecuteCellWebSocketHandler(WebSocketHandler, JupyterHandler):
    executing_cell: Dict[str, List[
        Dict[str, str]]
    ] = dict()
    save_lock = asyncio.Lock()

    def initialize(self):
        self.execution_start_datetime: Optional[datetime] = None
        self.execution_end_datetime: Optional[datetime] = None
        self.watch_dir = self.normal_path(self.serverapp.root_dir or '.')
        self.global_watcher = FileWatcher(self.file_id_manager)

    @property
    def file_id_manager(self) -> FileIDWrapper:
        return FileIDWrapper(self.settings.get("file_id_manager"), self.save_lock)

    def normal_path(self, path):
        return self.file_id_manager.normalize_path(path)

    def index(self, path):
        return self.file_id_manager.index(path)

    async def get_path(self, document_id):
        return await self.file_id_manager.get_path(document_id)

    def get_document_id(self, path):
        return self.file_id_manager.get_id(path)

    def get_request_auth(self):
        auth_in_header = self.request.headers.get('Authorization')
        return {
            'Authorization': auth_in_header
        } if auth_in_header else dict()

    def get_auth_header(self):
        # v2 using identity_provider for token
        provider = getattr(self, "identity_provider", self.serverapp)

        return {"Authorization": f"token {provider.token}"}

    def update_execute_result(self, result, kernel_id, cell_id, finished = False):
        for rec in self.executing_cell[kernel_id]:
            if rec['cell_id'] == cell_id:
                rec['execution_count'] = result['execution_count'] if finished else None
                output = ''.join([output.text for output in result['outputs']])
                rec['output'] = output

    async def mimic_get(self, model):
        kernel_id = model.get('kernel_id')
        if not self.kernel_manager.get_kernel(kernel_id):
            raise tornado.web.HTTPError(404, f"No such kernel {kernel_id}")

        records = self.executing_cell.get(kernel_id, [])
        response = [
            {
                "path": await self.get_path(record['document_id']),
                "cell_id": record['cell_id'],
                "execution_count": record['execution_count'],
                "output": record['output'],
            } for record in records
        ]
        return {
            'meta': 'get',
            'payload': response
        }

    def is_executing(self, kernel_id, document_id, cell_id):
        return self.get_record(document_id, cell_id) in self.executing_cell.get(kernel_id, [])

    async def mimic_post(self, model):
        kernel_id = model.get('kernel_id')
        if not self.kernel_manager.get_kernel(kernel_id):
            raise tornado.web.HTTPError(404, f"No such kernel {kernel_id}")

        path = model.get('path')
        cell_id = model.get('cell_id')
        not_write = model.get('not_write', False)
        document_id = self.index(path)
        if self.is_executing(kernel_id, document_id, cell_id):
            self.log.info(f'cell {cell_id} of {path}(id:{document_id}) is executing')
            # TODO: Return most recent output?
            return {
                "meta": "executing",
                "payload": {
                    "model": model
                }
            }

        if model.get('block'):
            # from request, respect it
            # when not_write=True and block=False, means to execute code or cell silently
            block = model.get('block')
        elif not document_id or not cell_id:
            # no file or cell to write, need to response result
            block = True
        else:
            block = False

        auth_header = self.get_request_auth()
        if not auth_header:
            # fallback using app setting, May not be compatible with jupyterhub-singleuser or other singleuser app
            auth_header = self.get_auth_header()

        client = KernelWebsocketClient(
            kernel_id=kernel_id,
            host=self.serverapp.ip,
            port=self.serverapp.port,
            base_url=self.base_url,
            auth_header=auth_header,
            encoded=True,
        )
        code = model.get('code') or await self.read_code_from_ipynb(
            document_id,
            cell_id,
        )
        assert code is not None

        if not block:
            self.log.debug("async execute code, write result to file")

            async def write_callback():
                try:
                    result = client.get_result()
                    self.update_execute_result(result, kernel_id, cell_id)
                    await self.write_output(document_id, cell_id, result)
                except Exception as e:
                    self.log.error('Exception when asynchronous writing result to file')
                    self.log.exception(e)

            if not not_write:
                client.register_callback(write_callback)
            await self.execute(client, code, document_id, cell_id)
            return {
                "meta": 'post',
                "payload": {
                    "model": model
                }
            }
        else:
            self.log.debug("sync execute code, return execution result in response")
            result = await self.execute(client, code, document_id, cell_id)
            if not not_write:
                await self.write_output(document_id, cell_id, result)
            return {
                "meta": 'post_block',
                "payload": {
                    # TODO: Implement this
                    #"model": **model,
                    #"result": **result
                }
            }

    async def execute(self, client, code, document_id, cell_id):
        kernel_id = client.kernel_id
        await self.pre_execute(kernel_id, document_id, cell_id)
        try:
            result = await client.execute(code)
        finally:
            await self.post_execute(kernel_id, document_id, cell_id)
        self.log.debug(f'execute time: {self.execution_end_datetime - self.execution_start_datetime}')
        self.update_execute_result(result, kernel_id, cell_id, finished = True)
        return result

    async def pre_execute(self, kernel_id, document_id, cell_id):
        if document_id and cell_id:
            self.executing_cell.setdefault(kernel_id, []).append(
                self.get_record(document_id, cell_id)
            )
            self.global_watcher.add(self)
            self.global_watcher.start_if_not(self.watch_dir)

        self.execution_start_datetime = datetime.now()

    async def post_execute(self, kernel_id, document_id, cell_id):
        self.execution_end_datetime = datetime.now()
        records = self.executing_cell.get(kernel_id, [])
        if document_id and cell_id:
            # prevent memory leak
            self.global_watcher.remove(self)
            record = self.get_record(document_id, cell_id)
            if record in records:
                records.remove(record)

    def get_record(self, document_id, cell_id):
        return {
            'document_id': document_id,
            'cell_id': cell_id,
            'output': '',
            'execution_count': None
        }

    async def read_code_from_ipynb(self, document_id, cell_id) -> Optional[str]:
        if not document_id or not cell_id:
            return None
        cm = self.contents_manager
        path = await self.get_path(document_id)
        model = await ensure_async(cm.get(path, content=True, type='notebook'))
        nb = model['content']
        for cell in nb['cells']:
            if cell['id'] == cell_id:
                return cell['source']
        raise tornado.web.HTTPError(404, f"cell {cell_id} not found in {path}")

    async def write_output(self, document_id, cell_id, result):
        if not document_id or not cell_id:
            return
        cm = self.contents_manager
        path = await self.get_path(document_id)
        async with self.save_lock:
            model = await ensure_async(cm.get(path, content=True, type='notebook'))
            nb = model['content']
            updated = False
            for cell in nb['cells']:
                if cell['id'] == cell_id:
                    if result['outputs'] != cell["outputs"]:
                        cell["outputs"] = result['outputs']
                        updated = True
                    if result['execution_count']:
                        cell['execution_count'] = int(result['execution_count'])
                        updated = True
                    break
            if updated:
                await ensure_async(cm.save(model, path))
                self.file_id_manager.save(path)
                self.write_message(json.dumps({
                    "meta": "get",
                    "payload": nb['cells']
                }))

    def executing_document(self):
        executing_document = []
        for kernel_records in self.executing_cell.values():
            for records in kernel_records:
                executing_document.append(records['document_id'])
        return executing_document

    @tornado.web.authenticated
    def open(self, *args, **kwargs):
        print(kwargs) # kernel_id from handler

    @tornado.web.authenticated
    async def on_message(self, message):
        msg = json.loads(message)
        print(msg)
        if (msg['meta'] == 'post'):
            res = await self.mimic_post(msg['payload'])
            self.write_message(json.dumps({
                "meta": "post_result",
                "payload": res["payload"]
            }))
        elif (msg['meta'] == 'get'):
            res = await self.mimic_get(msg['payload'])
            self.write_message(json.dumps({
                "meta": "get",
                "payload": res["payload"]
            }))

    # TODO: Determine when to close connection
    #self.close()

    @tornado.web.authenticated
    def on_close(self):
        print("Closed")

def setup_handlers(web_app):
    host_pattern = ".*$"

    base_url = web_app.settings["base_url"].rstrip('/')
    _kernel_id_regex = r"(?P<kernel_id>\w+-\w+-\w+-\w+-\w+)"
    handlers = [
        #(rf"{base_url}/api/kernels/{_kernel_id_regex}/execute", ExecuteCellHandler),
        (rf"{base_url}/api/kernels/{_kernel_id_regex}/execute_websocket", ExecuteCellWebSocketHandler),
    ]
    web_app.add_handlers(host_pattern, handlers)
