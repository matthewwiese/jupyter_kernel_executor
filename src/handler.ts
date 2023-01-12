import {URLExt} from '@jupyterlab/coreutils';
import {ServerConnection} from '@jupyterlab/services';
import * as nbformat from '@jupyterlab/nbformat';
import { CodeCellModel, CodeCell } from '@jupyterlab/cells';

/**
 * Call the API extension
 *
 * @param endPoint API REST end point for the extension
 * @param init Initial values for the request
 * @returns The response body interpreted as JSON
 */
async function requestAPI<T>(
  endPoint = '',
  init: RequestInit = {}
): Promise<T> {
  // Make request to Jupyter API
  const settings = ServerConnection.makeSettings();
  const requestUrl = URLExt.join(
    settings.baseUrl,
    'api/kernels/', // API Namespace
    endPoint
  );

  let response: Response;
  try {
    response = await ServerConnection.makeRequest(requestUrl, init, settings);
  } catch (error) {
    throw new ServerConnection.NetworkError(error as any);
  }

  let data: any = await response.text();

  if (data.length > 0) {
    try {
      data = JSON.parse(data);
    } catch (error) {
      console.log('Not a JSON response body.', response);
    }
  }

  if (!response.ok) {
    throw new ServerConnection.ResponseError(response, data.message || data);
  }

  return data;
}

export async function watchExecuteStatus(kernel_id: string, cell_id: string, cell_index: number, notebook: any) {
  const execute_status = await requestAPI<any>(
    `${kernel_id}/execute`,
    { method: `GET` }
  )
  for (const cell_status of execute_status) {
    // Does this object correspond to our executed cell?
    if (cell_status.cell_id === cell_id) {
      const exec_count = cell_status.execution_count;

      notebook.activeCell.inputArea.promptNode.innerText = `[${exec_count === null ? '*' : exec_count}]:`;

      const cell_result_data = {
        output_type: 'display_data',
        data: { 'text/plain': cell_status.output.trim() },
        metadata: {}
      } as nbformat.IDisplayData;
      const active_cell_model = (notebook.activeCell.model as CodeCellModel);
      active_cell_model.outputs.length === 1
        ? active_cell_model.outputs.add(cell_result_data)
        : active_cell_model.outputs.set(0, cell_result_data);

      if (exec_count === null) {
        // NOTE: Manage timeout duration in user config?
        setTimeout(() => watchExecuteStatus(kernel_id, cell_id, cell_index, notebook), 2000)
      } else {
        return {};
      }
    }
  }
}

export async function execute_cell<T>(
  notebook_path: string,
  cell_id: string,
  cell_index: number,
  kernel_id: string,
  notebook: any
): Promise<T> {
  const body = JSON.stringify(
    {
      "path": notebook_path,
      "cell_id": cell_id
    }
  )
  const data = await requestAPI<any>(
    `${kernel_id}/execute`,
    {
      "body": body,
      "method": "POST",
    })
  console.log(data)

  watchExecuteStatus(kernel_id, cell_id, cell_index, notebook)

  return data;
}

export async function execute_cell_websocket<T>(
  notebook_path: string,
  cell_id: string,
  cell_index: number,
  kernel_id: string,
  notebook: any
): Promise<number> {
  const settings = ServerConnection.makeSettings();
  const requestUrl = URLExt.join(
    settings.wsUrl,
    'api/kernels',
    `${kernel_id}/execute_websocket`
  );

  const ws = new WebSocket(requestUrl);

  ws.onopen = function() {
    ws.send(JSON.stringify({
      meta: 'post',
      payload: {
        path: notebook_path,
        cell_id: cell_id,
        cell_index: cell_index,
        kernel_id: kernel_id
      }
    }));
    const code_cell = notebook.widgets[cell_index] as CodeCell;
    code_cell.setPrompt('*');
  };

  ws.onmessage = function (evt) {
    const data = JSON.parse(evt.data);
    const meta = data.meta;
    const payload = data.payload;
    if (meta === 'post_result') {
      ws.send(JSON.stringify({
        meta: 'get',
        payload: payload.model,
      }))
    } else if (meta === 'get') {
      for (const record of payload) {
        // TODO: Remove or integrate this
        if (record.cell_id === cell_id) {
          //const cell_result_data = {
          //  output_type: 'display_data',
          //  data: { 'text/plain': record.output },
          //  metadata: {}
          //} as nbformat.IDisplayData;
          const active_cell_model = notebook.widgets[cell_index].model as CodeCellModel;
          active_cell_model.executionCount = record.execution_count;
          //if (active_cell_model.outputs.length !== 0)
          //  active_cell_model.outputs.clear()
          //active_cell_model.outputs.add(cell_result_data)
        }

        if (record.id === cell_id) {
          const active_cell_model = notebook.widgets[cell_index].model as CodeCellModel;
          active_cell_model.outputs.clear()
          for (const output of record.outputs) {
            const cell_result_data = {
              output_type: 'display_data',
              data: { 'text/plain': output.text },
              metadata: {}
            } as nbformat.IDisplayData;
            active_cell_model.outputs.add(cell_result_data)
          }
        }
      }
    }
  };

  return new Promise<number>(resolve => resolve(1));
}