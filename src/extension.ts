import {DocumentRegistry} from '@jupyterlab/docregistry';
import {
  INotebookModel,
  NotebookPanel
} from '@jupyterlab/notebook';
import {DisposableDelegate, IDisposable} from '@lumino/disposable';
import {ToolbarButton} from '@jupyterlab/apputils';
import {JupyterFrontEnd} from '@jupyterlab/application';
import { runIcon, terminalIcon } from '@jupyterlab/ui-components';
import { execute_cell, execute_cell_websocket } from "./handler";

export class RunBackendExtension
  implements DocumentRegistry.IWidgetExtension<NotebookPanel, INotebookModel> {
  public app: JupyterFrontEnd;
  public panel: any;

  public constructor(
    app: JupyterFrontEnd,
    panel: NotebookPanel
  ) {
    this.app = app;
    this.panel = null;
  }

  public createNew(
    panel: NotebookPanel,
    context: DocumentRegistry.IContext<INotebookModel>
  ): IDisposable {
    this.panel = panel;
    //const runBackendButton = new ToolbarButton({
    //  className: 'run-backend-button',
    //  icon: runIcon,
    //  onClick: this.onClick.bind(this),
    //  tooltip: 'Run code in backend'
    //});
    //panel.toolbar.insertItem(10, 'runFull', runBackendButton);

    const runBackendWebSocketButton = new ToolbarButton({
      className: 'run-backend-websocket-button',
      icon: runIcon,
      onClick: this.onClickWebsocket.bind(this),
      tooltip: 'Run code in backend'
    });
    panel.toolbar.insertItem(10, 'runFullWebSocket', runBackendWebSocketButton);

    return new DisposableDelegate(() => {
      //runBackendButton.dispose();
      runBackendWebSocketButton.dispose();
    });
  }

  public async onClick() {
    console.log('clicked!')
    const panel = this.panel
    const notebook = panel.content
    const cell_id = notebook.activeCell.model.id
    const cell_index = notebook.activeCellIndex
    const kernel_id = panel.sessionContext.session.kernel.id
    const path = panel.sessionContext.path
    await execute_cell(path, cell_id, cell_index, kernel_id, notebook)
  }

  public async onClickWebsocket() {
    const panel = this.panel
    const notebook = panel.content
    const cell_id = notebook.activeCell.model.id
    const cell_index = notebook.activeCellIndex
    const kernel_id = panel.sessionContext.session.kernel.id
    const path = panel.sessionContext.path
    await execute_cell_websocket(path, cell_id, cell_index, kernel_id, notebook);
  }
}
