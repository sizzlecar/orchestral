//! Cancellable file scans with at most one scan and one queued refresh.
use std::path::PathBuf;

use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::menu::Choice;

pub(crate) struct FileIndex {
    roots: Vec<PathBuf>,
    sender: UnboundedSender<Vec<Choice>>,
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
    refresh_queued: bool,
}

impl FileIndex {
    pub(crate) fn new(roots: Vec<PathBuf>, sender: UnboundedSender<Vec<Choice>>) -> Self {
        Self {
            roots,
            sender,
            cancel: CancellationToken::new(),
            task: None,
            refresh_queued: false,
        }
    }

    pub(crate) fn refresh(&mut self) {
        if self.task.is_some() {
            self.refresh_queued = true;
            return;
        }
        let roots = self.roots.clone();
        let sender = self.sender.clone();
        let cancel = self.cancel.clone();
        self.task = Some(tokio::task::spawn_blocking(move || {
            let choices = super::menu::file_index(&roots, &cancel);
            if !cancel.is_cancelled() {
                let _ = sender.send(choices);
            }
        }));
    }

    pub(crate) fn received(&mut self) {
        self.task.take();
        if std::mem::take(&mut self.refresh_queued) {
            self.refresh();
        }
    }
}

impl Drop for FileIndex {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
