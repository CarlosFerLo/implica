#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueueItem {
    pub(crate) index: usize,
    pub(crate) is_node: bool,
}

impl QueueItem {
    pub fn new(index: usize, is_node: bool) -> Self {
        QueueItem { index, is_node }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DataQueue {
    queue: Vec<QueueItem>,
}

impl DataQueue {
    pub fn new(nodes_data_len: usize) -> Self {
        let mut queue: Vec<QueueItem> = Vec::with_capacity(2 * nodes_data_len - 1);

        for i in 0..nodes_data_len - 1 {
            queue.push(QueueItem::new(i, true));
            queue.push(QueueItem::new(i, false));
        }

        queue.push(QueueItem::new(nodes_data_len - 1, true));

        DataQueue { queue }
    }

    pub fn pop(&mut self) -> Option<QueueItem> {
        self.queue.pop()
    }

    pub fn push(&mut self, item: QueueItem) {
        if !self.queue.contains(&item) {
            self.queue.push(item);
        }
    }
}
