use std::sync::{Arc, RwLock};
use std::time::Duration;

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Notice {
    DecodeError,
    RecvBufferClamped {
        requested: usize,
        effective: usize,
    },
    Reconnecting {
        attempt: u32,
        delay: Duration,
    },
    Reconnected,
}

type Hook = Arc<dyn Fn(Notice) + Send + Sync>;

#[derive(Clone, Default)]
pub(crate) struct NoticeHook(Arc<RwLock<Option<Hook>>>);

impl NoticeHook {
    pub fn set(&self, f: impl Fn(Notice) + Send + Sync + 'static) {
        *self.0.write().expect("notice hook poisoned") = Some(Arc::new(f));
    }

    pub fn fire(&self, notice: Notice) {
        let hook = self.0.read().expect("notice hook poisoned").clone();
        if let Some(hook) = hook {
            hook(notice);
        }
    }
}
