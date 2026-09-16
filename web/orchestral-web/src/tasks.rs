//! Ownership for effects that must outlive the control that started them.

use std::future::Future;

use dioxus::dioxus_core::{current_scope_id, Runtime, Task};
use dioxus::prelude::{spawn, ScopeId};

/// Captured by App so closing a dialog cannot cancel session coordination.
/// Tasks still end when App unmounts; individual streams retain their own
/// cancellation and generation checks when the selected session changes.
#[derive(Clone, Copy)]
pub(crate) struct AppTaskScope(ScopeId);

impl AppTaskScope {
    pub(crate) fn current() -> Self {
        Self(current_scope_id())
    }

    pub(crate) fn spawn(self, future: impl Future<Output = ()> + 'static) -> Task {
        Runtime::current().in_scope(self.0, || spawn(future))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::task::{Poll, Waker};

    use dioxus::dioxus_core::NoOpMutations;
    use dioxus::prelude::*;
    use futures_util::FutureExt;

    use super::AppTaskScope;

    #[derive(Clone, Default)]
    struct Gate(Rc<RefCell<(bool, Option<Waker>)>>);

    impl Gate {
        async fn wait(&self) {
            futures_util::future::poll_fn(|cx| {
                let mut state = self.0.borrow_mut();
                if state.0 {
                    Poll::Ready(())
                } else {
                    state.1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await;
        }

        fn release(&self) {
            let waker = {
                let mut state = self.0.borrow_mut();
                state.0 = true;
                state.1.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    #[derive(Clone, Default)]
    struct Fixture {
        acknowledgement: Gate,
        snapshot: Gate,
        live_event: Gate,
        needs_snapshot: bool,
        component_owned: bool,
        card_dropped: Rc<Cell<bool>>,
        reconciled: Rc<Cell<bool>>,
        observed: Rc<Cell<bool>>,
        observer_dropped: Rc<Cell<bool>>,
    }

    struct DropWitness(Rc<Cell<bool>>);

    impl Drop for DropWitness {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    fn app() -> Element {
        let owner = use_context_provider(AppTaskScope::current);
        let open = use_signal(|| true);
        use_context_provider(|| (owner, open));
        if open() {
            rsx! { ActionCard {} }
        } else {
            rsx! {}
        }
    }

    #[component]
    fn ActionCard() -> Element {
        let fixture = consume_context::<Fixture>();
        let (owner, mut open) = consume_context::<(AppTaskScope, Signal<bool>)>();
        let dropped = fixture.card_dropped.clone();
        use_drop(move || dropped.set(true));
        use_hook(move || {
            let component_owned = fixture.component_owned;
            let operation = async move {
                fixture.acknowledgement.wait().await;
                // The Host accepted the operation. Dismissing its control
                // must not cancel the snapshot or the new live subscription.
                open.set(false);
                if fixture.needs_snapshot {
                    fixture.snapshot.wait().await;
                    fixture.reconciled.set(true);
                }
                let witness = DropWitness(fixture.observer_dropped);
                let observer = async move {
                    let _witness = witness;
                    fixture.live_event.wait().await;
                    fixture.observed.set(true);
                    std::future::pending::<()>().await;
                };
                if component_owned {
                    spawn(observer);
                } else {
                    owner.spawn(observer);
                }
            };
            if component_owned {
                spawn(operation);
            } else {
                owner.spawn(operation);
            }
        });
        rsx! {}
    }

    fn render_ready(dom: &mut VirtualDom) {
        // Drain ready work without a timer or browser. A pending Gate is the
        // same suspension boundary as an outstanding HTTP/SSE read.
        let _ = dom.wait_for_work().now_or_never();
        dom.render_immediate(&mut NoOpMutations);
        let _ = dom.wait_for_work().now_or_never();
    }

    fn mount(fixture: Fixture) -> VirtualDom {
        let mut dom = VirtualDom::new(app);
        dom.provide_root_context(fixture);
        dom.rebuild(&mut NoOpMutations);
        render_ready(&mut dom);
        dom
    }

    #[test]
    fn action_reconciliation_and_live_events_survive_card_dismissal() {
        let fixture = Fixture {
            needs_snapshot: true,
            ..Fixture::default()
        };
        let mut dom = mount(fixture.clone());
        fixture.acknowledgement.release();
        render_ready(&mut dom);
        assert!(fixture.card_dropped.get());
        assert!(!fixture.reconciled.get());

        fixture.snapshot.release();
        render_ready(&mut dom);
        assert!(fixture.reconciled.get());
        fixture.live_event.release();
        render_ready(&mut dom);
        assert!(fixture.observed.get());

        drop(dom);
        assert!(fixture.observer_dropped.get());
    }

    #[test]
    fn new_session_observer_survives_the_card_that_started_it() {
        let fixture = Fixture::default();
        let mut dom = mount(fixture.clone());
        fixture.acknowledgement.release();
        render_ready(&mut dom);
        assert!(fixture.card_dropped.get());
        assert!(!fixture.observer_dropped.get());

        fixture.live_event.release();
        render_ready(&mut dom);
        assert!(fixture.observed.get());
    }

    #[test]
    fn component_owned_operation_reproduces_cancelled_reconciliation() {
        let fixture = Fixture {
            needs_snapshot: true,
            component_owned: true,
            ..Fixture::default()
        };
        let mut dom = mount(fixture.clone());
        fixture.acknowledgement.release();
        render_ready(&mut dom);
        assert!(fixture.card_dropped.get());

        fixture.snapshot.release();
        fixture.live_event.release();
        render_ready(&mut dom);
        assert!(!fixture.reconciled.get());
        assert!(!fixture.observed.get());
    }
}
