//! Fairness as a machine combinator.
//!
//! Fairness is a property of schedules, not of the system being
//! scheduled, so it does not belong inside a model's own state. But an
//! explicit-state checker is memoryless over paths: to exclude unfair
//! schedules from traversal, the schedule memory has to be *somewhere*
//! in the state it explores. [`Fair`] is that somewhere: a wrapper that
//! pairs any machine's state with one counter, refuses a "starving"
//! action once it has happened too many times consecutively, and resets
//! the count on any "progress" action.
//!
//! The wrapped machine stays pure — it says what *can* happen — and each
//! harness chooses its own fairness: traversal wraps in `Fair`, a
//! simulation driver imposes fairness behaviorally on the unwrapped
//! machine, and an LTL checker may instead prefer a fairness assumption
//! in the property (which explores unfair paths but discounts them).

use polestar::prelude::*;

/// Wraps a machine, bounding consecutive "starving" actions.
///
/// Actions are classified by two predicates. An action that satisfies
/// `starving` is refused when the counter has reached `max_consecutive`,
/// and counted otherwise; one that satisfies `progress` resets the
/// counter. An action satisfying neither passes through untouched.
/// `starving` takes precedence if both match.
#[derive(Clone, Debug)]
pub struct Fair<M: Machine> {
    pub machine: M,
    /// Actions which starve the system if repeated forever.
    pub starving: fn(&M::Action) -> bool,
    /// Actions which constitute progress, resetting the count.
    pub progress: fn(&M::Action) -> bool,
    /// How many starving actions may occur consecutively.
    pub max_consecutive: usize,
}

impl<M: Machine> Fair<M> {
    pub fn new(
        machine: M,
        starving: fn(&M::Action) -> bool,
        progress: fn(&M::Action) -> bool,
        max_consecutive: usize,
    ) -> Self {
        Self {
            machine,
            starving,
            progress,
            max_consecutive,
        }
    }
}

impl<M> Machine for Fair<M>
where
    M: Machine<Error = anyhow::Error> + Clone + std::fmt::Debug + Send + Sync,
{
    type State = (M::State, usize);
    type Action = M::Action;
    type Fx = M::Fx;
    type Error = M::Error;

    fn transition(
        &self,
        (state, mut streak): Self::State,
        action: Self::Action,
    ) -> TransitionResult<Self> {
        if (self.starving)(&action) {
            anyhow::ensure!(
                streak < self.max_consecutive,
                "unfair: {} consecutive starving actions",
                self.max_consecutive
            );
            streak += 1;
        } else if (self.progress)(&action) {
            streak = 0;
        }
        let (state, fx) = self.machine.transition(state, action)?;
        Ok(((state, streak), fx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Accepts every action; the interesting behavior is all `Fair`'s.
    #[derive(Clone, Debug)]
    struct Anything;

    #[derive(Clone, Debug, PartialEq)]
    enum Act {
        Starve,
        Feed,
        Neither,
    }

    impl Machine for Anything {
        type State = ();
        type Action = Act;
        type Fx = ();
        type Error = anyhow::Error;

        fn transition(&self, (): (), _: Act) -> TransitionResult<Self> {
            Ok(((), ()))
        }
    }

    #[test]
    fn starving_actions_are_bounded_and_progress_resets() {
        let m = Fair::new(
            Anything,
            |a| matches!(a, Act::Starve),
            |a| matches!(a, Act::Feed),
            2,
        );
        let s = ((), 0);
        let (s, ()) = m.transition(s, Act::Starve).unwrap();
        let (s, ()) = m.transition(s, Act::Starve).unwrap();
        assert!(m.transition(s, Act::Starve).is_err());

        // A neutral action neither counts nor resets.
        let (s, ()) = m.transition(s, Act::Neither).unwrap();
        assert!(m.transition(s, Act::Starve).is_err());

        // Progress resets the streak.
        let (s, ()) = m.transition(s, Act::Feed).unwrap();
        assert_eq!(s.1, 0);
        m.transition(s, Act::Starve).unwrap();
    }
}
