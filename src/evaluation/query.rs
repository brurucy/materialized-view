use datalog_syntax::{AnonymousGroundAtom, Matcher, Query};

pub fn pattern_match(query: &Query, fact: &AnonymousGroundAtom) -> bool {
    return fact.iter().enumerate().all(|(index, term)| {
        if let Some(matcher) = query.matchers.get(index) {
            return match (matcher, term) {
                (Matcher::Any, _) => true,
                (Matcher::Constant(target), term) => target == term,
            };
        }

        true
    });
}
