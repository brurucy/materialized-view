use std::time::Instant;
use datalog_rule_macro::program;
use datalog_syntax::*;
use crate::engine::datalog::{DyreRuntime, Weight};

pub type Diff = (Option<String>, AnonymousGroundAtom, Weight);
pub type ElapsedInMilliseconds = u128;

pub trait Evaluator {
    fn new() -> Self;
    fn update(&mut self, changes: &Vec<Diff>) -> ElapsedInMilliseconds;
    fn triple_count(&self) -> usize;
}

pub struct DyreTC {
    runtime: DyreRuntime
}

impl Evaluator for DyreTC {
    fn new() -> Self {
        let program = program! {
            tc(?x, ?y) <- [e(?x, ?y)],
            tc(?x, ?z) <- [e(?x, ?y), tc(?y, ?z)]
        };

        let dyre_runtime = DyreRuntime::new(program);

        Self { runtime: dyre_runtime }
    }

    fn update(&mut self, changes: &Vec<Diff>) -> ElapsedInMilliseconds {
        changes
            .into_iter()
            .for_each(|diff| {
                if diff.2 > 0 {
                    self.runtime.insert("e", diff.1.clone());
                } else {
                    self.runtime.delete("e", diff.1.clone());
                }
            });

        let now = Instant::now();
        self.runtime.step();
        let elapsed = now.elapsed().as_millis();
        self.runtime.consolidate();
        self.runtime.safe = true;

        return elapsed
    }

    fn triple_count(&self) -> usize {
        let query = build_query!(tc(_, _));

        return self.runtime.query(&query).unwrap().count()
    }
}

pub struct DyreRDFS {
    runtime: DyreRuntime
}

impl Evaluator for DyreRDFS {
    fn new() -> Self {
        let program = program! {
            T(?s, ?p, ?o)     <- [RDF(?s, ?p, ?o)],
            T(?y, 0usize, ?x) <- [T(?a, 3usize, ?x), T(?y, ?a, ?z)],
            T(?z, 0usize, ?x) <- [T(?a, 4usize, ?x), T(?y, ?a, ?z)],
            T(?x, 2usize, ?z) <- [T(?x, 2usize, ?y), T(?y, 2usize, ?z)],
            T(?x, 1usize, ?z) <- [T(?x, 1usize, ?y), T(?y, 1usize, ?z)],
            T(?z, 0usize, ?y) <- [T(?x, 1usize, ?y), T(?z, 0usize, ?x)],
            T(?x, ?b, ?y)     <- [T(?a, 2usize, ?b), T(?x, ?a, ?y)]
        };

        let dyre_runtime = DyreRuntime::new(program);

        Self { runtime: dyre_runtime }
    }

    fn update(&mut self, changes: &Vec<Diff>) -> ElapsedInMilliseconds {
        changes
            .into_iter()
            .for_each(|diff| {
                if diff.2 > 0 {
                    self.runtime.insert("RDF", diff.1.clone());
                } else {
                    self.runtime.delete("RDF", diff.1.clone());
                }
            });

        let now = Instant::now();
        self.runtime.step();
        let elapsed = now.elapsed().as_millis();
        self.runtime.consolidate();
        self.runtime.safe = true;

        return elapsed
    }

    fn triple_count(&self) -> usize {
        let query = build_query!(T(_, _));

        return self.runtime.query(&query).unwrap().count()
    }
}

pub struct DyreOWL2RL {
    runtime: DyreRuntime
}

impl Evaluator for DyreOWL2RL {
    fn new() -> Self {
        let program = program! {
            University(?y) <- [mastersDegreeFrom(?x,?y)],
            Person(?x) <- [title(?x,?y)],
            degreeFrom(?x,?y) <- [hasAlumnus(?y,?x)],
            hasAlumnus(?x,?y) <- [degreeFrom(?y,?x)],
            Employee(?x) <- [Faculty(?x)],
            Faculty(?x) <- [Professor(?x)],
            Course(?y) <- [listedCourse(?x,?y)],
            Professor(?x) <- [AssociateProfessor(?x)],
            Person(?y) <- [member(?x,?y)],
            Professor(?x) <- [AssistantProfessor(?x)],
            Organization(?x) <- [orgPublication(?x,?y)],
            Professor(?x) <- [Chair(?x)],
            Article(?x) <- [TechnicalReport(?x)],
            worksFor(?x,?y) <- [headOf(?x,?y)],
            Person(?x) <- [age(?x,?y)],
            Person(?x) <- [degreeFrom(?x,?y)],
            University(?y) <- [degreeFrom(?x,?y)],
            Publication(?x) <- [Specification(?x)],
            AdministrativeStaff(?x) <- [SystemsStaff(?x)],
            Person(?y) <- [hasAlumnus(?x,?y)],
            Publication(?y) <- [softwareDocumentation(?x,?y)],
            Faculty(?x) <- [PostDoc(?x)],
            Software(?x) <- [softwareVersion(?x,?y)],
            Article(?x) <- [ConferencePaper(?x)],
            TeachingAssistant(?x) <- [Person(?x), teachingAssistantOf(?x,?y), Course(?y)],
            Person(?y) <- [affiliateOf(?x,?y)],
            Chair(?x) <- [Person(?x), headOf(?x,?y), Department(?y)],
            Director(?x) <- [Person(?x), headOf(?x,?y), Program(?y)],
            memberOf(?x,?y) <- [member(?y,?x)],
            member(?x,?y) <- [memberOf(?y,?x)],
            Professor(?x) <- [tenured(?x,?y)],
            Course(?y) <- [teacherOf(?x,?y)],
            University(?x) <- [hasAlumnus(?x,?y)],
            Work(?x) <- [Research(?x)],
            Person(?x) <- [telephone(?x,?y)],
            Organization(?x) <- [Institute(?x)],
            Organization(?y) <- [subOrganizationOf(?x,?y)],
            memberOf(?x,?y) <- [worksFor(?x,?y)],
            Person(?x) <- [Employee(?x)],
            Software(?x) <- [softwareDocumentation(?x,?y)],
            Person(?x) <- [advisor(?x,?y)],
            Organization(?x) <- [member(?x,?y)],
            Organization(?x) <- [Department(?x)],
            Publication(?x) <- [Article(?x)],
            Faculty(?x) <- [Lecturer(?x)],
            Person(?y) <- [publicationAuthor(?x,?y)],
            Publication(?x) <- [Software(?x)],
            Research(?y) <- [researchProject(?x,?y)],
            Organization(?x) <- [Program(?x)],
            Employee(?x) <- [AdministrativeStaff(?x)],
            Professor(?y) <- [advisor(?x,?y)],
            Work(?x) <- [Course(?x)],
            Professor(?x) <- [FullProfessor(?x)],
            Publication(?x) <- [Book(?x)],
            Publication(?x) <- [publicationResearch(?x,?y)],
            AdministrativeStaff(?x) <- [ClericalStaff(?x)],
            degreeFrom(?x,?y) <- [doctoralDegreeFrom(?x,?y)],
            Organization(?x) <- [affiliatedOrganizationOf(?x,?y)],
            TeachingAssistant(?x) <- [teachingAssistantOf(?x,?y)],
            Professor(?x) <- [VisitingProfessor(?x)],
            Person(?x) <- [undergraduateDegreeFrom(?x,?y)],
            Organization(?x) <- [University(?x)],
            Article(?x) <- [JournalArticle(?x)],
            Research(?y) <- [publicationResearch(?x,?y)],
            Person(?x) <- [Director(?x)],
            Person(?x) <- [doctoralDegreeFrom(?x,?y)],
            Publication(?x) <- [publicationDate(?x,?y)],
            Organization(?y) <- [affiliatedOrganizationOf(?x,?y)],
            University(?y) <- [doctoralDegreeFrom(?x,?y)],
            Course(?y) <- [teachingAssistantOf(?x,?y)],
            University(?y) <- [undergraduateDegreeFrom(?x,?y)],
            degreeFrom(?x,?y) <- [mastersDegreeFrom(?x,?y)],
            Schedule(?x) <- [listedCourse(?x,?y)],
            Person(?x) <- [GraduateStudent(?x)],
            Person(?x) <- [ResearchAssistant(?x)],
            Student(?x) <- [UndergraduateStudent(?x)],
            degreeFrom(?x,?y) <- [undergraduateDegreeFrom(?x,?y)],
            Publication(?x) <- [publicationAuthor(?x,?y)],
            Person(?x) <- [mastersDegreeFrom(?x,?y)],
            Organization(?x) <- [College(?x)],
            Organization(?x) <- [ResearchGroup(?x)],
            Faculty(?x) <- [teacherOf(?x,?y)],
            Publication(?x) <- [UnofficialPublication(?x)],
            Person(?x) <- [Chair(?x)],
            Employee(?x) <- [Person(?x), worksFor(?x,?y), Organization(?y)],
            ResearchGroup(?x) <- [researchProject(?x,?y)],
            Organization(?x) <- [affiliateOf(?x,?y)],
            Course(?x) <- [GraduateCourse(?x)],
            Student(?x) <- [Person(?x), takesCourse(?x,?y), Course(?y)],
            Professor(?x) <- [Dean(?x)],
            Publication(?y) <- [orgPublication(?x,?y)],
            Publication(?x) <- [Manual(?x)],
            Dean(?x) <- [headOf(?x,?y), College(?y)],
            Person(?x) <- [TeachingAssistant(?x)],
            Organization(?x) <- [subOrganizationOf(?x,?y)],
            Person(?x) <- [Student(?x)],
            Person(?x) <- [emailAddress(?x,?y)],
            subOrganizationOf(?x,?z) <- [subOrganizationOf(?x,?y), subOrganizationOf(?y,?z)],
        };

        let dyre_runtime = DyreRuntime::new(program);

        Self { runtime: dyre_runtime }
    }

    fn update(&mut self, changes: &Vec<Diff>) -> ElapsedInMilliseconds {
        changes
            .into_iter()
            .for_each(|diff| {
                let relation = diff.0.clone().unwrap();

                if diff.2 > 0 {
                    self.runtime.insert(&relation, diff.1.clone());
                } else {
                    self.runtime.delete(&relation, diff.1.clone());
                }
            });

        let now = Instant::now();
        self.runtime.step();
        let elapsed = now.elapsed().as_millis();
        self.runtime.consolidate();
        self.runtime.safe = true;

        return elapsed
    }

    fn triple_count(&self) -> usize {
        return self.runtime.materialisation.inner.iter().map(|(_relation_name, fact_storage)| fact_storage.len()).sum()
    }
}

