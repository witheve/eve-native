use std::num::Wrapping;
use ops::{Program, make_scan, Constraint, Block, register};
extern crate time;
use std::time::Instant;

// In each University
    // 15~25 Departments are subOrgnization of the University

// In each Department:
    // 7~10 FullProfessors worksFor the Department
    // 10~14 AssociateProfessors worksFor the Department
    // 8~11 AssistantProfessors worksFor the Department
    // 5~7 Lecturers worksFor the Department
    // one of the FullProfessors is headOf the Department
    // every Faculty is teacherOf 1~2 Courses
    // every Faculty is teacherOf 1~2 GraduateCourses
    // Courses taught by faculties are pairwise disjoint
    // 10~20 ResearchGroups are subOrgnization of the Department
    // UndergraduateStudent : Faculty = 8~14 : 1
    // GraduateStudent : Faculty = 3~4 : 1
    // every Student is memberOf the Department
    // 1/5~1/4 of the GraduateStudents are chosen as TeachingAssistant for one Course
    // The Courses the GraduateStudents are TeachingAssistant of are pairwise different
    // 1/4~1/3 of the GraduateStudents are chosen as ResearchAssistant
    // 1/5 of the UndergraduateStudents have a Professor as their advisor
    // every GraduateStudent has a Professor as his advisor
    // every UndergraduateStudent takesCourse 2~4 Courses
    // every GraduateStudent takesCourse 1~3 GraduateCourses
    // every FullProfessor is publicationAuthor of 15~20 Publications
    // every AssociateProfessor is publicationAuthor of 10~18 Publications
    // every AssistantProfessor is publicationAuthor of 5~10 Publications
    // every Lecturer has 0~5 Publications
    // every GraduateStudent co-authors 0~5 Publications with some Professors
    // every Faculty has an undergraduateDegreeFrom a University, a mastersDegreeFrom a University, and a doctoralDegreeFrom a University
    // every GraudateStudent has an undergraduateDegreeFrom a University

fn rand(rseed:&mut u32) -> u32 {
    *rseed = ((Wrapping(*rseed) * Wrapping(1103515245) + Wrapping(12345)) & Wrapping(0x7fffffff)).0;
    return *rseed;
}

fn rand_between(rseed:&mut u32, from:u32, to:u32) -> u32 {
    rand(rseed);
    let range = to - from;
    from + *rseed % range
}

fn eav(program:&mut Program, eavs:&mut Vec<(u32,u32,u32)>, e:&str, a:&str, v:&str) {
    eavs.push((program.interner.string_id(&e), program.interner.string_id(&a), program.interner.string_id(&v)))
}

fn make_faculty(program:&mut Program, eavs:&mut Vec<(u32,u32,u32)>, department:&str, cur_type:&str, ix:u32, publications:u32, course_ix: &mut u32, grad_course_ix: &mut u32, mut seed: &mut u32) {
    let prof = format!("{}|{}{}", department, cur_type, ix);
    eav(program, eavs, &prof, "tag", cur_type);
    eav(program, eavs, &prof, "works for", department);
    eav(program, eavs, &prof, "email", &format!("{}@foo.edu", prof));
    eav(program, eavs, &prof, "telephone", "123-123-1234");
    eav(program, eavs, &prof, "research interest", "blah");
    // every Faculty is teacherOf 1~2 Courses
    for course in 0..rand_between(&mut seed, 1, 2) {
        let course = format!("{}|course{}", department, *course_ix);
        eav(program, eavs, &course, "tag", "course");
        eav(program, eavs, &course, "name", "foo");
        eav(program, eavs, &prof, "teacher of", &course);
        *course_ix += 1;
    }
    // every Faculty is teacherOf 1~2 GraduateCourses
    for graduate_course in 0..rand_between(&mut seed, 1, 2) {
        let course = format!("{}|graduate_course{}", department, *grad_course_ix);
        eav(program, eavs, &course, "tag", "graduate course");
        eav(program, eavs, &course, "name", "foo");
        eav(program, eavs, &prof, "teacher of", &course);
        *grad_course_ix += 1;
    }
    for pub_ix in 0..publications {
        let publication = format!("{}|publication{}", prof, pub_ix);
        eav(program, eavs, &publication, "tag", "publication");
        eav(program, eavs, &publication, "name", "foo");
        eav(program, eavs, &prof, "publication author", &publication);
    }
    // @TODO
    // every Faculty has an undergraduateDegreeFrom a University, a mastersDegreeFrom a University, and a doctoralDegreeFrom a University
}

fn generate(program: &mut Program, university_count:usize) -> Vec<(u32,u32,u32)> {
    let mut eavs:Vec<(u32,u32,u32)> = vec![];
    let mut seed = 0;
    for university_ix in 0..university_count {
        let university = format!("university{}", university_ix);
        eav(program, &mut eavs, &university, "tag", "university");

        // 15~25 Departments are subOrgnization of the University
        let department_count = rand_between(&mut seed, 10, 25);
        for department_ix in 0..department_count {
            let department = format!("{}|department{}", university, department_ix);
            eav(program, &mut eavs, &department, "tag", "department");
            eav(program, &mut eavs, &department, "suborganization of", &university);

            let mut course_ix = 0;
            let mut grad_course_ix = 0;
            let mut total_faculty = 0;
            // 7~10 FullProfessors worksFor the Department
            let full_professors_count = rand_between(&mut seed, 7, 10);
            for fp_ix in 0..full_professors_count {
                // every FullProfessor is publicationAuthor of 15~20 Publications
                let publications = rand_between(&mut seed, 15, 20);
                make_faculty(program, &mut eavs, &department, "full_professor", fp_ix, publications, &mut course_ix, &mut grad_course_ix, &mut seed);
                total_faculty += 1;
            }

            // one of the FullProfessors is headOf the Department
            let head = rand_between(&mut seed, 0, full_professors_count);
            let prof = format!("{}|full_professor{}", department, head);
            eav(program, &mut eavs, &prof, "head of", &department);

            // 10~14 AssociateProfessors worksFor the Department
            let associate_professors_count = rand_between(&mut seed, 10, 14);
            for ap_ix in 0..associate_professors_count {
                // every AssociateProfessor is publicationAuthor of 10~18 Publications
                let publications = rand_between(&mut seed, 10, 18);
                make_faculty(program, &mut eavs, &department, "associate_professor", ap_ix, publications, &mut course_ix, &mut grad_course_ix, &mut seed);
                total_faculty += 1;
            }

            // 8~11 AssistantProfessors worksFor the Department
            let assistant_professors_count = rand_between(&mut seed, 8, 11);
            for asp_ix in 0..assistant_professors_count {
                // every AssistantProfessor is publicationAuthor of 5~10 Publications
                let publications = rand_between(&mut seed, 5, 10);
                make_faculty(program, &mut eavs, &department, "assistant_professor", asp_ix, publications, &mut course_ix, &mut grad_course_ix, &mut seed);
                total_faculty += 1;
            }

            // 5~7 Lecturers worksFor the Department
            let lecturers_count = rand_between(&mut seed, 5, 7);
            for lec_ix in 0..lecturers_count {
                // every Lecturer has 0~5 Publications
                let publications = rand_between(&mut seed, 0, 5);
                make_faculty(program, &mut eavs, &department, "lecturer_count", lec_ix, publications, &mut course_ix, &mut grad_course_ix, &mut seed);
                total_faculty += 1;
            }

            // 10~20 ResearchGroups are subOrgnization of the Department
            let research_group_count = rand_between(&mut seed, 10, 20);
            for rg_ix in 0..research_group_count {
                let group = format!("{}|research_group{}", department, rg_ix);
                eav(program, &mut eavs, &group, "tag", "research group");
                eav(program, &mut eavs, &group, "suborganization of", &department);
            }

            // UndergraduateStudent : Faculty = 8~14 : 1
            let undergrads_count = rand_between(&mut seed, 8, 14) * total_faculty;
            for ug_ix in 0..undergrads_count {
                let undergrad = format!("{}|undergrad{}", department, ug_ix);
                eav(program, &mut eavs, &undergrad, "tag", "undergraduate student");
                // every Student is memberOf the Department
                eav(program, &mut eavs, &undergrad, "member of", &department);
                // @TODO
                // every UndergraduateStudent takesCourse 2~4 Courses
                // 1/5 of the UndergraduateStudents have a Professor as their advisor
            }
            // GraduateStudent : Faculty = 3~4 : 1
            let grads_count = rand_between(&mut seed, 3, 4) * total_faculty;
            // @TODO this should be grads_count
            for g_ix in 0..undergrads_count {
                let grad = format!("{}|graduate{}", department, g_ix);
                eav(program, &mut eavs, &grad, "tag", "graduate student");
                // if department_ix == 0 {
                //     println!("grad student: {:?}, {:?} == {:?}", &grad, program.interner.string_id(&grad), program.interner.string_id(&grad));
                // }
                // every Student is memberOf the Department
                eav(program, &mut eavs, &grad, "member of", &department);
                // every GraduateStudent takesCourse 1~3 GraduateCourses
                let course_count = rand_between(&mut seed, 1, 3);
                // eav(program, &mut eavs, &grad, "takes course", &format!("{}|graduate_course{}", department, 0));
                for _ in 0..course_count {
                    let course_ix = rand_between(&mut seed, 0, grad_course_ix);
                    eav(program, &mut eavs, &grad, "takes course", &format!("{}|graduate_course{}", department, course_ix));
                }
                // @TODO
                // every GraduateStudent has a Professor as his advisor
                // every GraduateStudent co-authors 0~5 Publications with some Professors
                // every GraudateStudent has an undergraduateDegreeFrom a University
                // 1/5~1/4 of the GraduateStudents are chosen as TeachingAssistant for one Course
                // 1/4~1/3 of the GraduateStudents are chosen as ResearchAssistant
            }
        }
    }
    eavs
}


// #[cfg(test)]
pub mod tests {
    extern crate test;

    use super::*;
    use self::test::Bencher;

    fn setup(program: &mut Program, size:usize) {
        let mut start = Instant::now();
        let eavs = generate(program, size);
        let mut dur = start.elapsed();
        println!("Gen took {:?}", (dur.as_secs() * 1000) as f32 + (dur.subsec_nanos() as f32) / 1_000_000.0);
        println!("size: {:?}", eavs.len());
        start = Instant::now();
        for (e,a,v) in eavs {
            program.raw_insert(e,a,v,0,1);
        }
        dur = start.elapsed();
        println!("Insert took {:?}", (dur.as_secs() * 1000) as f32 + (dur.subsec_nanos() as f32) / 1_000_000.0);
    }

    #[inline(never)]
    pub fn exec_lubm_1(program: &mut Program) -> Vec<u32> {
        let mut results = vec![];
        for _ in 0..1000 {
            results = program.exec_query();
        }
        results
    }

    // #[test]
    pub fn lubm_1() {
        let mut program = Program::new();
        setup(&mut program, 1);
        let constraints = vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("graduate student")),
            make_scan(register(0), program.interner.string("takes course"), program.interner.string("university0|department0|graduate_course0")),
            Constraint::Project { registers: vec![0] },
        ];
        let mut start = Instant::now();
        program.register_block(Block { name: "simple block".to_string(), constraints, pipes: vec![] });
        let mut dur = start.elapsed();
        println!("Compile took {:?}", (dur.as_secs() * 1000) as f32 + (dur.subsec_nanos() as f32) / 1_000_000.0);

        let start_ns = time::precise_time_ns();
        let results = exec_lubm_1(&mut program);
        let end_ns = time::precise_time_ns();
        println!("Run took {:?}", (end_ns - start_ns) as f64 / 1_000_000_000.0);
        println!("Results: {:?}", results);
    }

    #[bench]
    fn lubm_generate(b: &mut Bencher) {
        let mut program = Program::new();
        setup(&mut program, 1000);
        let constraints = vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("graduate student")),
            make_scan(register(0), program.interner.string("takes course"), program.interner.string("university0|department0|graduate_course0")),
            // Constraint::Project { registers: vec![0] },
            // Constraint::Insert {e: register(0), a: program.interner.string("tag"), v: program.interner.string("cool student")},
        ];
        let mut start = Instant::now();
        program.register_block(Block { name: "simple block".to_string(), constraints, pipes: vec![] });
        let mut dur = start.elapsed();
        println!("Compile took {:?}", (dur.as_secs() * 1000) as f32 + (dur.subsec_nanos() as f32) / 1_000_000.0);

        let mut results = vec![];
        // start = Instant::now();
        b.iter(|| {
            results = program.exec_query();
        })
        // dur = start.elapsed();
        // println!("Run took {:?}", (dur.as_secs() * 1000) as f32 + (dur.subsec_nanos() as f32) / 1_000_000.0);
    }
}
