use std::num::Wrapping;
use ops::{Program, make_scan, Constraint, Block, register};
extern crate time;
use std::time::Instant;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std;
use indexes::MyHasher;

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
    let range = (to - from) + 1;
    from + *rseed % range
}

fn eav(program:&mut Program, eavs:&mut Vec<(u32,u32,u32)>, e:&str, a:&str, v:&str) {
    eavs.push((program.interner.string_id(&e), program.interner.string_id(&a), program.interner.string_id(&v)))
}

fn make_faculty(program:&mut Program, eavs:&mut Vec<(u32,u32,u32)>, university_count:usize, department:&str, cur_type:&str, ix:u32, publications:u32, course_ix: &mut u32, grad_course_ix: &mut u32, mut seed: &mut u32, prof_to_pubs: &mut HashMap<String, u32>) {
    let prof = format!("{}|{}{}", department, cur_type, ix);
    eav(program, eavs, &prof, "tag", cur_type);
    eav(program, eavs, &prof, "works for", department);
    eav(program, eavs, &prof, "name", &format!("{}|name", prof));
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
    prof_to_pubs.insert(prof.to_string(), publications);
    for pub_ix in 0..publications {
        let publication = format!("{}|publication{}", prof, pub_ix);
        eav(program, eavs, &publication, "tag", "publication");
        eav(program, eavs, &publication, "name", "foo");
        eav(program, eavs, &publication, "author", &prof);
    }
    // every Faculty has an undergraduateDegreeFrom a University, a mastersDegreeFrom a University, and a doctoralDegreeFrom a University
    let ugrad = rand_between(&mut seed, 0, university_count as u32);
    eav(program, eavs, &prof, "undergraduate degree from", &format!("university{:?}", ugrad));

    let masters = rand_between(&mut seed, 0, university_count as u32);
    eav(program, eavs, &prof, "masters degree from", &format!("university{:?}", masters));

    let phd = rand_between(&mut seed, 0, university_count as u32);
    eav(program, eavs, &prof, "doctoral degree from", &format!("university{:?}", phd));
}

fn random_professor(seed:&mut u32, department:&str, fulls:u32, associates:u32, assistants:u32) -> String {
    let random_type = rand_between(seed, 1, 3);
    let (prof_type, id) = match random_type {
        1 => {
            let id = rand_between(seed, 0, fulls - 1);
            ("full_professor", id)
        },
        2 => {
            let id = rand_between(seed, 0, associates - 1);
            ("associate_professor", id)
        },
        3 => {
            let id = rand_between(seed, 0, assistants - 1);
            ("assistant_professor", id)
        },
        _ => panic!("bad professor type"),
    };
    format!("{}|{}{}", department, prof_type, id)
}

fn generate(program: &mut Program, university_count:usize) -> Vec<(u32,u32,u32)> {
    let mut counter = 0;
    let mut eavs:Vec<(u32,u32,u32)> = vec![];
    let mut seed = 0;
    let mut prof_to_pubs = HashMap::new();
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
                make_faculty(program, &mut eavs, university_count, &department, "full_professor", fp_ix, publications, &mut course_ix, &mut grad_course_ix, &mut seed, &mut prof_to_pubs);
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
                make_faculty(program, &mut eavs, university_count, &department, "associate_professor", ap_ix, publications, &mut course_ix, &mut grad_course_ix, &mut seed, &mut prof_to_pubs);
                total_faculty += 1;
            }

            // 8~11 AssistantProfessors worksFor the Department
            let assistant_professors_count = rand_between(&mut seed, 8, 11);
            for asp_ix in 0..assistant_professors_count {
                // every AssistantProfessor is publicationAuthor of 5~10 Publications
                let publications = rand_between(&mut seed, 5, 10);
                make_faculty(program, &mut eavs, university_count, &department, "assistant_professor", asp_ix, publications, &mut course_ix, &mut grad_course_ix, &mut seed, &mut prof_to_pubs);
                total_faculty += 1;
            }

            // 5~7 Lecturers worksFor the Department
            let lecturers_count = rand_between(&mut seed, 5, 7);
            for lec_ix in 0..lecturers_count {
                // every Lecturer has 0~5 Publications
                let publications = rand_between(&mut seed, 0, 5);
                make_faculty(program, &mut eavs, university_count, &department, "lecturer_count", lec_ix, publications, &mut course_ix, &mut grad_course_ix, &mut seed, &mut prof_to_pubs);
                total_faculty += 1;
            }

            // 10~20 ResearchGroups are subOrgnization of the Department
            let research_group_count = rand_between(&mut seed, 10, 20);
            for rg_ix in 0..research_group_count {
                let group = format!("{}|research_group{}", department, rg_ix);
                eav(program, &mut eavs, &group, "tag", "research group");
                eav(program, &mut eavs, &group, "suborganization of", &department);
                // eav(program, &mut eavs, &group, "suborganization of", &university);
            }

            // UndergraduateStudent : Faculty = 8~14 : 1
            let undergrads_count = rand_between(&mut seed, 8, 14) * total_faculty;
            if university == "university0" {
                counter += undergrads_count;
            }
            for ug_ix in 0..undergrads_count {
                let undergrad = format!("{}|undergrad{}", department, ug_ix);
                eav(program, &mut eavs, &undergrad, "tag", "undergraduate student");
                eav(program, &mut eavs, &undergrad, "name", &format!("{}|name", undergrad));
                eav(program, &mut eavs, &undergrad, "email", &format!("{}@foo.edu", undergrad));
                eav(program, &mut eavs, &undergrad, "telephone", "123-123-1234");
                // every Student is memberOf the Department
                eav(program, &mut eavs, &undergrad, "member of", &department);
                // every UndergraduateStudent takesCourse 2~4 Courses
                let course_count = rand_between(&mut seed, 2, 4);
                for _ in 0..course_count {
                    let undergrad_course_ix = rand_between(&mut seed, 0, course_ix);
                    eav(program, &mut eavs, &undergrad, "takes course", &format!("{}|course{}", department, undergrad_course_ix));
                }
                // 1/5 of the UndergraduateStudents have a Professor as their advisor
                if ug_ix % 5 == 0 {
                    let prof = random_professor(&mut seed, &department, full_professors_count, associate_professors_count, assistant_professors_count);
                    eav(program, &mut eavs, &undergrad, "advisor", &prof);
                }
            }
            // GraduateStudent : Faculty = 3~4 : 1
            let TA_ratio = rand_between(&mut seed, 4, 5);
            let RA_ratio = rand_between(&mut seed, 3, 4);
            let grads_count = rand_between(&mut seed, 3, 4) * total_faculty;
            // @TODO this should be grads_count
            for g_ix in 0..grads_count {
                let grad = format!("{}|graduate{}", department, g_ix);
                eav(program, &mut eavs, &grad, "tag", "graduate student");
                eav(program, &mut eavs, &grad, "name", &format!("{}|name", grad));
                eav(program, &mut eavs, &grad, "email", &format!("{}@foo.edu", grad));
                eav(program, &mut eavs, &grad, "telephone", "123-123-1234");
                // if department_ix == 0 {
                //     println!("grad student: {:?}, {:?} == {:?}", &grad, program.interner.string_id(&grad), program.interner.string_id(&grad));
                // }
                // every Student is memberOf the Department
                eav(program, &mut eavs, &grad, "member of", &department);
                // every GraduateStudent takesCourse 1~3 GraduateCourses
                let course_count = rand_between(&mut seed, 1, 3);
                for _ in 0..course_count {
                    let course_ix = rand_between(&mut seed, 0, grad_course_ix);
                    eav(program, &mut eavs, &grad, "takes course", &format!("{}|graduate_course{}", department, course_ix));
                }
                // every GraduateStudent has a Professor as his advisor
                let prof = random_professor(&mut seed, &department, full_professors_count, associate_professors_count, assistant_professors_count);
                eav(program, &mut eavs, &grad, "advisor", &prof);
                // every GraudateStudent has an undergraduateDegreeFrom a University
                let degree = rand_between(&mut seed, 0, university_count as u32);
                eav(program, &mut eavs, &grad, "undergraduate degree from", &format!("university{:?}", degree));
                // @TODO
                // every GraduateStudent co-authors 0~5 Publications with some Professors
                let paper_count = rand_between(&mut seed, 0, 5);
                for _ in 0..paper_count {
                    let prof = random_professor(&mut seed, &department, full_professors_count, associate_professors_count, assistant_professors_count);
                    let pub_ix = rand_between(&mut seed, 0, *prof_to_pubs.get(&prof.to_string()).unwrap());
                    let publication = format!("{}|publication{}", prof, pub_ix);
                    eav(program, &mut eavs, &publication, "author", &grad);
                }
                // 1/5~1/4 of the GraduateStudents are chosen as TeachingAssistant for one Course
                if g_ix % TA_ratio == 0 {
                    let course = rand_between(&mut seed, 0, course_ix);
                    eav(program, &mut eavs, &grad, "teaching assistant for", &format!("{}|course{}", department, course));
                }
                // 1/4~1/3 of the GraduateStudents are chosen as ResearchAssistant
                if g_ix % RA_ratio == 0 {
                    eav(program, &mut eavs, &grad, "tag", "research assistant");
                }
            }
        }
    }
    println!("undergrad count: {:?}", counter);
    eavs
}

// #[cfg(test)]
pub mod tests {
    extern crate test;

    use super::*;
    use self::test::Bencher;
    use self::test::BenchMode;
    use std::time::Duration;

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

    pub fn exec_query(program:&mut Program, constraints: Vec<Constraint>) {
        program.blocks.pop();

        let mut start_ns = time::precise_time_ns();
        program.register_block(Block { name: "simple block".to_string(), constraints, pipes: vec![] });
        let mut end_ns = time::precise_time_ns();
        // println!("Compile took {:?}", (end_ns - start_ns) as f64 / 1_000_000.0);

        let mut results: Vec<u32> = vec![];
        start_ns = time::precise_time_ns();
        let mut all_results = vec![];
        for _ in 0..20 {
            let results = program.exec_query();
            all_results.push(results);
        }
        end_ns = time::precise_time_ns();
        println!("Run took {:?}", (end_ns - start_ns) as f64 / 20_000_000.0);
        println!("Results: {:?}", all_results[0].len());
        // println!("Results: {:?}", results);
    }

    pub fn do_bench(b: &mut Bencher, func: fn(&mut Program) -> Vec<Constraint>) {
        let mut program = Program::new();
        setup(&mut program, 1000);

        let constraints = func(&mut program);

        let mut start = Instant::now();
        program.register_block(Block { name: "simple block".to_string(), constraints, pipes: vec![] });
        let mut dur = start.elapsed();
        println!("Compile took {:?}", (dur.as_secs() * 1000) as f32 + (dur.subsec_nanos() as f32) / 1_000_000.0);
        let mut all_results = vec![];
        // start = Instant::now();
        b.iter(|| {
            let results = program.exec_query();
            all_results.push(results);
        });
        println!("results: {:?}", all_results[0].len());
    }

    #[bench] pub fn bench_lubm_1(b: &mut Bencher) { do_bench(b, lubm_1); }
    #[bench] pub fn bench_lubm_2(b: &mut Bencher) { do_bench(b, lubm_2); }
    #[bench] pub fn bench_lubm_3(b: &mut Bencher) { do_bench(b, lubm_3); }
    #[bench] pub fn bench_lubm_4(b: &mut Bencher) { do_bench(b, lubm_4); }
    #[bench] pub fn bench_lubm_5(b: &mut Bencher) { do_bench(b, lubm_5); }
    #[bench] pub fn bench_lubm_7(b: &mut Bencher) { do_bench(b, lubm_7); }
    #[bench] pub fn bench_lubm_8(b: &mut Bencher) { do_bench(b, lubm_8); }
    #[bench] pub fn bench_lubm_9(b: &mut Bencher) { do_bench(b, lubm_9); }
    #[bench] pub fn bench_lubm_11(b: &mut Bencher) { do_bench(b, lubm_11); }
    #[bench] pub fn bench_lubm_12(b: &mut Bencher) { do_bench(b, lubm_12); }
    #[bench] pub fn bench_lubm_13(b: &mut Bencher) { do_bench(b, lubm_13); }
    #[bench] pub fn bench_lubm_14(b: &mut Bencher) { do_bench(b, lubm_14); }

    // #[test]
    pub fn test_lubm() {
        let mut program = Program::new();
        setup(&mut program, 1000);

        println!("\nQuery 1");
        let query_1 = lubm_1(&mut program);
        exec_query(&mut program, query_1);

        println!("\nQuery 2");
        let query_2 = lubm_2(&mut program);
        exec_query(&mut program, query_2);

        println!("\nQuery 3");
        let query_3 = lubm_3(&mut program);
        exec_query(&mut program, query_3);

        println!("\nQuery 4");
        let query_4 = lubm_4(&mut program);
        exec_query(&mut program, query_4);

        println!("\nQuery 5");
        let query_5 = lubm_5(&mut program);
        exec_query(&mut program, query_5);

        println!("\nQuery 7");
        let query_7 = lubm_7(&mut program);
        exec_query(&mut program, query_7);

        println!("\nQuery 8");
        let query_8 = lubm_8(&mut program);
        exec_query(&mut program, query_8);

        println!("\nQuery 9");
        let query_9 = lubm_9(&mut program);
        exec_query(&mut program, query_9);

        println!("\nQuery 11");
        let query_11 = lubm_11(&mut program);
        exec_query(&mut program, query_11);

        println!("\nQuery 12");
        let query_12 = lubm_12(&mut program);
        exec_query(&mut program, query_12);

        println!("\nQuery 13");
        let query_13 = lubm_13(&mut program);
        exec_query(&mut program, query_13);

        println!("\nQuery 14");
        let query_14 = lubm_14(&mut program);
        exec_query(&mut program, query_14);

    }

    pub fn lubm_1(program:&mut Program) -> Vec<Constraint> {
        vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("graduate student")),
            make_scan(register(0), program.interner.string("takes course"), program.interner.string("university0|department0|graduate_course0")),
            Constraint::Project { registers: vec![0] },
        ]
    }

    pub fn lubm_2(program:&mut Program) -> Vec<Constraint> {
        vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("graduate student")),
            make_scan(register(1), program.interner.string("tag"), program.interner.string("university")),
            make_scan(register(2), program.interner.string("tag"), program.interner.string("department")),
            make_scan(register(0), program.interner.string("member of"), register(2)),
            make_scan(register(2), program.interner.string("suborganization of"), register(1)),
            make_scan(register(0), program.interner.string("undergraduate degree from"), register(1)),
            Constraint::Project { registers: vec![0, 1, 2] },
        ]
    }

    pub fn lubm_3(program:&mut Program) -> Vec<Constraint> {
        vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("publication")),
            make_scan(register(0), program.interner.string("author"), program.interner.string("university0|department0|assistant_professor0")),
            Constraint::Project { registers: vec![0] },
        ]
    }

    pub fn lubm_4(program:&mut Program) -> Vec<Constraint> {
        vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("associate_professor")),
            make_scan(register(0), program.interner.string("works for"), program.interner.string("university0|department0")),
            make_scan(register(0), program.interner.string("name"), register(1)),
            make_scan(register(0), program.interner.string("email"), register(2)),
            make_scan(register(0), program.interner.string("telephone"), register(3)),
            Constraint::Project { registers: vec![0, 1, 2, 3] },
        ]
    }

    pub fn lubm_5(program:&mut Program) -> Vec<Constraint> {
        vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("undergraduate student")),
            make_scan(register(0), program.interner.string("member of"), program.interner.string("university0|department0")),
            Constraint::Project { registers: vec![0] },
        ]
    }

    pub fn lubm_7(program:&mut Program) -> Vec<Constraint> {
        vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("undergraduate student")),
            make_scan(register(1), program.interner.string("tag"), program.interner.string("course")),
            make_scan(register(0), program.interner.string("takes course"), register(1)),
            make_scan(program.interner.string("university0|department0|associate_professor0"), program.interner.string("teacher of"), register(1)),
            make_scan(register(0), program.interner.string("telephone"), register(2)),
            Constraint::Project { registers: vec![0, 1, 2] },
        ]
    }

    pub fn lubm_8(program:&mut Program) -> Vec<Constraint> {
        vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("undergraduate student")),
            make_scan(register(1), program.interner.string("tag"), program.interner.string("department")),
            make_scan(register(0), program.interner.string("member of"), register(1)),
            make_scan(register(1), program.interner.string("suborganization of"), program.interner.string("university0")),
            make_scan(register(0), program.interner.string("email"), register(2)),
            Constraint::Project { registers: vec![0, 1, 2] },
        ]
    }

    pub fn lubm_9(program:&mut Program) -> Vec<Constraint> {
        vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("undergraduate student")),
            make_scan(register(1), program.interner.string("tag"), program.interner.string("course")),
            make_scan(register(2), program.interner.string("tag"), program.interner.string("assistant_professor")),
            make_scan(register(0), program.interner.string("advisor"), register(2)),
            make_scan(register(2), program.interner.string("teacher of"), register(1)),
            make_scan(register(0), program.interner.string("takes course"), register(1)),
            Constraint::Project { registers: vec![0, 1, 2] },
        ]
    }

    pub fn lubm_11(program:&mut Program) -> Vec<Constraint> {
        vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("research group")),
            make_scan(register(0), program.interner.string("suborganization of"), program.interner.string("university0")),
            Constraint::Project { registers: vec![0] },
        ]
    }

    pub fn lubm_12(program:&mut Program) -> Vec<Constraint> {
        vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("full_professor")),
            make_scan(register(1), program.interner.string("tag"), program.interner.string("department")),
            make_scan(register(0), program.interner.string("works for"), register(1)),
            make_scan(register(1), program.interner.string("suborganization of"), program.interner.string("university0")),
            Constraint::Project { registers: vec![0, 1] },
        ]
    }

    pub fn lubm_13(program:&mut Program) -> Vec<Constraint> {
        vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("graduate student")),
            make_scan(register(0), program.interner.string("undergraduate degree from"), program.interner.string("university5")),
            Constraint::Project { registers: vec![0] },
        ]
    }

    pub fn lubm_14(program:&mut Program) -> Vec<Constraint> {
        vec![
            make_scan(register(0), program.interner.string("tag"), program.interner.string("undergraduate student")),
            Constraint::Project { registers: vec![0] },
        ]
    }

}
