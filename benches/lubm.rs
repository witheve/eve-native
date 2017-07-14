#![feature(test)]

extern crate test;
extern crate time;
extern crate eve;

use std::num::Wrapping;
use eve::ops::{Program, make_scan, Constraint, Block, register};
use std::collections::HashMap;
use test::Bencher;

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
    eavs.push((program.state.interner.string_id(&e), program.state.interner.string_id(&a), program.state.interner.string_id(&v)))
}

fn make_faculty(program:&mut Program, eavs:&mut Vec<(u32,u32,u32)>, university_count:usize, department:&str, cur_type:&str, ix:u32, publications:u32, course_ix: &mut u32, grad_course_ix: &mut u32, mut seed: &mut u32, prof_to_pubs: &mut HashMap<String, u32>) {
    let prof = format!("{}|{}{}", department, cur_type, ix);
    eav(program, eavs, &prof, "tag", cur_type);
    eav(program, eavs, &prof, "works-for", department);
    eav(program, eavs, &prof, "name", &format!("{}|name", prof));
    eav(program, eavs, &prof, "email", &format!("{}@foo.edu", prof));
    eav(program, eavs, &prof, "telephone", "123-123-1234");
    eav(program, eavs, &prof, "research-interest", "blah");
    // every Faculty is teacherOf 1~2 Courses
    for _ in 0..rand_between(&mut seed, 1, 2) {
        let course = format!("{}|course{}", department, *course_ix);
        eav(program, eavs, &course, "tag", "course");
        eav(program, eavs, &course, "name", "foo");
        eav(program, eavs, &prof, "teacher-of", &course);
        *course_ix += 1;
    }
    // every Faculty is teacherOf 1~2 GraduateCourses
    for _ in 0..rand_between(&mut seed, 1, 2) {
        let course = format!("{}|graduate_course{}", department, *grad_course_ix);
        eav(program, eavs, &course, "tag", "graduate-course");
        eav(program, eavs, &course, "name", "foo");
        eav(program, eavs, &prof, "teacher-of", &course);
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
    eav(program, eavs, &prof, "undergraduate-degree-from", &format!("university{:?}", ugrad));

    let masters = rand_between(&mut seed, 0, university_count as u32);
    eav(program, eavs, &prof, "masters-degree-from", &format!("university{:?}", masters));

    let phd = rand_between(&mut seed, 0, university_count as u32);
    eav(program, eavs, &prof, "doctoral-degree-from", &format!("university{:?}", phd));
}

fn random_professor(seed:&mut u32, department:&str, fulls:u32, associates:u32, assistants:u32) -> String {
    let random_type = rand_between(seed, 1, 3);
    let (prof_type, id) = match random_type {
        1 => {
            let id = rand_between(seed, 0, fulls - 1);
            ("full-professor", id)
        },
        2 => {
            let id = rand_between(seed, 0, associates - 1);
            ("associate-professor", id)
        },
        3 => {
            let id = rand_between(seed, 0, assistants - 1);
            ("assistant-professor", id)
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
            eav(program, &mut eavs, &department, "suborganization-of", &university);

            let mut course_ix = 0;
            let mut grad_course_ix = 0;
            let mut total_faculty = 0;
            // 7~10 FullProfessors worksFor the Department
            let full_professors_count = rand_between(&mut seed, 7, 10);
            for fp_ix in 0..full_professors_count {
                // every FullProfessor is publicationAuthor of 15~20 Publications
                let publications = rand_between(&mut seed, 15, 20);
                make_faculty(program, &mut eavs, university_count, &department, "full-professor", fp_ix, publications, &mut course_ix, &mut grad_course_ix, &mut seed, &mut prof_to_pubs);
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
                make_faculty(program, &mut eavs, university_count, &department, "associate-professor", ap_ix, publications, &mut course_ix, &mut grad_course_ix, &mut seed, &mut prof_to_pubs);
                total_faculty += 1;
            }

            // 8~11 AssistantProfessors worksFor the Department
            let assistant_professors_count = rand_between(&mut seed, 8, 11);
            for asp_ix in 0..assistant_professors_count {
                // every AssistantProfessor is publicationAuthor of 5~10 Publications
                let publications = rand_between(&mut seed, 5, 10);
                make_faculty(program, &mut eavs, university_count, &department, "assistant-professor", asp_ix, publications, &mut course_ix, &mut grad_course_ix, &mut seed, &mut prof_to_pubs);
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
                eav(program, &mut eavs, &group, "tag", "research-group");
                eav(program, &mut eavs, &group, "suborganization-of", &department);
                // eav(program, &mut eavs, &group, "suborganization of", &university);
            }

            // UndergraduateStudent : Faculty = 8~14 : 1
            let undergrads_count = rand_between(&mut seed, 8, 14) * total_faculty;
            if university == "university0" {
                counter += undergrads_count;
            }
            for ug_ix in 0..undergrads_count {
                let undergrad = format!("{}|undergrad{}", department, ug_ix);
                eav(program, &mut eavs, &undergrad, "tag", "undergraduate-student");
                eav(program, &mut eavs, &undergrad, "name", &format!("{}|name", undergrad));
                eav(program, &mut eavs, &undergrad, "email", &format!("{}@foo.edu", undergrad));
                eav(program, &mut eavs, &undergrad, "telephone", "123-123-1234");
                // every Student is memberOf the Department
                eav(program, &mut eavs, &undergrad, "member-of", &department);
                // every UndergraduateStudent takesCourse 2~4 Courses
                let course_count = rand_between(&mut seed, 2, 4);
                for _ in 0..course_count {
                    let undergrad_course_ix = rand_between(&mut seed, 0, course_ix);
                    eav(program, &mut eavs, &undergrad, "takes-course", &format!("{}|course{}", department, undergrad_course_ix));
                }
                // 1/5 of the UndergraduateStudents have a Professor as their advisor
                if ug_ix % 5 == 0 {
                    let prof = random_professor(&mut seed, &department, full_professors_count, associate_professors_count, assistant_professors_count);
                    eav(program, &mut eavs, &undergrad, "advisor", &prof);
                }
            }
            // GraduateStudent : Faculty = 3~4 : 1
            let ta_ratio = rand_between(&mut seed, 4, 5);
            let ra_ratio = rand_between(&mut seed, 3, 4);
            let grads_count = rand_between(&mut seed, 3, 4) * total_faculty;
            // @TODO this should be grads_count
            for g_ix in 0..grads_count {
                let grad = format!("{}|graduate{}", department, g_ix);
                eav(program, &mut eavs, &grad, "tag", "graduate-student");
                eav(program, &mut eavs, &grad, "name", &format!("{}|name", grad));
                eav(program, &mut eavs, &grad, "email", &format!("{}@foo.edu", grad));
                eav(program, &mut eavs, &grad, "telephone", "123-123-1234");
                // if department_ix == 0 {
                //     println!("grad student: {:?}, {:?} == {:?}", &grad, program.state.interner.string_id(&grad), program.state.interner.string_id(&grad));
                // }
                // every Student is memberOf the Department
                eav(program, &mut eavs, &grad, "member-of", &department);
                // every GraduateStudent takesCourse 1~3 GraduateCourses
                let course_count = rand_between(&mut seed, 1, 3);
                for _ in 0..course_count {
                    let course_ix = rand_between(&mut seed, 0, grad_course_ix);
                    eav(program, &mut eavs, &grad, "takes-course", &format!("{}|graduate_course{}", department, course_ix));
                }
                // every GraduateStudent has a Professor as his advisor
                let prof = random_professor(&mut seed, &department, full_professors_count, associate_professors_count, assistant_professors_count);
                eav(program, &mut eavs, &grad, "advisor", &prof);
                // every GraudateStudent has an undergraduateDegreeFrom a University
                let degree = rand_between(&mut seed, 0, university_count as u32);
                eav(program, &mut eavs, &grad, "undergraduate-degree-from", &format!("university{:?}", degree));
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
                if g_ix % ta_ratio == 0 {
                    let course = rand_between(&mut seed, 0, course_ix);
                    eav(program, &mut eavs, &grad, "teaching-assistant-for", &format!("{}|course{}", department, course));
                }
                // 1/4~1/3 of the GraduateStudents are chosen as ResearchAssistant
                if g_ix % ra_ratio == 0 {
                    eav(program, &mut eavs, &grad, "tag", "research-assistant");
                }
            }
        }
    }
    // println!("undergrad count: {:?}", counter);
    eavs
}

#[allow(unused_assignments)]
fn setup(program: &mut Program, size:usize) {
    let mut start_ns = time::precise_time_ns();
    let eavs = generate(program, size);
    let mut end_ns = time::precise_time_ns();
    // println!("Gen took {:?}", (end_ns - start_ns) as f64 / 1_000_000.0);
    // println!("size: {:?}", eavs.len());
    start_ns = time::precise_time_ns();
    for (e,a,v) in eavs {
        program.raw_insert(e,a,v,0,1);
    }
    end_ns = time::precise_time_ns();
    // println!("Insert took {:?}", (end_ns - start_ns) as f64 / 1_000_000.0);
}

pub fn do_bench(b: &mut Bencher, name:&str, func: fn(&mut Program)) {
    let mut program = Program::new();
    setup(&mut program, 1);

    func(&mut program);

    let mut all_results = vec![];
    b.iter(|| {
        let results = program.exec_query(name);
        all_results.push(results);
    });
    println!("results: {:?}", all_results[0].len());
}

#[bench] pub fn do_lubm_1(b: &mut Bencher) { do_bench(b, "lubm1", lubm_1); }
#[bench] pub fn do_lubm_2(b: &mut Bencher) { do_bench(b, "lubm2", lubm_2); }
#[bench] pub fn do_lubm_3(b: &mut Bencher) { do_bench(b, "lubm3", lubm_3); }
#[bench] pub fn do_lubm_4(b: &mut Bencher) { do_bench(b, "lubm4", lubm_4); }
#[bench] pub fn do_lubm_5(b: &mut Bencher) { do_bench(b, "lubm5", lubm_5); }
#[bench] pub fn do_lubm_7(b: &mut Bencher) { do_bench(b, "lubm7", lubm_7); }
#[bench] pub fn do_lubm_8(b: &mut Bencher) { do_bench(b, "lubm8", lubm_8); }
#[bench] pub fn do_lubm_9(b: &mut Bencher) { do_bench(b, "lubm9", lubm_9); }
#[bench] pub fn do_lubm_11(b: &mut Bencher) { do_bench(b, "lubm11", lubm_11); }
#[bench] pub fn do_lubm_12(b: &mut Bencher) { do_bench(b, "lubm12", lubm_12); }
#[bench] pub fn do_lubm_13(b: &mut Bencher) { do_bench(b, "lubm13", lubm_13); }
#[bench] pub fn do_lubm_14(b: &mut Bencher) { do_bench(b, "lubm14", lubm_14); }

pub fn lubm_1(program:&mut Program) {
    program.insert_block("lubm1", r#"
        search
            person = [#graduate-student takes-course:"university0|department0|graduate_course0"]
        project
            (person)
    "#);
}

pub fn lubm_2(program:&mut Program) {
    program.insert_block("lubm2", r#"
        search
            person = [#graduate-student member-of:department undergraduate-degree-from:university]
            department = [#department suborganization-of:university]
            university = [#university]
        project
            (university department person)
    "#);
}

pub fn lubm_3(program:&mut Program) {
    program.insert_block("lubm3", r#"
        search
            pub = [#publication author:"university0|department0|assistant-professor0"]
        project
            (pub)
    "#);
}

pub fn lubm_4(program:&mut Program) {
    program.insert_block("lubm4", r#"
        search
            prof = [#associate-professor works-for:"university0|department0" name email telephone]
        project
            (prof name email telephone)
    "#);
}

pub fn lubm_5(program:&mut Program) {
    program.insert_block("lubm5", r#"
        search
            student = [#undergraduate-student member-of:"university0|department0"]
        project
            (student)
    "#);
}

pub fn lubm_7(program:&mut Program) {
    program.insert_block("lubm7", r#"
        search
            student = [#undergraduate-student takes-course:course telephone]
            course = [#course]
            "university0|department0|associate-professor0" = [teacher-of:course]
        project
            (student course telephone)
    "#);
}

pub fn lubm_8(program:&mut Program) {
    program.insert_block("lubm8", r#"
        search
            student = [#undergraduate-student member-of:department email]
            department = [#department suborganization-of:"university0"]
        project
            (student department email)
    "#);
}

pub fn lubm_9(program:&mut Program) {
    program.insert_block("lubm9", r#"
        search
            student = [#undergraduate-student advisor takes-course:course]
            advisor = [#assistant-professor teacher-of:course]
            course = [#course]
        project
            (student course advisor)
    "#);
}

pub fn lubm_11(program:&mut Program) {
    program.insert_block("lubm11", r#"
        search
            group = [#research-group suborganization-of:"university0"]
        project
            (group)
    "#);
}

pub fn lubm_12(program:&mut Program) {
    program.insert_block("lubm12", r#"
        search
            prof = [#full-professor works-for:department]
            department = [#department suborganization-of:"university0"]
        project
            (prof department)
    "#);
}

pub fn lubm_13(program:&mut Program) {
    program.insert_block("lubm13", r#"
        search
            student = [#graduate-student undergraduate-degree-from:"university5"]
        project
            (student)
    "#);
}

pub fn lubm_14(program:&mut Program) {
    program.insert_block("lubm14", r#"
        search
            student = [#undergraduate-student]
        project
            (student)
    "#);
}
