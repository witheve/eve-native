#![feature(test)]

extern crate test;
extern crate time;
extern crate eve;
extern crate csv;

use eve::ops::{Program};
use test::Bencher;


pub fn load(program:&mut Program) {
    let mut eavs = vec![];
    macro_rules! n (($i:expr) => ({ program.interner.number_id($i as f32) }));
    macro_rules! s (($i:expr) => ({ program.interner.string_id(&$i) }));
    macro_rules! eav (($e:expr,$a:expr,$v:expr) => ({ eavs.push(($e,$a,$v)) }));
    macro_rules! csv_eav (($rec:ident, $attr:expr, $idx:tt, String) => { if let Some(v) = $rec.$idx { eav!(n!($rec.0), s!($attr), s!(v)); } };
                          ($rec:ident, $attr:expr, $idx:tt, f32) => { if let Some(v) = $rec.$idx { eav!(n!($rec.0), s!($attr), n!(v)); } };
                          ($rec:ident, $attr:expr, $idx:tt, i32) => { if let Some(v) = $rec.$idx { eav!(n!($rec.0), s!($attr), n!(v)); } };
                          );
    macro_rules! csv (($file:expr, $tag:expr, ($(($idx:tt, $attr:expr,$type:tt)),*)) => ({
        let mut rdr = csv::Reader::from_file("./data/imdb/".to_string()+$file).unwrap();
        for record in rdr.decode() {
            let record:(u32 $(,Option<$type>)*) = record.unwrap();
            eav!(n!(record.0), s!("tag"), s!($tag));
            $( csv_eav!(record, $attr, $idx, $type); )*
        }
    }));

// CREATE TABLE aka_name (
//     id integer NOT NULL PRIMARY KEY,
//     person_id integer NOT NULL,
//     name character varying,
//     imdb_index character varying(3),
//     name_pcode_cf character varying(11),
//     name_pcode_nf character varying(11),
//     surname_pcode character varying(11),
//     md5sum character varying(65)
// );

// CREATE TABLE aka_title (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer NOT NULL,
//     title character varying,
//     imdb_index character varying(4),
//     kind_id integer NOT NULL,
//     production_year integer,
//     phonetic_code character varying(5),
//     episode_of_id integer,
//     season_nr integer,
//     episode_nr integer,
//     note character varying(72),
//     md5sum character varying(32)
// );

// CREATE TABLE cast_info (
//     id integer NOT NULL PRIMARY KEY,
//     person_id integer NOT NULL,
//     movie_id integer NOT NULL,
//     person_role_id integer,
//     note character varying,
//     nr_order integer,
//     role_id integer NOT NULL
// );

// CREATE TABLE char_name (
//     id integer NOT NULL PRIMARY KEY,
//     name character varying NOT NULL,
//     imdb_index character varying(2),
//     imdb_id integer,
//     name_pcode_nf character varying(5),
//     surname_pcode character varying(5),
//     md5sum character varying(32)
// );

// CREATE TABLE comp_cast_type (
//     id integer NOT NULL PRIMARY KEY,
//     kind character varying(32) NOT NULL
// );

// CREATE TABLE company_name (
//     id integer NOT NULL PRIMARY KEY,
//     name character varying NOT NULL,
//     country_code character varying(6),
//     imdb_id integer,
//     name_pcode_nf character varying(5),
//     name_pcode_sf character varying(5),
//     md5sum character varying(32)
// );

// CREATE TABLE company_type (
//     id integer NOT NULL PRIMARY KEY,
//     kind character varying(32)
// );

// CREATE TABLE complete_cast (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer,
//     subject_id integer NOT NULL,
//     status_id integer NOT NULL
// );

// CREATE TABLE info_type (
//     id integer NOT NULL PRIMARY KEY,
//     info character varying(32) NOT NULL
// );

// CREATE TABLE keyword (
//     id integer NOT NULL PRIMARY KEY,
//     keyword character varying NOT NULL,
//     phonetic_code character varying(5)
// );

// CREATE TABLE kind_type (
//     id integer NOT NULL PRIMARY KEY,
//     kind character varying(15)
// );

// CREATE TABLE link_type (
//     id integer NOT NULL PRIMARY KEY,
//     link character varying(32) NOT NULL
// );

// CREATE TABLE movie_companies (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer NOT NULL,
//     company_id integer NOT NULL,
//     company_type_id integer NOT NULL,
//     note character varying
// );


// CREATE TABLE movie_keyword (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer NOT NULL,
//     keyword_id integer NOT NULL
// );

// CREATE TABLE movie_link (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer NOT NULL,
//     linked_movie_id integer NOT NULL,
//     link_type_id integer NOT NULL
// );

// CREATE TABLE name (
//     id integer NOT NULL PRIMARY KEY,
//     name character varying NOT NULL,
//     imdb_index character varying(9),
//     imdb_id integer,
//     gender character varying(1),
//     name_pcode_cf character varying(5),
//     name_pcode_nf character varying(5),
//     surname_pcode character varying(5),
//     md5sum character varying(32)
// );

// CREATE TABLE role_type (
//     id integer NOT NULL PRIMARY KEY,
//     role character varying(32) NOT NULL
// );

// CREATE TABLE title (
//     id integer NOT NULL PRIMARY KEY,
//     title character varying NOT NULL,
//     imdb_index character varying(5),
//     kind_id integer NOT NULL,
//     production_year integer,
//     imdb_id integer,
//     phonetic_code character varying(5),
//     episode_of_id integer,
//     season_nr integer,
//     episode_nr integer,
//     series_years character varying(49),
//     md5sum character varying(32)
// );

// CREATE TABLE movie_info (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer NOT NULL,
//     info_type_id integer NOT NULL,
//     info character varying NOT NULL,
//     note character varying
// );

// CREATE TABLE person_info (
//     id integer NOT NULL PRIMARY KEY,
//     person_id integer NOT NULL,
//     info_type_id integer NOT NULL,
//     info character varying NOT NULL,
//     note character varying
// );

// CREATE TABLE movie_info_idx (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer NOT NULL,
//     info_type_id integer NOT NULL,
//     info character varying NOT NULL,
//     note character varying(1)
// );
// CREATE TABLE info_type (
//     id integer NOT NULL PRIMARY KEY,
//     info character varying(32) NOT NULL
// );
// CREATE TABLE title (
//     id integer NOT NULL PRIMARY KEY,
//     title character varying NOT NULL,
//     imdb_index character varying(5),
//     kind_id integer NOT NULL,
//     production_year integer,
//     imdb_id integer,
//     phonetic_code character varying(5),
//     episode_of_id integer,
//     season_nr integer,
//     episode_nr integer,
//     series_years character varying(49),
//     md5sum character varying(32)
// );
    csv!("keyword.csv", "keyword", ((1, "keyword", String), (2, "phonetic-code", String)));
    csv!("movie_info_idx.csv", "movie-info-idx", ((1, "movie-id", i32), (2, "info-type-id", i32), (3, "info", String), (4, "note", String)));
    csv!("movie_keyword.csv", "movie-keyword", ((1, "movie-id", i32), (2, "keyword-id", i32)));
    csv!("info_type.csv", "info-type", ((1, "info", String)));
    csv!("title_subset.csv", "title", ((1, "title", String),
                                (2, "imdb-index", String),
                                (3, "kind-id", i32),
                                (4, "production-year", i32),
                                (5, "imdb-id", i32),
                                (6, "phonetic-code", String),
                                (7, "episode-of-id", i32),
                                (8, "season-nr", i32),
                                (9, "episode-nr", i32),
                                (10, "series-years", String),
                                (11, "md5sum", String)
                                ));
    println!("num: {:?}", eavs.len());
    let start_ns = time::precise_time_ns();
    for (e,a,v) in eavs {
        program.raw_insert(e,a,v,0,1);
    }
    let end_ns = time::precise_time_ns();
    println!("Insert took {:?}", (end_ns - start_ns) as f64 / 1_000_000.0);
}

#[bench]
pub fn job_4b(b: &mut Bencher) {
// SELECT Min(mi_idx.info) AS rating,
//        Min(t.title)     AS movie_title
// FROM   info_type AS it,
//        keyword AS k,
//        movie_info_idx AS mi_idx,
//        movie_keyword AS mk,
//        title AS t
// WHERE  it.info = 'rating'
//        AND k.keyword LIKE '%sequel%'
//        AND mi_idx.info > '9.0'
//        AND t.production_year > 2010
//        AND t.id = mi_idx.movie_id
//        AND t.id = mk.movie_id
//        AND mk.movie_id = mi_idx.movie_id
//        AND k.id = mk.keyword_id
//        AND it.id = mi_idx.info_type_id;

    let mut program = Program::new();
    load(&mut program);
    // program.block("job_4b", r#"
    //     search
    //         info-type-id = [#info-type info:"rating"]
    //         movie-id = [#title title production-year > 2010]
    //         keyword-id = [#keyword keyword]
    //         [#movie-info-idx movie-id info-type-id info > "9.0"]
    //         [#movie-keyword movie-id keyword-id]
    //     project
    //         (info, title)
    // "#);
    program.block("job_4b", r#"
        search
            movie-id = [title production-year > 2010]
            info-type-id = [info: "rating"]
            [#movie-info-idx movie-id info-type-id info > "9.0"]
        project
            (movie-id)
    "#);
    let mut all_results = vec![];
    b.iter(|| {
        let results = program.exec_query();
        all_results.push(results);
    });
    println!("results: {:?}", all_results[0].len());
}

