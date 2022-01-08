use criterion::{black_box, criterion_group, criterion_main, Criterion};

// ------------- Thing -------------
pub type Thing = u64; 

// used for internal result sets
use roaring::RoaringTreemap;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResultSetMode {
    Empty,
    Thing, 
    Multi
}

#[derive(Debug)]
pub struct ResultSet {
    mode: ResultSetMode,
    thing: Option<Thing>,
    multi: Option<RoaringTreemap>
}
impl ResultSet {
    pub fn new() -> Self {
        Self {
            mode: ResultSetMode::Empty,
            thing: None,
            multi: None,
        }
    }
    fn empty(&mut self) {
        self.mode = ResultSetMode::Empty;
        self.thing = None;
        self.multi = None;  
    }
    fn thing(&mut self, thing: Thing) {
        self.mode = ResultSetMode::Thing;
        self.thing = Some(thing);
        self.multi = None;
    }
    fn multi(&mut self, multi: RoaringTreemap) {
        self.mode = ResultSetMode::Multi;
        self.thing = None;
        self.multi = Some(multi);
    }
    pub fn intersect_with(&mut self, other: &ResultSet) {
        if self.mode != ResultSetMode::Empty {
            match (&self.mode, &other.mode) {
                (_, ResultSetMode::Empty) => {
                    self.empty();
                }, 
                (ResultSetMode::Thing, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    if self.thing.unwrap() != other_thing {
                        self.empty();
                    }
                },
                (ResultSetMode::Multi, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    if self.multi.as_ref().unwrap().contains(other_thing) {
                        self.thing(other_thing);
                    }
                    else {
                        self.empty();
                    }
                },
                (ResultSetMode::Thing, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    if !other_multi.contains(self.thing.unwrap()) {
                        self.empty();
                    }
                },
                (ResultSetMode::Multi, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    // this is instead of the deprecated intersect_with
                    *self.multi.as_mut().unwrap() &= other_multi; 
                    match self.multi.as_ref().unwrap().len() {
                        0 => {
                            self.empty();
                        },
                        1 => {
                            let thing = self.multi.as_ref().unwrap().min().unwrap();
                            self.thing(thing);
                        },
                        _ => ()
                    }
                },
                (_, _) => ()
            }
        }
    }

    /* 
    pub fn union_with(&mut self, other: &ResultSet) {
        let mut merge = HashSet::<u64>::new();
        for u in &self.small {
            merge.insert(*u);
        }
        self.small.clear();
        for u in &other.small {
            merge.insert(*u);
        }
        for u in &merge {
            self.small.push(*u);
        }
    }
    */
    pub fn push(&mut self, thing: u64) {
        match self.mode {
            ResultSetMode::Empty => {
                self.thing(thing);
            }, 
            ResultSetMode::Thing => {
                let mut multi = RoaringTreemap::new(); 
                multi.insert(self.thing.unwrap());
                multi.insert(thing);
                self.multi(multi);
            },   
            ResultSetMode::Multi => {
                self.multi.as_mut().unwrap().push(thing);
            }    
        }
    }
}



pub fn criterion_benchmark(c: &mut Criterion) {
    let mut r1 = ResultSet::new();
    let mut r2 = ResultSet::new();
    println!("{:?}", r1);
    c.bench_function("intersect 0", |b| b.iter(|| r1.intersect_with(&r2)));
    r1.push(42);
    r2.push(42);
    println!("{:?}", r1);
    c.bench_function("intersect 1", |b| b.iter(|| r1.intersect_with(&r2)));
    for n in 1..1000 {
        r1.push(n);
        r2.push(n);
    }
    println!("{:?}", r1);
    c.bench_function("intersect 1k", |b| b.iter(|| r1.intersect_with(&r2)));
    for n in 100000..200000 {
        r1.push(n);
        r2.push(n);
    }
    println!("{:?}", r1);
    c.bench_function("intersect 100k", |b| b.iter(|| r1.intersect_with(&r2)));
    for n in 1000000..2000000 {
        r1.push(n);
        r2.push(n);
    }
    println!("{:?}", r1);
    c.bench_function("intersect 1M", |b| b.iter(|| r1.intersect_with(&r2)));
    for n in 10000000..20000000 {
        r1.push(n);
        r2.push(n);
    }
    println!("{:?}", r1);
    c.bench_function("intersect 10M", |b| b.iter(|| r1.intersect_with(&r2)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);