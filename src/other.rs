use std::ops::Deref;

struct Lock<T>(T);

impl<T> Deref for Lock<T> { 
    type Target = T; 
    fn deref(&self) -> &T { 
        &self.0 
    } 
}

use std::sync::{Arc};

    /*
    #[derive(Debug)]
    pub struct Index<'a, T: 'a + Eq + Hash + Copy> {
        index:  Vec<&'a T>,
        kept:   HashMap<T, usize> 
    } 
    impl<'a, T> Index<'a, T> where T: 'a + Eq + Hash + Copy {
        pub fn new() -> Index<'a, T> {
            Index { 
                index: Vec::new(), 
                kept:  HashMap::new() 
            }
        }
        pub fn keep(&mut self, keepsake: &'a T) -> usize {
            self.index.push(keepsake);
            match self.kept.entry(*keepsake) {
                Occupied(entry) => *entry.get(),
                Vacant(entry)   => *entry.insert(self.index.len() - 1)
            }        
        }
        pub fn find(&self, i:usize) -> &T {
            self.index[i]
        }
        pub fn index_of(&self, k:&T) -> Option<&usize> {
            self.kept.get(k)
        }
        pub fn count(&self) -> usize {
            self.index.len()
        }
    }

    trait DataMap {}
    impl<K,V> DataMap for HashMap<K,V> where K: Hash + Eq {}
    
    pub struct AnyIndex {
        index:   Vec<(usize, Rc<RefCell<DataMap>>)>,
        indexes: AnyMap,
        keeps:   AnyMap
    } 
    impl<'a> AnyIndex {
        pub fn new() -> AnyIndex {
            AnyIndex {
                index:   Vec::new(),
                indexes: AnyMap::new(),
                keeps:   AnyMap::new()
            }
        }
        pub fn keep<T>(&mut self, keepsake: T) -> usize where T: Eq + Hash + 'static {
            let k = Rc::new(keepsake);
            let keep: Rc<RefCell<HashMap<Rc<T>, usize>>> = match self.keeps.get::<Rc<RefCell<HashMap<Rc<T>, usize>>>>() {
                Some(map) => map.clone(),
                None => Rc::new(RefCell::new(HashMap::new()))
            };
            self.keeps.entry::<Rc<RefCell<HashMap<Rc<T>, usize>>>>().or_insert(keep.clone());
            let index_of_keep: Rc<RefCell<Vec<Rc<T>>>> = match self.indexes.get_mut::<Rc<RefCell<Vec<Rc<T>>>>>() {
                Some(vec) => vec.clone(),
                None => Rc::new(RefCell::new(Vec::new()))
            };
            self.indexes.entry::<Rc<RefCell<Vec<Rc<T>>>>>().or_insert(index_of_keep.clone());

            let return_value = match keep.borrow_mut().entry(k.clone()) {
                Occupied(entry) => *entry.get(),
                Vacant(entry)   => {
                    entry.insert(self.index.len()); // the index of indexes
                    self.index.push((index_of_keep.borrow().len(), keep.clone()));
                    self.index.len() - 1
                }
            };
            return_value
        }
    }
    */


    /* TODO
    static LOCAL: &str = "localhost";
    */


    /* TODO
    // set up the cluster mapping
    #[derive(Debug)]
    struct ClusterAddress<'a> {
        network_address:    &'a str,
        memory_address:     *const u64
    };

    // ----------- identity table -----------
    let mut cluster_map: HashMap<u64, ClusterAddress> = HashMap::new();    


    // insert a key only if it doesn't already exist
    cluster_map.entry(thing).or_insert(ClusterAddress { 
        network_address: LOCAL, 
        memory_address: &thing
    });
    cluster_map.entry(another_thing).or_insert(ClusterAddress { 
        network_address: LOCAL, 
        memory_address: &another_thing
    });

    for (identity, cluster_address) in &cluster_map {
        println!("Key: {}, Value: {:?}, Unsafe dereference: {}", 
            identity, 
            cluster_address.memory_address,
            unsafe {*cluster_address.memory_address}
        );
    }
    */

    /*
    let appearance = Appearance { 
        identity: thing, 
        role: role_keeper.keep("hair color")
    };
    println!("{} appears for {}.", role_keeper.find(appearance.role), appearance.identity);

 //   identity_to_appearance.insert(Arc::new(*thing), Arc::new(appearance));

    let p = Posit { // type inference lets us omit a type declaration, as in Posit::<&str>
        value: String::from("brown"), 
        time: Utc::now().timestamp(), 
        dereference: &Dereference::new(&mut [&appearance]).unwrap()
    };
    println!("The {} of {} is {} since {}.", 
        role_keeper.find(p.dereference.set[0].role), 
        p.dereference.set[0].identity, 
        p.value, 
        p.time
    );

    let me = 555;
    let a = Assertion {
        positor: &me,
        reliability: Reliability::new(0.9),
        time: Utc::now().timestamp(),
        posit: &p
    };
    println!("On {} {} stated that 'The {} of {} is {} since {}' with {} reliability.", 
        a.time,
        a.positor,
        role_keeper.find(a.posit.dereference.set[0].role), 
        a.posit.dereference.set[0].identity, 
        a.posit.value, 
        a.posit.time,
        a.reliability
    );
    println!("Reliability {} bytes", std::mem::size_of::<Reliability>());
    println!("Appearance {} bytes", std::mem::size_of::<Appearance>());
    println!("Dereference {} bytes", std::mem::size_of::<Dereference>());
    println!("Posit {} bytes", std::mem::size_of::<Posit<&str>>());
    println!("Assertion {} bytes", std::mem::size_of::<Assertion<&str>>());
    */

