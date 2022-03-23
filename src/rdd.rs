use std::collections::HashMap;
use std::fmt;

#[derive(Debug,Clone)]
pub struct RDD<T:ToString> {
    elements : Vec<T>, // maybe reference instead like incendium?
}

#[derive(Clone)]
pub struct Tuple(String, i32);
//impl Copy for Tuple {};

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}


impl<'a, T:ToString> RDD<T> {

    pub fn count(&self) -> usize {
        return self.elements.len();
    }

    pub fn flat_map(&self) -> RDD<String> {
        let mut new_elements = Vec::new();

        for elem in self.elements.iter() {
            // need to avoid to_string since it goes through formatting system instead of 
            // just directly constructing the result
            let res : Vec<String> = elem.to_string().split(" ").map(str::to_string).collect();
            new_elements.extend(res);
        }
        return RDD{elements : new_elements}; // Return pointer instead
    }

    pub fn map(&self) -> RDD<Tuple> where T:Copy {
        let mut new_elements : Vec<Tuple<>> = Vec::new();

        for elem in self.elements.iter() {
            new_elements.push(Tuple(elem.to_string(), 1));
        }

        return RDD{elements : new_elements};
    }

    // Currently implementing addition only for WordCount example
    pub fn reduce_by_key(a : RDD<Tuple<>>) -> RDD<Tuple<>> {
        let mut seen : HashMap<&String, usize> = HashMap::new();
        let mut res = Vec::new();
         
        for tuple in a.elements.iter() {
            if seen[&tuple.0] == 0 {
                let pos = res.len();
                res.push((*tuple).clone());
                seen.insert(&tuple.0, pos);
            } else {
                let pos = seen[&tuple.0];
                res[pos].1 += 1;
            } 
        } 

        return RDD{elements : res};
    }
    
}

