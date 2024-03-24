pub mod resp;

pub type Db = Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
