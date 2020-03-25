
extern crate rand;
use rand::Rng;

use std::env;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::fs::{File, OpenOptions};
use std::f32;
use std::thread;
use std::sync::{Arc, Barrier, Mutex};
use bytes::Buf;


fn main() {
    let args : Vec<String> = env::args().collect();
    if args.len() != 4 {
        println!("Usage: {} <threads> input output", args[0]);
    }

    let threads =  args[1].parse::<usize>().unwrap();
    let inp_path = &args[2];
    let out_path = &args[3];

    // Sample
    // Calculate pivots
    let mut inpf = File::open(inp_path).unwrap();
    let size = read_size(&mut inpf);
    let pivots = find_pivots(&mut inpf, threads);

    // Create output file
    {
        let mut outf = File::create(out_path).unwrap();
        let tmp = size.to_ne_bytes();
        outf.write_all(&tmp).unwrap();
        outf.set_len(size).unwrap();
    }

    let mut workers = vec![];

    // Spawn worker threads
    let sizes = Arc::new(Mutex::new(vec![0u64; threads]));
    let barrier = Arc::new(Barrier::new(threads));

    for ii in 0..threads {
        let inp = inp_path.clone();
        let out = out_path.clone();
        let piv = pivots.clone();
        let szs = sizes.clone();
        let bar = barrier.clone();

        let tt = thread::spawn(move || {
            worker(ii, inp, out, piv, szs, bar);
        });
        workers.push(tt);
    }

    // Join worker threads
    for tt in workers {
        tt.join().unwrap();
    }
}

fn read_size(file: &mut File) -> u64 {
    // TODO: Read size field from data file
    let mut buf = [0u8];
    file.read(&mut buf).unwrap();
    //println!("{:?}\n", buf[0]);
    let size1 = buf[0] as u64;
    size1
}

fn read_item(file: &mut File, ii: u64) -> f32 {
    // TODO: Read the ii'th float from data file
    //let size1 = read_size(&mut file);
    file.seek(SeekFrom::Start(8)).unwrap();
    let item1 = 0f32;
    let mut tmp = [0u8;4];
    file.seek(SeekFrom::Start(8 + ii*4)).unwrap();
    file.read_exact(&mut tmp).unwrap();
    let item1 = Cursor::new(tmp).get_f32_le();
    //println!("{:?}\n", item1); 
    item1
}

fn sample(file: &mut File, count: usize, size: u64) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    let mut rand_items = vec![];
    let mut sample = vec![];

    // TODO: Sample 'count' random items from the
    // provided file
    let sample_size = 3 * (count - 1);
    for i in 0..sample_size{
        let jj = rng.gen_range(0,sample_size);
        let j = jj as u64;
        rand_items [i] = read_item(file,j);
    }

    rand_items.sort_by(|a, b| a.partial_cmp(b).unwrap());

    



    sample
}

fn find_pivots(file: &mut File, threads: usize) -> Vec<f32> {
    // TODO: Sample 3*(threads-1) items from the file
    // TODO: Sort the sampled list
    let mut pivots = vec![0f32];

    // TODO: push the pivots into the array

    pivots.push(f32::INFINITY);
    pivots
}

fn worker(tid: usize, inp_path: String, out_path: String, pivots: Vec<f32>,
          sizes: Arc<Mutex<Vec<u64>>>, bb: Arc<Barrier>) {

    // TODO: Open input as local fh

    // TODO: Scan to collect local data
    let data = vec![0f32, 1f32];

    // TODO: Write local size to shared sizes
    /*{
        // curly braces to scope our lock guard
    }*/

    // TODO: Sort local data

    // Here's our printout
    println!("{}: start {}, count {}", tid, &data[0], data.len());

    // TODO: Write data to local buffer
    {
    //let mut cur = Cursor::new(vec![]);
    //read_item
    }

    // TODO: Get position for output file
    let prev_count = {
        // curly braces to scope our lock guard
        5
    };

    /*
    let mut outf = OpenOptions::new()
        .read(true)
        .write(true)
        .open(out_path).unwrap();
    */
    // TODO: Seek and write local buffer.

    // TODO: Figure out where the barrier goes.
}
