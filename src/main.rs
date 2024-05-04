use chrono::DateTime;
use std::env;
use oneio;
use bgpkit_broker::{BgpkitBroker, BrokerItem};
use bgpkit_parser::models::AsPathSegment;
use bgpkit_parser::{BgpElem, BgpkitParser};
use crossbeam_channel::{unbounded, Receiver};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::process::exit;
use threadpool::ThreadPool;

// ensure that there is exactly one arg and parse it as a date.
fn parse_timestamp_arg() -> (String, i64){
    // check args
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        panic!("Expected exactly one argument, got {}", args.len()-1);
    }

    let dir = format!("./output/{}/pfx2as_{}.bz2", &args[1][..7], &args[1]);
    // parse date and get midnight ts
    let ts = DateTime::parse_from_rfc3339(&format!("{}T00:00:00-00:00", &args[1]))
        .expect("Expected date format to be yyyy-mm-dd").timestamp();
    (dir, ts)
}

/// obtains list of available route collector RIBs within the hour before and after the timestamp,
/// sorted descending by rib size.
///
/// Please note: In the early days, snapshots were named after when they were uploaded rather than
/// taken. Hence, there are files that are up to an hour late or sometimes to early, e.g., the
/// Routeviews6 RIB uploaded at 2008-07-07T00:37:00.
fn bgpkit_get_ribs_size_ordered(ts: i64) -> Vec<BrokerItem> {
    let broker = BgpkitBroker::new()
        .ts_start(&(ts-3600).to_string())
        .ts_end(&(ts+3600).to_string())
        .data_type("rib");

    broker
        .into_iter()
        .sorted_by_key(|item| -item.rough_size)
        .collect()
}

fn run_consumer(ch_in: Receiver<(u32, u32, String, String)>, fn_out: String){
    let mut map_asn: HashMap<(String, u32), HashSet<u32>> = HashMap::new();
    let mut map_feed: HashMap<(String, u32), HashSet<(u32, String)>> = HashMap::new();

    // get all the triplets from the workers
    loop {
        if let Ok((origin, peer_asn, peer_ip, pfx)) = ch_in.recv() {
            map_asn.entry((pfx.clone(), origin)).or_default().insert(peer_asn);
            map_feed.entry((pfx, origin)).or_default().insert((peer_asn, peer_ip));
        } else {
            break;
        }
    }

    let mut writer = oneio::get_writer(&fn_out).unwrap();
    writer.write_all("#pfx,origin,peer_asns,peer_feeds\n".as_ref()).unwrap();
    // consolidate data

    for ((pfx, origin), peers) in map_asn {
        let peer_cnt = peers.len();
        let feed_cnt = map_feed.entry((pfx.clone(), origin)).or_default().len();
        writer.write_all(format!("{},{},{},{}\n", pfx, origin, peer_cnt, feed_cnt).as_ref()).unwrap();
    }

    println!("Finished writing file {}.", &fn_out);

}
fn main() {
    // configuration
    let num_workers = 20;
    let (fn_out, rib_ts) = parse_timestamp_arg();

    if Path::new(&fn_out).exists(){
        println!("File {} already exists. Skipping.", &fn_out);
        exit(0)
    }

    // worker pool and channels for connecting
    let pool = ThreadPool::new(num_workers);
    let (ch_out, ch_in) = unbounded();


    println!("Going through ribs!");
    // get size-ordered broker items at rib ts
    for target in bgpkit_get_ribs_size_ordered(rib_ts){
        println!("Went through file file {}.", &target);
        // ensure needed structures are cloned and ready to move into closure
        let ch_out_cl = ch_out.clone();

        // enqueue the spawn of a new thread
        pool.execute(move || {

            // closure that processes the data of a single route collector.
            let parser = BgpkitParser::new(target.url.as_str()).unwrap();

            // iterate through elements
            for elem in parser.into_elem_iter() {

                // ignore empty and moas paths
                let origin: u32 = match elem.origin_asns {
                    None => continue,
                    Some(asns) if asns.len() != 1 => continue,
                    Some(asns) => asns[0].asn
                };

                ch_out_cl
                    .send((origin, elem.peer_asn.asn, elem.peer_ip.to_string(), elem.prefix.prefix.to_string()))
                    .unwrap();
            }
        });
    }


    // we delivered clones to all threads, so we still have to drop the initial reference.
    drop(ch_out);

    // collects results & produces summary.
    run_consumer(ch_in, fn_out);
}
