# `dir-cache` - A very low-effort directory cache

A map-interface which propagates writes to disk 
in a somewhat browsable format.

Designed to be simple to use and understand, 
not particularly effective.

[![ci](https://github.com/MarcusGrass/dir-cache/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/MarcusGrass/dir-cache/actions/workflows/main.yml)
[![crates.io](https://img.shields.io/crates/v/dir-cache)](https://crates.io/crates/dir-cache)
[![docs](https://docs.rs/dir-cache/badge.svg)](https://docs.rs/dir-cache)
[![codecov](https://codecov.io/gh/MarcusGrass/dir-cache/graph/badge.svg?token=IHKY1G2ARV)](https://codecov.io/gh/MarcusGrass/dir-cache)

## Performance

Bad.

## Performance pt. 2

Okay if you write lazily to disk. 
Since (potentially depending on options) each map operation corresponds to at least multiple  
disk-operations, the map is ill-suited to high-frequency operations.  

Since a specific value will net a disk-representation that is strictly larger than the raw content size the `dir-cache` is space-inefficient.

Lastly, file-system specifics may make many keys perform poorly (I'm looking at you NTFS).

## Should be used when

You have some toy-project that's pinging an external 
API and you want to easily just cache responses to certain requests
to reduce load on your counterparts servers, or similar.  

## What you should check out for production use-cases

If you want an embedded KV-store in `Rust`, consider [Sled](https://docs.rs/sled/latest/sled/).

If you want an embedded KV-store not in `Rust`, consider [RocksDB (github)](https://github.com/facebook/rocksdb), 
[RocksDB(docs.rs)](https://docs.rs/rocksdb/latest/rocksdb/).

If you want an embedded SQL-store, not in `Rust`, consider [Sqlite(website)](https://www.sqlite.org/index.html), 
[Sync rust crate(Rusqlite)](https://crates.io/crates/rusqlite), [Async rust crate (Sqlx)](https://crates.io/crates/sqlx).

Or whatever else.

## Why

Now that the above is out of the way, we can get into why this crate exists.  

I have in my time exploring public APIs using `Rust` encountered the same problem many times:
>I am exploring an API, taking apart the response and analysing it to inform my further code, but I don't want to 
make a new web request each iteration out of both latency, and respect for my counterpart. 

What I generally have done in these cases is saving the responses to disk and done offline analysis on them. 
This works, but it's cumbersome, it's handling the same old `std::fs::...` errors, figuring out a fitting directory 
structure, and worst of all, writing two separate parts of code, fetch and analyze.

### Raison d'etre summarized

I want to write this:

```Rust
fn iterate_on_api_response_handling(dir_cache: &mut Cache) {
    // This is preferably not dynamic
    let req_key = Path::new("examplerequest");
    // If this has run before then don't send an http request
    let resp = dir_cache.get_or_insert_with(req_key, || {
        let resp = http::client::get("https://example.com")?;
        Ok(resp)
    });
    // Mess around with resp here
    println!("Got length {}", resp.len());
}
```

With the above, both the fetching and analyzing code can be kept in the same place.

## Features

The feature set is kept fairly minimal to support the above use case.

### Map like interface

There are `get`, `get_or_insert_with`, `insert`, and `remove` methods on the `DirCache` .

### Browsable disk representation

The values are written to disk at `cache-location/{key}/`, which makes it easy to check out the saved 
file, which in my cases are most-often `json`.

### Max age on responses

Since values may become stale, depending on how long the iterating takes, a max age can be set by duration, 
after which the value will be treated as non-existent. Meaning, running the same `get_or_insert_with` will 
the first time fetch data, each time up until the max age has passed, return the cached data, and after the 
max age has passed fetch new data.

### Data optionally saved as generations

Overwriting the same key can optionally shuffle the older key down one generation, leaving it on disk.  
Useful in some cases where response changes over time, and you wish to keep a history. 
Although it's definitely the least useful feature.

#### Optionally compress generational data

I found some use for this when working with an incredibly sparse `json` dataset where responses were pretty huge, 
with the feature `lz4` `lz4`-compression can be picked for old generations.


## Caveats

There is one caveat apart from performance that bears consideration.

### DISK DANGER

Keys are `PathBufs` and joined with the `dir-cache` base directory. This opens up a can of worms, 
the worst of which is accidentally joining with an abs-path, see the docs on [Path::join](https://doc.rust-lang.org/std/path/struct.Path.html#method.join).

This could potentially lead to destructive results.

There are a few mitigations:

1. Paths are never joined if the right side is absolute, and paths are not allowed to be anything but a [Component::Normal](https://doc.rust-lang.org/std/path/enum.Component.html).
as well as making sure parsed components combined length makes sense with the provided `OsStr` length (Mitigating unexpected effective paths).  
2. Write operations are only done on specific file-names `dir-cache-generation-{manifest.txt | n}`. (Reducing risk of accidental overwrites of important files).  
3. Removal operations are only done on the above specific file-names, as well as empty directories.  

This covers all the cases that I can think of, but of course, doesn't cover the cases that I fail to think of.

If using this library, a recommendation is to not use dynamic keys. 
Fuzzing is done on `Linux` only, so extra danger if using dynamic keys on other Oses, although it's not safe 
on `Linux` just because it's fuzzed.


## License

The project is licensed under [MPL-2.0](LICENSE).  
