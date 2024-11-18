//! Utilities for working with the alpm/pacman database format

use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::path::Path;

use ahash::HashMap;
use anyhow::Context;

use crate::{regex, Repo};

/// Generic parser for the `desc` file format.
///
/// Takes in the full (utf-8 required) contents of a `desc` file and acts as an iterator yielding
/// `(tag, value)` tuples. The tag is whatever is found between `%` symbols on its own line, and
/// the value is everything after that until a blank line, but omitting the trailing `\n`.
///
/// The `'d` lifetime in this type corresponds to the `desc` text passed to `DescIter::new`.
///
/// This parser is very dumb: it's impossible for the file format to fail validation. All input
/// before the first line matching `^%.*%$` is ignored
#[derive(Debug)]
struct DescIter<'d> {
    /// Inner regex iterator. The 'static lifetime represents the compiled regex (which is static
    /// because we use OnceLock) and 'd is the lifetime of the haystack.
    iter: regex::CaptureMatches<'static, 'd>,
}

impl<'d> DescIter<'d> {
    pub fn new(desc: &'d str) -> Self {
        // It's so fun how we can parse this whole file format with a single regex.
        // But unfortunately, it's a fairly slow regex with a lot of backtracking which makes
        // a similar version here considerably slower than `pacman -Si`, when the whole point of
        // writing any of this code was for speed gains.
        let re = regex!(
            r"(?mx)                     # enable multiline and verbose modes
              ^%(?<tag>[^%]+)%$\n       # tag on its own line between %%
              (?s)(?<value>.*?)(?-s)$\n # value, non-greedy capture everything including \n
              ^$\n                      # empty line delimeter
            "
        );

        Self { iter: re.captures_iter(desc) }
    }
}

impl<'d> Iterator for DescIter<'d> {
    type Item = (&'d str, &'d str);

    fn next(&mut self) -> Option<Self::Item> {
        let m = self.iter.next()?;
        let tag = m.name("tag").unwrap().as_str();
        let value = m.name("value").unwrap().as_str();
        Some((tag, value))
    }
}

/// Extract the pkgname part of a `${pkgname}-${pkgver}-${pkgrel}` string.
/// Returns None if `s` doesn't contain at least two `-` characters.
fn split_pkgname(s: &str) -> Option<&str> {
    let mut split = s.rsplitn(3, '-');
    let _pkgrel = split.next()?;
    let _pkgver = split.next()?;
    split.next()
}

/// Get the installed size (%SIZE%) of a locally installed package.
pub fn local_package_sizes(
    db_path: impl AsRef<Path>,
    filter: impl Fn(&str) -> bool,
) -> anyhow::Result<HashMap<String, u64>> {
    let local_dir = db_path.as_ref().join("local");
    let dirents = local_dir
        .read_dir()
        .with_context(|| format!("failed to read directory {}", local_dir.display()))?;

    let mut desc_buf = String::new();
    let mut map = HashMap::default();
    for result in dirents {
        let entry =
            result.with_context(|| format!("failed to read dirent in {}", local_dir.display()))?;

        // skip non-directories
        if !entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
            continue;
        }

        let path = entry.path();
        // skip if no utf8 path name
        let dirname = match path.file_name().and_then(OsStr::to_str) {
            Some(s) => s,
            None => continue,
        };

        // skip if this isn't the package we're looking for
        let pkgname = match split_pkgname(dirname) {
            Some(s) => s,
            None => continue,
        };
        if !filter(pkgname) {
            continue;
        }

        // we care about this package, find its size
        let path = path.join("desc");
        desc_buf.clear();
        File::open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?
            .read_to_string(&mut desc_buf)
            .with_context(|| format!("failed to read {}", path.display()))?;

        let mut size = Some(0);
        for (tag, value) in DescIter::new(&desc_buf) {
            match tag {
                "NAME" => {
                    if value != pkgname {
                        size = None;
                    }
                }
                "SIZE" => {
                    size = Some(value.parse().with_context(|| {
                        "unable to parse package {dirname} size value {value:?}"
                    })?);
                }
                _ => (),
            }
        }

        if let Some(size) = size {
            map.insert(pkgname.to_owned(), size);
        }
    }

    Ok(map)
}

/// A package from a desc file in a sync database
#[derive(Debug)]
pub struct SyncPkg {
    pub name: String,
    //pub version: String,
    pub repo: Repo,
    pub download_size: u64,
    pub install_size: u64,
}

impl SyncPkg {
    pub fn from_desc(desc: &str) -> anyhow::Result<Self> {
        let mut name = None;
        //let mut version = None;
        let mut download_size = None;
        let mut install_size = None;

        for (tag, value) in DescIter::new(desc) {
            match tag {
                "NAME" => name = Some(value.to_owned()),
                //"VERSION" => version = Some(value.to_owned()),
                "CSIZE" => {
                    download_size = Some(value.parse().with_context(|| {
                        format!("failed to parse package csize {value:?} as an integer")
                    })?)
                }
                "ISIZE" => {
                    install_size = Some(value.parse().with_context(|| {
                        format!("failed to parse package isize {value:?} as an integer")
                    })?)
                }
                _ => (),
            }
        }

        Ok(SyncPkg {
            name: name.ok_or_else(|| anyhow::anyhow!("missing package name in desc"))?,
            //version: version.ok_or_else(|| anyhow::anyhow!("missing package version in desc"))?,
            repo: Repo::Unknown,
            download_size: download_size
                .ok_or_else(|| anyhow::anyhow!("missing package download size in desc"))?,
            install_size: install_size
                .ok_or_else(|| anyhow::anyhow!("missing package install size in desc"))?,
        })
    }

    /// Load a database (e.g. `/var/lib/pacman/sync/core.db`) and collect all packages found into
    /// the given map.
    ///
    /// pkgname is the map key. The database may be gzip or zstd compressed. Existing entries in
    /// `map` with the same pkgname will be replaced. `Err` may be returned even if some entries
    /// have been added to `map`.
    pub fn read_one_db(
        map: &mut HashMap<String, SyncPkg>,
        db_path: impl AsRef<Path>,
        filter: impl Fn(&str) -> bool,
    ) -> anyhow::Result<()> {
        let db_path = db_path.as_ref();
        let repo: Repo = db_path
            .file_stem()
            .context("db path has no filestem")?
            .to_str()
            .context("db path isn't utf-8")?
            .into();

        // read magic to determine compression type
        let mut db_file = File::open(db_path).context("failed to open file")?;
        let mut magic = [0u8; 4];
        db_file.read_exact(&mut magic).context("failed to read file header")?;
        db_file.rewind().context("failed to rewind file")?;

        // Dynamic decompressor
        let input: Box<dyn Read> = if &magic[..] == b"\x28\xb5\x2f\xfd" {
            Box::new(zstd::Decoder::new(db_file).context("failed to initialize zstd decoder")?)
        } else if &magic[..2] == b"\x1f\x8b" {
            Box::new(flate2::read::GzDecoder::new(db_file))
        } else {
            // no recognized compression magic, assume uncompressed
            Box::new(BufReader::new(db_file))
        };

        let mut tarball = tar::Archive::new(input);
        let mut desc_buf = String::new();

        for result in tarball.entries().context("failed to read tar file")? {
            let mut entry = result.context("failed to read tar entry")?;
            // skip non-files
            if !entry.header().entry_type().is_file() {
                continue;
            }

            // getting the path can't fail on linux
            let path = entry.path().unwrap();
            // skip files that aren't named "desc"
            if path.file_name().and_then(OsStr::to_str) != Some("desc") {
                continue;
            }

            // determine pkgname from the path to the desc file (inside the archive)
            let desc_pkgname = match path
                .parent()
                .and_then(Path::file_name)
                .and_then(OsStr::to_str)
                .and_then(split_pkgname)
            {
                Some(pkgname) => pkgname,
                None => continue,
            };

            // run the caller's filter, skip if it doesn't match
            if !filter(desc_pkgname) {
                continue;
            }

            desc_buf.clear();
            entry.read_to_string(&mut desc_buf).context("failed to read tar entry data")?;
            let mut pkg = SyncPkg::from_desc(&desc_buf)
                .with_context(|| format!("failed to parse {}", entry.path().unwrap().display()))?;
            pkg.repo = repo.clone();
            map.insert(pkg.name.clone(), pkg);
        }

        Ok(())
    }

    /// Read all `$db_dir/sync/*.db` files into a pkgname->SyncPkg map
    ///
    /// TODO: this function does not check or respect the order that repos are defined in
    /// `pacman.conf`. Therefore, if the same pkgname exists in multiple repos, it's unspecified
    /// which one will end up in the resulting map.
    ///
    /// Takes an optional filter which is passed the pkgname.
    pub fn load_sync_dbs(
        db_dir: impl AsRef<Path>,
        filter: impl Fn(&str) -> bool,
    ) -> anyhow::Result<HashMap<String, SyncPkg>> {
        let sync_dir = db_dir.as_ref().join("sync");
        let dirents = sync_dir
            .read_dir()
            .with_context(|| format!("failed to read directory {}", sync_dir.display()))?;

        let mut map = HashMap::default();
        for result in dirents {
            let entry = result
                .with_context(|| format!("failed to read dirent in {}", sync_dir.display()))?;
            let path = entry.path();
            if entry.file_type().map(|ft| ft.is_file()).unwrap_or(false)
                && path.extension().and_then(OsStr::to_str) == Some("db")
            {
                SyncPkg::read_one_db(&mut map, &path, &filter)
                    .with_context(|| format!("failed to load {}", path.display()))?;
            }
        }

        Ok(map)
    }
}
