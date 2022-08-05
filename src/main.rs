use std::collections::HashMap;
use std::env;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::{self, Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use bstr::ByteSlice;
use clap::Parser;
use once_cell::sync::Lazy;
use regex::Regex;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

#[derive(Debug)]
struct Upgrade {
    repo: Option<Repo>,
    pkgname: String,
    oldver: String,
    newver: String,
}

impl FromStr for Upgrade {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^(\S+) (\S+) -> (\S+)$").unwrap());

        let caps = RE.captures(s).ok_or(())?;
        Ok(Self {
            repo: None,
            pkgname: caps.get(1).unwrap().as_str().into(),
            oldver: caps.get(2).unwrap().as_str().into(),
            newver: caps.get(3).unwrap().as_str().into(),
        })
    }
}

impl Upgrade {
    fn common_length(&self) -> usize {
        let old = &self.oldver;
        let new = &self.newver;
        // not ready to handle multibyte unicode characters
        assert!(old.is_ascii());
        assert!(new.is_ascii());

        // get the length of the common prefix
        let common = old
            .bytes()
            .zip(new.bytes())
            .take_while(|(ob, nb)| ob == nb)
            .count();

        // walk backwards to the first non-alphanumeric character
        let extra = old[..common]
            .bytes()
            .rev()
            .take_while(|c| c.is_ascii_alphanumeric())
            .count();
        assert!(extra <= common);
        common - extra
    }
}

/// Represents a pacman repo. Either one of the standard ones, or a custom named repo.
///
/// The automatically derived (Partial)Ord implementation does what we want - sorts first by the
/// enum discriminant value (order variants are defined) and lexographically if both are
/// Repo::Custom variants.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Repo {
    Core,
    Extra,
    Community,
    Multilib,
    Custom(String),
}

impl FromStr for Repo {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "core" => Self::Core,
            "extra" => Self::Extra,
            "community" => Self::Community,
            "multilib" => Self::Multilib,
            _ => Self::Custom(s.to_owned()),
        })
    }
}

impl fmt::Display for Repo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(self.as_str())
    }
}

impl Repo {
    fn as_str(&self) -> &str {
        match self {
            Self::Core => "core",
            Self::Extra => "extra",
            Self::Community => "community",
            Self::Multilib => "multilib",
            Self::Custom(repo) => repo.as_str(),
        }
    }

    fn color_spec(&self) -> ColorSpec {
        let color = match self {
            Self::Core => Color::Magenta,
            Self::Extra => Color::Blue,
            Self::Community => Color::Red,
            Self::Multilib => Color::Green,
            Self::Custom(_) => Color::Cyan,
        };
        let mut spec = ColorSpec::new();
        spec.set_fg(Some(color));
        spec
    }
}

/// This is nominally a reimplementation of /usr/bin/checkupdates, but with nicer error handling
fn get_all_upgrades() -> Result<Vec<Upgrade>> {
    // figure out the main pacman DB path. Normally this should just be "/var/lib/pacman/" but
    // check pacman-conf in case it's set to something different somehow
    let dbpath = match Command::new("pacman-conf").arg("DBPath").output() {
        Ok(output) if output.status.success() => output.stdout.trim().to_owned(),
        _ => Vec::new(),
    };
    let dbpath = if dbpath.is_empty() || !Path::new(OsStr::from_bytes(&dbpath)).is_dir() {
        PathBuf::from("/var/lib/pacman/")
    } else {
        PathBuf::from(OsString::from_vec(dbpath))
    };

    // get the checkup db path
    let checkupdates_db = env::var_os("CHECKUPDATES_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            // safety: getuid can never fail, it's only unsafe because FFI
            let uid = unsafe { libc::getuid() };
            let mut path = env::var_os("TMPDIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/tmp"));
            path.push(format!("checkup-db-{uid}"));
            path
        });

    // create and set up the checkup db directory
    if !checkupdates_db.is_dir() {
        std::fs::create_dir_all(&checkupdates_db).with_context(|| {
            format!(
                "Failed to create checkupdates DB directory '{}'",
                checkupdates_db.display()
            )
        })?;
    }

    match std::os::unix::fs::symlink(dbpath.join("local"), checkupdates_db.join("local")) {
        Ok(()) => (),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => (),
        Err(err) => {
            return Err(err).context(format!(
                "failed to create symlink {}/local -> {}/local",
                dbpath.display(),
                checkupdates_db.display()
            ));
        }
    }

    // Call pacman to sync the checkupdates DB. This needs to be done with fakeroot or pacman will
    // immediately error out
    let mut sync_cmd = Command::new("fakeroot");
    sync_cmd
        .args(["--", "pacman", "-Sy", "--dbpath"])
        .arg(&checkupdates_db)
        .args(["--logfile", "/dev/null"]);
    let sync_output = sync_cmd
        .output()
        .context("failed to execute (fakeroot) pacman -Sy")?;

    if !sync_output.status.success() {
        eprintln!("Failed to fetch updates!");
        eprintln!("Command: {sync_cmd:?}");
        eprintln!("Standard Output:");
        let _ = io::stderr().write_all(&sync_output.stdout);
        eprintln!("Standard Error:");
        let _ = io::stderr().write_all(&sync_output.stderr);
        return Err(anyhow!("cannot fetch updates"));
    }

    // Call pacman to list available updates. This doesn't need fakeroot
    let mut update_cmd = Command::new("pacman");
    update_cmd
        .args(["-Qu", "--dbpath"])
        .arg(&checkupdates_db)
        .args(["--logfile", "/dev/null"]);
    let update_output = update_cmd
        .output()
        .context("failed to execute pacman -Qu")?;

    // if no updates are available, pacman exits 1 with no output. Therefore the error case is only
    // when we the status is nonzero and we get something on stdout or stderr.
    if !update_output.status.success()
        && (!update_output.stdout.is_empty() || !update_output.stderr.is_empty())
    {
        eprintln!("Failed to check updates!");
        eprintln!("Command: {update_cmd:?}");
        eprintln!("Standard Output:");
        let _ = io::stderr().write_all(&update_output.stdout);
        eprintln!("Standard Error:");
        let _ = io::stderr().write_all(&update_output.stderr);
        return Err(anyhow!("cannot check updates"));
    }

    // finally, parse the output into a vec of upgrades
    Ok(std::str::from_utf8(&update_output.stdout)
        .context("pacman -Qu output is not UTF-8")?
        .lines()
        .filter_map(|line| Upgrade::from_str(line).ok())
        .collect())
}

/// Given a list of `Upgrade`s, query pacman and set the `repo` field appropriately for each
/// `Upgrade` if possible.
fn add_repos(upgrades: &mut [Upgrade]) -> Result<()> {
    // The easiest way to figure out which repo a package comes from is with `pacman -Sl`. We end
    // up collecting *all* of the packages doing this, but something more granular would involve
    // parsing a bunch of `pacman -Si` output or diving into libalpm. This simple method should be
    // good enough.
    let output = Command::new("pacman")
        .arg("-Sl")
        .output()
        .context("failed to run `pacman -Sl`")?;
    let stdout = String::from_utf8(output.stdout).context("`pacman -Sl` output was not UTF-8")?;

    // The output we're parsing looks like this, we want the first two words
    //     % pacman -Sl | head -n3
    //     core acl 2.3.1-2 [installed]
    //     core amd-ucode 20220708.be7798e-1
    //     core archlinux-keyring 20220713-2 [installed]
    // repomap is a mapping of pkgname -> reponame, all borrowed from `stdout`
    let repomap: HashMap<&str, &str> = stdout
        .lines()
        .filter_map(|line| {
            let mut s = line.split(' ');
            match (s.next(), s.next()) {
                (Some(repo), Some(pkgname)) => Some((pkgname, repo)),
                _ => None,
            }
        })
        .collect();

    for upgrade in upgrades.iter_mut() {
        upgrade.repo = repomap
            .get(upgrade.pkgname.as_str())
            .map(|s| s.parse().unwrap());
    }

    Ok(())
}

fn run() -> Result<()> {
    let mut upgrades = if atty::is(atty::Stream::Stdin) {
        // running from a terminal, do normal pacman things to get updates
        get_all_upgrades()?
    } else {
        // stdin is redirected, assume that we're piping in the output of /usr/bin/checkupdates
        let mut buf = String::new();
        io::stdin()
            .read_to_string(&mut buf)
            .context("failed to read stdin")?;
        buf.lines().filter_map(|line| line.parse().ok()).collect()
    };
    if let Err(err) = add_repos(&mut upgrades) {
        eprintln!("Warning: failed to map packages to repos: {err:#}");
    }

    // sort by repo, then by pkgname
    upgrades.sort_unstable_by(|a, b| match a.repo.cmp(&b.repo) {
        std::cmp::Ordering::Equal => a.pkgname.cmp(&b.pkgname),
        greater_or_less => greater_or_less,
    });

    let repo_width = upgrades
        .iter()
        .filter_map(|u| u.repo.as_ref().map(|r| r.as_str().len()))
        .max()
        .unwrap_or(0);
    let name_width = upgrades.iter().map(|u| u.pkgname.len()).max().unwrap_or(0);
    let repo_name_width = repo_width + name_width + 1;
    let oldver_width = upgrades.iter().map(|u| u.oldver.len()).max().unwrap_or(0);

    let mut out = StandardStream::stdout(ColorChoice::Always);
    let red = ColorSpec::new().set_fg(Some(Color::Red)).clone();
    let green = ColorSpec::new().set_fg(Some(Color::Green)).clone();

    for u in upgrades.iter() {
        match u.repo {
            Some(ref repo) => {
                out.set_color(&repo.color_spec())?;
                write!(out, "{repo}")?;
                out.reset()?;
                write!(
                    out,
                    "/{}{:width$}",
                    u.pkgname,
                    "",
                    width = repo_name_width - u.pkgname.len() - repo.as_str().len() - 1
                )?;
            }
            None => write!(out, "{:repo_name_width$} ", u.pkgname)?,
        }

        let clen = u.common_length();
        write!(out, "{}", &u.oldver[..clen])?;
        out.set_color(&red)?;
        write!(out, "{}", &u.oldver[clen..])?;
        out.reset()?;
        if u.oldver.len() < oldver_width {
            write!(out, "{:1$}", " ", oldver_width - u.oldver.len())?;
        }
        write!(out, " -> {}", &u.newver[..clen])?;
        out.set_color(&green)?;
        write!(out, "{}", &u.newver[clen..])?;
        out.reset()?;
        writeln!(out)?;
    }

    Ok(())
}

/// Check for available pacman package updates.
///
/// checkupgrades lists available pacman package updates without needing to be
/// and without actually touching the main pacman sync databases. The output is
/// colorized formatted to look nice based on paru's layout.
///
/// Usage:
///     checkupgrades [-h|--help]
///     /usr/bin/checkupdates | checkupgrades
///
/// By default, checkupgrades implements the same logic as checkupdates (from the
/// pacman-contrib package) to fetch a copy of the sync databases and list available
/// updates for installed packages.
///
/// Alternatively, if stdin is piped, it's assumed to be the output of the
/// checkupdates script from pacman-contrib, and checkupgrades will not invoke any
/// extra pacman logic besides associating package names with sync db names.
#[derive(Debug, Parser)]
#[clap(version, verbatim_doc_comment)]
struct Args {
    // no args yet, for now just use clap for help/version
}

fn main() {
    let _args = Args::parse();

    if let Err(err) = run() {
        if let Some(ioerr) = err.downcast_ref::<io::Error>() {
            if ioerr.kind() == io::ErrorKind::BrokenPipe {
                return;
            }
        }
        eprintln!("Error: {err:?}");
        std::process::exit(libc::EXIT_FAILURE);
    }
}
