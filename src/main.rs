use std::collections::HashMap;
use std::env;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::{self, Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::sync::OnceLock;

use anyhow::{anyhow, Context, Result};
use bstr::ByteSlice;
use is_terminal::IsTerminal;
use regex::Regex;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

#[derive(Debug)]
struct Upgrade {
    repo: Option<Repo>,
    pkgname: String,
    oldver: String,
    newver: String,
    // sizes are in bytes (despite being floats)
    download_size: f32,
    install_size: f32,
}

impl FromStr for Upgrade {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let re = {
            static CELL: OnceLock<Regex> = OnceLock::new();
            CELL.get_or_init(|| Regex::new(r"^(\S+) (\S+) -> (\S+)$").unwrap())
        };

        let caps = re.captures(s).ok_or(())?;
        Ok(Self {
            repo: None,
            pkgname: caps.get(1).unwrap().as_str().into(),
            oldver: caps.get(2).unwrap().as_str().into(),
            newver: caps.get(3).unwrap().as_str().into(),
            download_size: 0.,
            install_size: 0.,
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
    Unknown,
}

impl Repo {
    fn from_str_common(s: &str) -> Option<Self> {
        match s {
            "core" => Some(Self::Core),
            "extra" => Some(Self::Extra),
            "community" => Some(Self::Community),
            "multilib" => Some(Self::Multilib),
            "" => Some(Self::Unknown),
            _ => None,
        }
    }
}

impl From<String> for Repo {
    fn from(s: String) -> Self {
        match Self::from_str_common(&s) {
            Some(repo) => repo,
            None => Self::Custom(s),
        }
    }
}

impl From<&str> for Repo {
    fn from(s: &str) -> Self {
        match Self::from_str_common(s) {
            Some(repo) => repo,
            None => Self::Custom(s.to_owned()),
        }
    }
}

impl fmt::Display for Repo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(self.as_str())
    }
}

impl Default for Repo {
    fn default() -> Self {
        Self::Unknown
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
            Self::Unknown => "[unknown]",
        }
    }

    fn color_spec(&self) -> ColorSpec {
        let color = match self {
            Self::Core => Color::Magenta,
            Self::Extra => Color::Blue,
            Self::Community => Color::Red,
            Self::Multilib => Color::Green,
            Self::Custom(_) => Color::Cyan,
            Self::Unknown => Color::White,
        };
        let mut spec = ColorSpec::new();
        spec.set_fg(Some(color));
        spec
    }
}

fn checkupdates_db_path() -> PathBuf {
    env::var_os("CHECKUPDATES_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            // safety: getuid can never fail, it's only unsafe because FFI
            let uid = unsafe { libc::getuid() };
            let mut path = env::var_os("TMPDIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/tmp"));
            path.push(format!("checkup-db-{uid}"));
            path
        })
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
    let checkupdates_db = checkupdates_db_path();

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

/// Parse `pacman -Si` to get the repository, download size, and installed size for each package
fn add_extra_info(upgrades: &mut [Upgrade]) -> Result<()> {
    /// Additional information we get from `pacman -Si` that we can't get from `pacman -Qu`
    #[derive(Clone, Default)]
    struct PkgInfo {
        repo: Repo,
        download_size: f32,
        install_size: f32,
    }

    /// Parse a string like "35.33 KiB" into an f32 number of bytes.
    ///
    /// Supported suffixes are B, KiB, MiB, and GiB.  There must be exactly one space between the
    /// number and the suffix. Returns None if the format doesn't match.
    fn parse_size(s: &str) -> Option<f32> {
        let (num, unit) = s.split_once(' ')?;
        let num = num.parse::<f32>().ok()?;
        let multiplier = match unit {
            "B" => 1.0,
            "KiB" => 1024.0,
            "MiB" => 1_048_576.0,
            "GiB" => 1_073_741_824.0,
            _ => return None,
        };
        Some(num * multiplier)
    }

    let output = Command::new("pacman")
        .arg("-Si")
        .arg("--dbpath")
        .arg(checkupdates_db_path())
        .output()
        .context("failed to run `pacman -Si`")?;
    let stdout = String::from_utf8(output.stdout).context("`pacman -Si` output was not UTF-8")?;

    let mut map = HashMap::new();
    let mut pkg = PkgInfo::default();
    let mut name = None::<&str>;

    for line in stdout.lines() {
        if line.is_empty() {
            match name.take() {
                Some(pkgname) => {
                    map.insert(pkgname, std::mem::take(&mut pkg));
                }
                None => {
                    eprintln!("warning: encountered blank `pacman -Si` line with no pkgname found");
                    pkg = PkgInfo::default();
                }
            }
            continue;
        }

        // skip lines that start with whitespace
        if line.chars().next().unwrap().is_whitespace() {
            continue;
        }

        // split on the first ':' and strip whitespace from key and value
        let (key, val) = match line.split_once(':') {
            Some((a, b)) => (a.trim(), b.trim()),
            None => continue,
        };

        match key {
            "Repository" => pkg.repo = val.into(),
            "Name" => name = Some(val),
            "Download Size" => pkg.download_size = parse_size(val).unwrap_or_default(),
            "Installed Size" => pkg.install_size = parse_size(val).unwrap_or_default(),
            _ => (),
        }
    }

    for upgrade in upgrades.iter_mut() {
        if let Some(info) = map.get(upgrade.pkgname.as_str()) {
            upgrade.repo = Some(info.repo.clone());
            upgrade.download_size = info.download_size;
            upgrade.install_size = info.install_size;
        }
    }

    Ok(())
}

fn run() -> Result<()> {
    let mut upgrades = if io::stdin().is_terminal() {
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

    if let Err(err) = add_extra_info(&mut upgrades) {
        eprintln!("Warning: failed to map packages to repos: {err:#}");
    }

    // sort by repo, then by pkgname
    upgrades.sort_unstable_by(|a, b| match a.repo.cmp(&b.repo) {
        std::cmp::Ordering::Equal => a.pkgname.cmp(&b.pkgname),
        greater_or_less => greater_or_less,
    });

    // the max length of "repo/pkgname" for all upgrades
    let repo_name_width = upgrades
        .iter()
        .map(|u| {
            let repo_width = match &u.repo {
                // add 1 for the '/' after the repo name
                Some(repo) => repo.as_str().len() + 1,
                None => 0,
            };
            repo_width + u.pkgname.len()
        })
        .max()
        .unwrap_or(0);

    let oldver_width = upgrades.iter().map(|u| u.oldver.len()).max().unwrap_or(0);

    let out = StandardStream::stdout(ColorChoice::Always);
    let mut out = out.lock();
    let red = ColorSpec::new().set_fg(Some(Color::Red)).clone();
    let green = ColorSpec::new().set_fg(Some(Color::Green)).clone();

    for u in upgrades.iter() {
        // [repo/]pkgname
        match &u.repo {
            Some(repo) => {
                out.set_color(&repo.color_spec())?;
                write!(out, "{repo}")?;
                out.reset()?;
                write!(
                    out,
                    "/{pkgname}{space:width$}",
                    pkgname = u.pkgname,
                    space = "",
                    width = repo_name_width - (u.pkgname.len() + repo.as_str().len() + 1),
                )?;
            }
            None => write!(out, "{:repo_name_width$}", u.pkgname)?,
        }

        // two spaces between pkgname and old version
        write!(out, "  ")?;

        // old version
        let clen = u.common_length();
        write!(out, "{}", &u.oldver[..clen])?;
        out.set_color(&red)?;
        write!(out, "{}", &u.oldver[clen..])?;
        out.reset()?;

        // padding and arrow between old and new version
        write!(
            out,
            "{space:width$} -> ",
            space = "",
            width = oldver_width - u.oldver.len()
        )?;

        // new version
        write!(out, "{}", &u.newver[..clen])?;
        out.set_color(&green)?;
        write!(out, "{}", &u.newver[clen..])?;
        out.reset()?;

        // finally, end the line
        writeln!(out)?;
    }

    let (total_dl, total_inst) = {
        let (dl, inst) = upgrades.iter().fold((0f32, 0f32), |(dl, inst), u| {
            (dl + u.download_size, inst + u.install_size)
        });
        (dl / 1048576.0, inst / 1048576.0)
    };

    writeln!(out)?;
    writeln!(out, "Packages to upgrade: {}", upgrades.len())?;
    writeln!(out, "Total download size:  {total_dl:8.2} MiB")?;
    writeln!(out, "Total installed size: {total_inst:8.2} MiB")?;

    Ok(())
}

static HELP_TEXT: &str = "\
Check for available pacman package updates.

checkupgrades lists available pacman package updates without needing to be
and without actually touching the main pacman sync databases. The output is
colorized formatted to look nice based on paru's layout.

Usage:
    checkupgrades [-h|--help]
    /usr/bin/checkupdates | checkupgrades

By default, checkupgrades implements the same logic as checkupdates (from the
pacman-contrib package) to fetch a copy of the sync databases and list available
updates for installed packages.

Alternatively, if stdin is piped, it's assumed to be the output of the
checkupdates script from pacman-contrib, and checkupgrades will not invoke any
extra pacman logic besides associating package names with sync db names.
";

fn main() {
    // we don't actually do any argument parsing (yet), instead clap is just used to implement help
    // and version flags and error out if any arguments are passed
    let _ = clap::command!()
        .about(HELP_TEXT.lines().next().unwrap())
        .long_about(HELP_TEXT)
        .get_matches();

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
