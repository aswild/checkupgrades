use std::env;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::sync::OnceLock;

use ahash::HashSet;
use anstream::{AutoStream, ColorChoice};
use anyhow::{anyhow, Context, Result};
use bstr::ByteSlice;
use clap::{Arg, ArgAction};
use owo_colors::{AnsiColors, OwoColorize};

mod alpm;

#[macro_export]
macro_rules! regex {
    ($re:literal $(,)?) => {{
        static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        RE.get_or_init(|| regex::Regex::new($re).unwrap())
    }};
}

#[derive(Debug)]
struct Upgrade {
    repo: Option<Repo>,
    pkgname: String,
    oldver: String,
    newver: String,
    download_size: u64,
    install_size: u64,
    old_size: u64,
}

impl FromStr for Upgrade {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let re = regex!(r"^(\S+) (\S+) -> (\S+)$");
        let caps = re.captures(s).ok_or(())?;
        Ok(Self {
            repo: None,
            pkgname: caps.get(1).unwrap().as_str().into(),
            oldver: caps.get(2).unwrap().as_str().into(),
            newver: caps.get(3).unwrap().as_str().into(),
            download_size: 0,
            install_size: 0,
            old_size: 0,
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
        let common = old.bytes().zip(new.bytes()).take_while(|(ob, nb)| ob == nb).count();

        // walk backwards to the first non-alphanumeric character
        let extra = old[..common].bytes().rev().take_while(|c| c.is_ascii_alphanumeric()).count();
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

    fn get_color(&self) -> AnsiColors {
        match self {
            Self::Core => AnsiColors::Magenta,
            Self::Extra => AnsiColors::Blue,
            Self::Community => AnsiColors::Red,
            Self::Multilib => AnsiColors::Green,
            Self::Custom(_) => AnsiColors::Cyan,
            Self::Unknown => AnsiColors::White,
        }
    }
}

fn checkupdates_db_path() -> &'static Path {
    static CELL: OnceLock<PathBuf> = OnceLock::new();
    CELL.get_or_init(|| {
        env::var_os("CHECKUPDATES_DB").map(PathBuf::from).unwrap_or_else(|| {
            let uid = rustix::process::getuid().as_raw();
            let mut path =
                env::var_os("TMPDIR").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("/tmp"));
            path.push(format!("checkup-db-{uid}"));
            path
        })
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
        std::fs::create_dir_all(checkupdates_db).with_context(|| {
            format!("Failed to create checkupdates DB directory '{}'", checkupdates_db.display())
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
        .arg(checkupdates_db)
        .args(["--logfile", "/dev/null"]);
    let sync_output = sync_cmd.output().context("failed to execute (fakeroot) pacman -Sy")?;

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
    update_cmd.args(["-Qu", "--dbpath"]).arg(checkupdates_db).args(["--logfile", "/dev/null"]);
    let update_output = update_cmd.output().context("failed to execute pacman -Qu")?;

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

/// Load sync databases to determine download size and installed size for each package
fn add_extra_info(upgrades: &mut [Upgrade]) -> Result<()> {
    let upgrade_pkgs: HashSet<&str> = upgrades.iter().map(|u| &*u.pkgname).collect();
    let syncdb = alpm::SyncPkg::load_sync_dbs(checkupdates_db_path(), |pkgname| {
        upgrade_pkgs.contains(pkgname)
    })?;
    let local_sizes = alpm::local_package_sizes(checkupdates_db_path(), |pkgname| {
        upgrade_pkgs.contains(pkgname)
    })?;

    for upgrade in upgrades.iter_mut() {
        if let Some(pkg) = syncdb.get(&upgrade.pkgname) {
            upgrade.download_size = pkg.download_size;
            upgrade.install_size = pkg.install_size;
            upgrade.repo = Some(pkg.repo.clone());
            match local_sizes.get(&upgrade.pkgname) {
                Some(size) => upgrade.old_size = *size,
                None => eprintln!("Warning: couldn't get local size for {}", upgrade.pkgname),
            }
        } else {
            eprintln!("Warning: package {} not foundin sync DBs", upgrade.pkgname);
        }
    }
    Ok(())
}

fn run(args: Args) -> Result<()> {
    let mut upgrades = match args.input {
        Input::None => get_all_upgrades()?,
        Input::Stdin => io::read_to_string(io::stdin().lock())
            .context("failed to read stdin")?
            .lines()
            .filter_map(|line| line.parse().ok())
            .collect(),
        Input::File(path) => fs::read_to_string(&path)
            .with_context(|| format!("failed to read input file {}", path.display()))?
            .lines()
            .filter_map(|line| line.parse().ok())
            .collect(),
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

    let mut out = AutoStream::new(io::stdout().lock(), args.color_choice);

    for u in upgrades.iter() {
        match &u.repo {
            Some(repo) => write!(
                out,
                "{repo}/{pkgname}{space:width$}",
                repo = repo.color(repo.get_color()),
                pkgname = u.pkgname,
                space = "",
                width = repo_name_width - (u.pkgname.len() + repo.as_str().len() + 1),
            )?,
            None => write!(out, "{:repo_name_width$}", u.pkgname)?,
        }

        let clen = u.common_length();
        writeln!(
            out,
            "  {ocommon}{ounique}{space:width$} -> {ncommon}{nunique}",
            ocommon = &u.oldver[..clen],
            ounique = (&u.oldver[clen..]).red(),
            space = "",
            width = oldver_width - u.oldver.len(),
            ncommon = &u.newver[..clen],
            nunique = (&u.newver[clen..]).green(),
        )?;
    }

    let (total_dl, total_inst, net_upsize) = {
        let (dl, inst, old) = upgrades.iter().fold((0, 0, 0), |(dl, inst, old), u| {
            (dl + u.download_size, inst + u.install_size, old + u.old_size)
        });
        let net = (inst as i64) - (old as i64);
        (dl as f32 / 1048576.0, inst as f32 / 1048576.0, net as f32 / 1048576.0)
    };

    writeln!(out)?;
    writeln!(out, "Packages to upgrade:  {:5}", upgrades.len())?;
    writeln!(out, "Total download size:  {total_dl:8.2} MiB")?;
    writeln!(out, "Total installed size: {total_inst:8.2} MiB")?;
    writeln!(out, "Net upgrade size:     {net_upsize:8.2} MiB")?;

    Ok(())
}

static HELP_TEXT: &str = "\
Check for available pacman package updates.

checkupgrades lists available pacman package updates without needing root and without touching the
main pacman sync databases. The output is colorized formatted to look nice based on paru's layout.

By default, checkupgrades implements the same logic as checkupdates (from the pacman-contrib
package) to fetch a copy of the sync databases and list available updates for installed packages.
You may instead use a file containing the same output format as `pacman -Qu` as the input, though
this is mainly for testing.";

enum Input {
    None,
    Stdin,
    File(PathBuf),
}

struct Args {
    color_choice: ColorChoice,
    input: Input,
}

impl Args {
    fn parse() -> Self {
        let mut args = clap::command!()
            .about(HELP_TEXT.lines().next().unwrap())
            .long_about(HELP_TEXT)
            .arg(
                Arg::new("no-color")
                    .long("no-color")
                    .action(ArgAction::SetTrue)
                    .help("Disable colored output"),
            )
            .arg(
                Arg::new("upgrades-file")
                    .required(false)
                    .value_parser(clap::value_parser!(PathBuf))
                    .value_name("FILE")
                    .help(
                        "Read list of upgrades from FILE (or stdin when FILE is '-') \
                         instead of `pacman -Qu`",
                    ),
            )
            .get_matches();

        Self {
            color_choice: if args.get_flag("no-color") || anstyle_query::no_color() {
                ColorChoice::Never
            } else {
                ColorChoice::Always
            },

            input: args.remove_one::<PathBuf>("upgrades-file").map_or(Input::None, |path| {
                if path.to_str() == Some("-") {
                    Input::Stdin
                } else {
                    Input::File(path)
                }
            }),
        }
    }
}

fn main() {
    if let Err(err) = run(Args::parse()) {
        if let Some(ioerr) = err.downcast_ref::<io::Error>() {
            if ioerr.kind() == io::ErrorKind::BrokenPipe {
                return;
            }
        }
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
