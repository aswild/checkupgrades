use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::Context;
use chrono::{DateTime, NaiveDateTime};
use reqwest::{header, Client, StatusCode};
use serif::macros::*;

use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::task::JoinSet;

use crate::Repo;

const SYNC_DBS: &[(&str, &str)] = &[
    ("core", "https://arch.mirror.constant.com/core/os/x86_64/core.db"),
    ("extra", "https://arch.mirror.constant.com/extra/os/x86_64/extra.db"),
    //("core", "https://mirror.wdc1.us.leaseweb.net/archlinux/core/os/x86_64/core.db"),
    //("extra", "https://mirror.wdc1.us.leaseweb.net/archlinux/extra/os/x86_64/extra.db"),
    ("awild", "https://awild.cc/pub/archlinux/repo/awild.db"),
    //("core", "https://awild.cc/arch/core.db"),
    //("extra", "https://awild.cc/arch/extra.db"),
    //("awild", "https://awild.cc/arch/awild.db"),
];

fn format_http_timestamp(stime: SystemTime) -> anyhow::Result<String> {
    let dur = stime.duration_since(SystemTime::UNIX_EPOCH)?;
    // format is "Wed, 21 Oct 2015 07:28:00 GMT"
    Ok(DateTime::from_timestamp(dur.as_secs().try_into().unwrap(), dur.subsec_nanos())
        .unwrap()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string())
}

fn parse_http_timestamp(s: &str) -> anyhow::Result<SystemTime> {
    let dt = NaiveDateTime::parse_from_str(s, "%a, %d %b %Y %H:%M:%S GMT")?.and_utc();
    let dur = Duration::new(dt.timestamp().try_into().unwrap(), dt.timestamp_subsec_nanos());
    SystemTime::UNIX_EPOCH
        .checked_add(dur)
        .ok_or_else(|| anyhow::anyhow!("unrepresentable timestamp {dt:?} / {dur:?}"))
}

/// Download a single file and save it to `file_path`.
///
/// The mtime of `file_path` is checked and we may not actually download if it's up to date.
async fn download_to_disk(
    client: Client,
    url: &str,
    file_path: PathBuf,
) -> anyhow::Result<Vec<u8>> {
    let path_disp = file_path.display();
    let maybe_metadata = fs::metadata(&file_path).await;
    let mtime = if let Ok(meta) = &maybe_metadata {
        anyhow::ensure!(meta.is_file(), "{path_disp} exists but is not a file");
        meta.modified().map_err(anyhow::Error::from).and_then(format_http_timestamp).ok()
    } else {
        None
    };

    let mut req = client.get(url);
    if let Some(mtime_str) = mtime {
        debug!("request if-modified-since {mtime_str:?} for {url}");
        req = req.header(header::IF_MODIFIED_SINCE, mtime_str);
    }

    let mut resp = req
        .send()
        .await
        .with_context(|| format!("Failed to send request for URL {url}"))?
        .error_for_status()
        .with_context(|| format!("Request for {url} returned an error"))?;

    let mut data = Vec::new();
    if resp.status() == StatusCode::NOT_MODIFIED {
        info!("Cached: {url}");
        data = Vec::with_capacity(
            maybe_metadata
                .as_ref()
                .map(|meta| meta.len().try_into().unwrap())
                .unwrap_or(128 * 1024),
        );
        File::open(&file_path)
            .await
            .with_context(|| format!("failed to open {path_disp} for reading"))?
            .read_to_end(&mut data)
            .await
            .with_context(|| format!("failed to read {path_disp}"))?;
    } else {
        info!("Downloading {url}");
        let mut data =
            Vec::with_capacity(resp.content_length().map(|n| n as usize).unwrap_or(128 * 1024));
        let mut file = BufWriter::new(
            File::create(&file_path)
                .await
                .with_context(|| format!("failed to open {path_disp} for writing"))?,
        );

        while let Some(chunk) =
            resp.chunk().await.with_context(|| format!("failed to read data for {}", url))?
        {
            data.extend_from_slice(&chunk);
            file.write_all(&chunk)
                .await
                .with_context(|| format!("failed writing to {path_disp}"))?;
        }

        // not automatically flushed on drop!
        file.flush().await?;

        // update mtime
        let maybe_mtime = resp
            .headers()
            .get(header::LAST_MODIFIED)
            .and_then(|val| val.to_str().ok())
            .and_then(|val| parse_http_timestamp(val).ok());
        if let Some(mtime) = maybe_mtime {
            // tokio::fs::File is missing the set_modified method, so we have to destructure the
            // tokio File to get the std file and use that directly.
            debug!("setting mtime of {path_disp} to {mtime:?}");
            let std_file = file.into_inner().into_std().await;

            // Spawn this as a blocking task, then await it. This is how most of the tokio File
            // methods are implemented.
            tokio::task::spawn_blocking(move || {
                let path_disp = file_path.display();
                if let Err(err) = std_file.set_modified(mtime) {
                    warn!("failed to set mtime of {path_disp} to {mtime:?}: {err}");
                }
                if let Err(err) = std_file.sync_all() {
                    warn!("sync failed for {path_disp}: {err}");
                }
            })
            .await
            .expect("panic'd while setting mtime");
        }
    }
    Ok(data)
}

/// Download the pacman sync databases and save them in `sync_dir`
pub async fn download_dbs(sync_dir: &Path) -> anyhow::Result<Vec<(Repo, Vec<u8>)>> {
    fs::metadata(sync_dir).await.map_err(anyhow::Error::from).and_then(|meta| {
        if meta.is_dir() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "sync db path '{}' exists but is not a directory",
                sync_dir.display()
            ))
        }
    })?;

    let client = Client::new();
    let mut tasks = JoinSet::new();
    for (repo, url) in SYNC_DBS.iter().copied() {
        let file_path = sync_dir.join(format!("{repo}.db"));
        let client = client.clone();
        tasks.spawn(async move { (repo, download_to_disk(client, url, file_path).await) });
    }

    let mut dbs = Vec::with_capacity(SYNC_DBS.len());
    while let Some(join_res) = tasks.join_next().await {
        let (repo, data_res) = join_res.expect("download thread panic'd");
        let data = data_res.with_context(|| format!("failed downloading repo {repo}"))?;
        dbs.push((Repo::from(repo), data));
    }

    Ok(dbs)
}
