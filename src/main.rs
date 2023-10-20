use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::{anyhow, bail, Context, Result};

use azure_storage::StorageCredentials;
use azure_storage_blobs::{
    container::operations::BlobItem,
    prelude::{ClientBuilder, ContainerClient},
};
use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use log::info;
use projfs::{start_proj_virtualization, FileBasicInfo, ProjFSDirEnum, ProjFSRead};
use url::Url;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Destination directory to project into
    path: PathBuf,

    /// Azure SAS URL
    url: Url,
}

fn builder_from_url(url: &Url) -> Result<ClientBuilder> {
    // Determine the account.
    let account = if let Some(domain) = url.domain() {
        // Split out the subdomain.
        if let Some(subdomain) = domain.split('.').next() {
            subdomain
        } else {
            bail!("could not parse domain: {domain}");
        }
    } else {
        bail!("unsupported URL: {url}");
    };

    // Determine if the URL is an SAS URL.
    let creds = if url.query_pairs().any(|(a, _)| a == "sig") {
        // This is an SAS URL.
        // FIXME: Somehow avoid that unwrapping?
        StorageCredentials::sas_token(url.query().unwrap()).context("failed to parse SAS token")?
    } else {
        todo!()
    };

    Ok(ClientBuilder::new(account, creds))
}

#[derive(Debug, Clone)]
struct BlobPath(String);

impl<P: AsRef<Path>> From<P> for BlobPath {
    fn from(value: P) -> Self {
        let c = value.as_ref().components();

        Self(
            c.into_iter()
                .filter_map(|c| match c {
                    std::path::Component::Prefix(_) => todo!(),
                    std::path::Component::RootDir => todo!(),
                    std::path::Component::CurDir => todo!(),
                    std::path::Component::ParentDir => todo!(),
                    std::path::Component::Normal(p) => Some(p.to_string_lossy()),
                })
                .collect::<Vec<_>>()
                .join("/"),
        )
    }
}

impl std::fmt::Display for BlobPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl BlobPath {
    fn new(p: impl Into<String>) -> Self {
        Self(p.into())
    }

    fn as_str(&self) -> &str {
        &self.0
    }

    fn to_path_buf(&self) -> PathBuf {
        let n = &self.0;
        let c = n.split('/');

        let mut p = PathBuf::new();
        for c in c {
            p.push(c);
        }

        p
    }
}

fn blob_to_path(blob: impl AsRef<str>) -> PathBuf {
    let n = blob.as_ref();
    let c = n.split('/');

    let mut p = PathBuf::new();
    for c in c {
        p.push(c);
    }

    p
}

fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let client = builder_from_url(&args.url).context("failed to build storage account client")?;
    let mut segments = args
        .url
        .path_segments()
        .context("SAS URL has no path segments")?;
    let container = segments.next().context("no container specified")?;

    let driver =
        BlobFSDriver::new(client.container_client(container)).context("failed to setup driver")?;

    std::fs::create_dir_all("test").unwrap();
    let _instance = start_proj_virtualization("test", Box::new(driver)).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(u64::MAX));

    Ok(())
}

struct BlobFSDriver {
    client: ContainerClient,
    /// Directories that we know about. Hack to ensure consistency between iteration and metadata calls.
    known_dirs: Mutex<HashSet<PathBuf>>,
    /// Required by the current API for ProjFS.
    iter_cache: projfs::CacheMap<<Self as ProjFSDirEnum>::DirIter>,
    /// An asynchronous runtime for dispatching requests to Azure blob storage.
    rt: tokio::runtime::Runtime,
}

impl BlobFSDriver {
    pub fn new(client: ContainerClient) -> Result<Self> {
        Ok(Self {
            client,
            known_dirs: Default::default(),
            iter_cache: Default::default(),
            rt: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build tokio runtime")?,
        })
    }
}

impl ProjFSDirEnum for BlobFSDriver {
    type DirIter = Box<dyn Iterator<Item = FileBasicInfo> + Send + Sync>;

    fn dir_iter(
        &self,
        _id: projfs::Guid,
        path: projfs::RawPath,
        _pattern: Option<projfs::RawPath>,
        _version: projfs::VersionInfo,
    ) -> std::io::Result<Self::DirIter> {
        let path = BlobPath::from(path.to_path_buf());
        info!("iter: {path}");

        let stream = self
            .client
            .list_blobs()
            .prefix(path.to_string())
            .into_stream();

        let r = self
            .rt
            .block_on(async {
                stream
                    .map_ok(|b| {
                        // HACK: Not really sure why I have to map the inner here, but
                        // we quickly get into trait hell if it isn't mapped to a Result<_>.
                        futures::stream::iter(
                            b.blobs.items.into_iter().map(|b| Ok::<_, anyhow::Error>(b)),
                        )
                    })
                    .try_flatten()
                    .try_collect::<Vec<_>>()
                    .await
            })
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.context("failed to query blob storage"),
                )
            })?;

        let mut subdirs = HashSet::new();
        let mut items = Vec::new();

        for i in r.iter() {
            match i {
                BlobItem::Blob(b) => {
                    let path = path.to_path_buf();

                    // Determine which files are located in "subdirectories" from the search path,
                    // and hide them behind folder entries.
                    let blob_path = BlobPath::new(&b.name).to_path_buf();

                    // All blobs should have a parent, since blob storage cannot represent empty folders.
                    if let Some(blob_folder) = blob_path.parent() {
                        if blob_folder != &path {
                            // Determine the relative path, and strip the first component to use as the folder name.
                            let rel_path = blob_folder.strip_prefix(&path).unwrap();

                            if let Some(std::path::Component::Normal(dir)) =
                                rel_path.components().next()
                            {
                                let dir = dir.to_str().unwrap();

                                if subdirs.insert(dir.to_string()) == true {
                                    info!("-> folder: {}", dir);

                                    // HACK: Track "known" directories.
                                    let mut dirs = self.known_dirs.lock().unwrap();
                                    dirs.insert(path.join(dir));

                                    items.push(FileBasicInfo {
                                        file_name: dir.into(),
                                        is_dir: true,
                                        file_size: 0,
                                        created: 0,
                                        accessed: 0,
                                        writed: 0,
                                        changed: 0,
                                        attrs: 0,
                                    })
                                }
                            }

                            continue;
                        }
                    }

                    let file_name = blob_path.file_name().unwrap().to_str().unwrap();
                    info!("-> {file_name}");

                    // Alright, we should only get here if this is a file in the current directory.
                    items.push(FileBasicInfo {
                        file_name: file_name.into(),
                        is_dir: false,
                        file_size: b.properties.content_length,
                        created: 0,
                        accessed: 0,
                        writed: 0,
                        changed: 0,
                        attrs: 0,
                    })
                }
                BlobItem::BlobPrefix(_) => todo!(),
            }
        }

        Ok(Box::new(items.into_iter()))
    }

    fn dir_iter_cache(&self, _version: projfs::VersionInfo) -> &projfs::CacheMap<Self::DirIter> {
        &self.iter_cache
    }
}

impl ProjFSRead for BlobFSDriver {
    fn get_metadata(
        &self,
        path: projfs::RawPath,
        _version: projfs::VersionInfo,
    ) -> std::io::Result<FileBasicInfo> {
        let path = path.to_path_buf();
        info!("metadata: {}", path.display());

        let dirs = self.known_dirs.lock().unwrap();
        if dirs.contains(&path) {
            return Ok(FileBasicInfo {
                file_name: path,
                is_dir: true,
                file_size: 0,
                created: 0,
                accessed: 0,
                writed: 0,
                changed: 0,
                attrs: 0,
            });
        }

        drop(dirs);

        let r = self
            .client
            .blob_client(path.to_string_lossy())
            .get_properties()
            .into_future();

        let blob = self
            .rt
            .block_on(async { r.await })
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.context("failed to query blob storage"),
                )
            })?
            .blob;

        Ok(FileBasicInfo {
            file_name: blob.name.clone().into(),
            is_dir: false,
            file_size: blob.properties.content_length,
            created: 0,
            accessed: 0,
            writed: 0,
            changed: 0,
            attrs: 0,
        })
    }

    fn read(
        &self,
        path: projfs::RawPath,
        _version: projfs::VersionInfo,
        offset: u64,
        buf: &mut [u8],
    ) -> std::io::Result<()> {
        let path = BlobPath::from(path.to_path_buf());
        info!("{path}: {offset}, {}", buf.len());

        let mut r = self
            .client
            .blob_client(path.as_str())
            .get()
            .range(azure_core::request_options::Range {
                start: offset,
                end: offset + (buf.len() as u64),
            })
            .into_stream();

        self.rt.block_on(async move {
            while let Ok(Some(r)) = r.try_next().await {
                let bytes = r.data.collect().await.unwrap();

                if let Some(range) = r.content_range {
                    buf[range.start as usize..(range.end + 1) as usize].copy_from_slice(&bytes[..]);
                } else {
                    buf[..].copy_from_slice(&bytes[..]);
                }
            }
        });

        Ok(())
    }
}
