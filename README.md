# (Rust) azmount
[azure-storage-fuse](https://github.com/Azure/azure-storage-fuse), re-implemented in Rust using [ProjFS](https://learn.microsoft.com/en-us/windows/win32/projfs/projected-file-system) to support Windows.

This is a non-performant proof of concept for mounting an Azure blob storage account directly into a folder on Windows. Things are glitchy and broken, but can be vastly improved over time.

## Features
| Feature                    | azmount | azure-storage-fuse |
| -------------------------- | ------- | ------------------ |
| Downloading files          | ✅     | ✅                |
| Uploading files            | ❌     | ✅                |
| Windows support            | ✅     | ❌                |
| Linux support              | ❌     | ✅                |
