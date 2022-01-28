
## Introduction

This directory contains code for the following paper:

**Gang Liu, Leying Chen, Shimin Chen**. *Zen: a High-Throughput Log-Free OLTP Engine for Non-Volatile Main Memory*. PVLDB 14(5): 835-848. 

We implement two prototypes of Zen based on Cicada and DBx1000, respectively.

## Machine Configuration

The machines should be equipped with Intel Optane DC Persistent Memory. The Intel Optane DC Persistent Memory should be configured as App-Direct mode. It uses PMDK to map NVM into the virtual address space of the process. 

Suppose the NVDIMM device is /dev/pmem0, a file system is created on the device, and it is mounted to /mnt/mypmem0. Make sure we have permissions to create and read files in /mnt/mypmem0. For example, we can create a subdirectory for each user that wants to work on NVM, then use chown to change the owner of the subdirectory to the user.

## Directory Structure and Usage

The directory is organized as follows:

Name         | Explanation
------------ | -------------
README.md    | this file
zen-cicada   | zen prototype based on cicada engine
zen-dbx1000  | zen prototype based on dbx1000
LICENSE      | license for zen and its based codebase

Please read zen-cicada/README.md and zen-dbx1000/README.md for detail usage.

## License 

The copyrights of original DBx1000 and Cicada are held by original authors, respectively.

Apache License, Version 2.0 (required by Cicada)
ISC License (required by DBx1000)

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

