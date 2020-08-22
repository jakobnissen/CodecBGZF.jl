# CodecBGZF.jl

![CI](https://github.com/jakobnissen/CodecBGZF.jl/workflows/CI/badge.svg)
[![codecov](https://codecov.io/gh/jakobnissen/CodecBGZF.jl/branch/master/graph/badge.svg?token=VKFC19YZUG)](https://codecov.io/gh/jakobnissen/CodecBGZF.jl)

_Codec for BGZF files_

This package implements an efficient codec for [BGZF files](https://samtools.github.io/hts-specs/SAMv1.pdf). The BGZF format consists of the concatenation of small gzip blocks. Because the format is blocked, it allows for random access and siginificantly faster de/compression.

The package has the following notable features:
* Correctness above all: The BGZF format is well specified, and the package must write and read spec-compliantly. This includes validating the given checksums, decompression lengths, and the trailing EOF block.
* Integration with the Julia ecosystem. This is achieved by this package being a codec for the `TranscodingStreams.jl` package.
* Speed: This package should be the fastest Julia implementation of a BGZF parser. It is achieved by leveraging LibDeflate.jl, and by doing de/compression in a multithreaded and asynchronous manner.
* Convenient random access with *virtual file offsets*.
* Creation of [GZI](http://www.htslib.org/doc/bgzip.html) index files directly from compressed bgzipped files.

## API
__High level API__

* `BGZFDecompressorStream(io::IO; nthreads=Threads.nthreads())` - create a decompressing `TranscodingStream`.
* `BGZFCompressorStream(io::IO; nthreads=Threads.nthreads(), compresslevel=6)` - create a compressing `TranscodingStream` compressing to level `compresslevel`.
* `gzi(io::IO)` - return a `Vector{UInt8}` representing the GZI index for a BGZF file `io`. To be used like this: `gzi(open("/path/to/file.bgz"))`
* `VirtualOffset(s::BGZFDecompressorStream)` - Get an object representing the current offset of the stream. You can obtain the block offset and inblock offsets with `offsets(v)`
* `seek(s::BGZFDecompressorStream, v::VirtualOffset)` - seek the stream to the given offset.
* Being `TranscodingStreams`, you can expect the usual IO-related functions to work on the streams.
