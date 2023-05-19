"""
    BGZF blocks
 
Code in this file should not "know about" Codecs, TranscodingStreams,
or any of that. It should only rely on Base and LibDeflate, so that means the
code in this file can easily be cannibalized for other packages, or repurposed.
"""
module Blocks

import ..bitstore!, ..bitload, ..DE_COMPRESSOR, ..bgzferror

using Base.Threads: @spawn

using LibDeflate:
    ReadableMemory,
    LibDeflateError,
    Compressor,
    Decompressor,
    parse_gzip_header,
    GzipExtraField,
    unsafe_decompress!,
    unsafe_compress!,
    unsafe_crc32

export Block,
    bsize,
    load_block!

# Every BGZF block begins with exactly these bytes.
# Note that we MUST e.g. ignore mtime and OS to be completely BGZF compliant
const BLOCK_HEADER = [
    0x1f, 0x8b, # Magic bytes
    0x08, # Compression method is DEFLATE
    0x04, # Flags: Contains extra fields
    0x00, 0x00, 0x00, 0x00, # Modification time (mtime): Zero'd out
    0x00, # Extra flags: None used
    0xff, # Operating system: Unknown (we don't care about OS)
    0x06, 0x00, # 6 bytes of extra data to follow
    0x42, 0x43, # Xtra info tag: "BC"
    0x02, 0x00 # 2 bytes of data for tag "BC",
]

const EOF_BLOCK = vcat(
    BLOCK_HEADER,
    [
        0x1b, 0x00, # Total size of block - 1
        0x03, 0x00, # DEFLATE compressed load of the empty input
        0x00, 0x00, 0x00, 0x00, # CRC32 of the empty input
        0x00, 0x00, 0x00, 0x00  # Input size of the empty input 
    ]
)

# BGZF blocks are no larger than 64 KiB before and after compression.
const MAX_BLOCK_SIZE = UInt(64 * 1024)

# Maximum number of bytes to be compressed at one time. Random bytes usually end up filling
# a bit more when compressed, so we have a generous 256 byte margin of safety.
const SAFE_BLOCK_SIZE = UInt(MAX_BLOCK_SIZE - 256)

# Field descriptions are for decompressors / compressors
mutable struct Block{T}
    de_compressor::T
    outdata::Vector{UInt8}
    indata::Vector{UInt8}
    task::Task
    gzip_extra_fields::Vector{GzipExtraField} # cached to avoid allocating
    crc32::UInt32    # stated checksum / calculated checksum

    # BGZF blocks can store 0:typemax(UInt16)+1 bytes
    # so unfortunately UInt16 will not suffice here.
    outlen::UInt32   # Length of decompressed payload / compressed block
    inlen::UInt32    # Length of compressed payload / total input block
end

function Block(dc::T) where T <: DE_COMPRESSOR
    outdata = Vector{UInt8}(undef, MAX_BLOCK_SIZE)
    indata = similar(outdata)

    # We initialize with a trivial, but completable task for sake of simplicity
    task = schedule(Task(() -> nothing))
    return Block{T}(dc, outdata, indata, task, GzipExtraField[], 0, 0, 0)
end

function Base.empty!(block::Block)
    block.outlen = 0
    block.inlen = 0
    block
end
Base.isempty(block::Block) = iszero(block.outlen)

nfull(::Type{Block{Decompressor}}) = MAX_BLOCK_SIZE
nfull(::Type{Block{Compressor}}) = SAFE_BLOCK_SIZE
Base.wait(b::Block) = wait(b.task)

function check_eof_block(block::Block{Decompressor})
    # We don't store the metadata, so can't compare to EOF_BLOCK. BGZF specs
    # allows us to simply check for a zero-length block.
    if !iszero(block.outlen)
        bgzferror("No EOF block. Truncated file?")
    end
end

"Get the BSIZE field from block by reading from given vector"
function bsize(block::Block{Decompressor}, vector::Vector{UInt8})::Union{UInt16, Nothing}
    fieldnum = findfirst(block.gzip_extra_fields) do field
        field.tag === (UInt8('B'), UInt8('C'))
    end
    fieldnum === nothing && return nothing
    field = @inbounds block.gzip_extra_fields[fieldnum]
    field.data === nothing && return nothing
    length(field.data) != 2 && return nothing
    return (vector[first(field.data)] % UInt16) | ((vector[last(field.data)] % UInt16) << 8)
end

"""Load data from buffer starting at index 1 into the block, and queue its decompression
in another thread. Return number of bytes consumed"""
function load_block!(block::Block{Decompressor}, buffer::Vector{UInt8}, len::Unsigned)
    GC.@preserve buffer begin
        parsed_header = parse_gzip_header(
            ReadableMemory(pointer(buffer), len); extra_data=block.gzip_extra_fields
        )
        parsed_header isa LibDeflateError && bgzferror("Block does not contain a gzip header")
        header_len, _ = parsed_header
        bsiz = bsize(block, buffer)
        bsiz === nothing && bgzferror("No GZIP extra field \"BSIZE\"")
        # By spec, BSIZE is block size -1. Include header_len bytes header, 8 byte tail
        blocksize = bsiz + 1
        block.inlen = blocksize - header_len - 8
        block.crc32 = bitload(UInt32, buffer, blocksize - 7)
        block.outlen = bitload(UInt32, buffer, blocksize - 3)
        unsafe_copyto!(block.indata, 1, buffer, header_len + 1, block.inlen)
    end
    queue!(block)
    return blocksize
end

"Load `len_minus_one+1` bytes from `buffer` into the block and queue its compression in another thread."
function load_block!(block::Block{Compressor}, buffer::Vector{UInt8}, len::Unsigned)
    to_compress = min(len, SAFE_BLOCK_SIZE)
    block.inlen = to_compress
    # We zero the outlen to mark that it has no data _until_ the queue happens,
    # where the correct outlen is set
    block.outlen = 0
    unsafe_copyto!(block.indata, 1, buffer, 1, to_compress)
    queue!(block)
    return nothing
end

"Process the block in another thread"
queue!(block::Block) = block.task = @spawn _queue!(block)
 
function _queue!(block::Block{Decompressor})
    (indata, outdata) = (block.indata, block.outdata)
    GC.@preserve indata outdata begin
        unsafe_decompress!(
            Base.HasLength(),
            block.de_compressor,
            pointer(outdata), block.outlen,
            pointer(indata), block.inlen
        )

        crc32 = unsafe_crc32(pointer(outdata), block.outlen)
    end
    crc32 != block.crc32 && bgzferror("CRC32 checksum does not match")
    return nothing
end

function _queue!(block::Block{Compressor})
    (indata, outdata) = (block.indata, block.outdata)
    GC.@preserve indata outdata begin
        # Meat: The compressed data
        compress_len = unsafe_compress!(
            block.de_compressor,
            pointer(outdata, 19), MAX_BLOCK_SIZE - 26,
            pointer(indata), block.inlen
        )
        block.crc32 = unsafe_crc32(pointer(block.indata), block.inlen)
        block.outlen = compress_len + 26

        # Header: 18 bytes of header
        unsafe_copyto!(outdata, 1, BLOCK_HEADER, 1, 16)
        bitstore!(UInt16(block.outlen - 1), outdata, 17)

        # Tail: CRC + isize
        bitstore!(block.crc32, outdata, 18 + compress_len + 1)
        bitstore!(block.inlen % UInt32, outdata, 18 + compress_len + 5)
    end
    return nothing
end

end # module