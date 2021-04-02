# BGZF blocks
# 
# Code in this file should not "know about" Codecs, TranscodingStreams,
# or any of that. It should only rely on LibDeflate, so that means the
# code in this file can easily be cannibalized for other packages.

# We must write these bytes *exactly* per the BGZF spec,
# we can't just write an empty block, as some of the fields
# may not be these exact bytes.
const EOF_BLOCK = [
    0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00,
    0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
    0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00
]

# BGZF blocks are no larger than 64 KiB before and after compression.
const MAX_BLOCK_SIZE = UInt(64 * 1024)

# Maximum number of bytes to be compressed at one time. Random bytes usually end up filling
# a bit more when compressed, so we have a 256 byte margin of safety.
const SAFE_BLOCK_SIZE = UInt(MAX_BLOCK_SIZE - 256)

# Field descriptions are for decompressors / compressors
mutable struct Block{T}
    de_compressor::T
    outdata::Vector{UInt8}
    indata::Vector{UInt8}
    task::Task
    gzip_extra_fields::Vector{LibDeflate.GzipExtraField} # cached to avoid allocating
    crc32::UInt32    # stated checksum / calculated checksum
    outlen::UInt16   # Length of decompressed payload / compressed block
    inlen::UInt16    # Length of compressed payload / total input block
end

function Block(dc::T) where T <: DE_COMPRESSOR
    outdata = Vector{UInt8}(undef, MAX_BLOCK_SIZE)
    indata = similar(outdata)

    # We initialize with a trivial, but completable task for sake of simplicity
    task = schedule(Task(() -> nothing))
    return Block{T}(dc, outdata, indata, task, LibDeflate.GzipExtraField[], 0, 0, 0)
end

function Base.empty!(block::Block)
    block.outlen = 0
    block.inlen = 0
end
Base.isempty(block::Block) = iszero(block.outlen)

nfull(::Type{Block{Decompressor}}) = MAX_BLOCK_SIZE
nfull(::Type{Block{Compressor}}) = SAFE_BLOCK_SIZE
Base.wait(b::Block) = wait(b.task)

function check_eof_block(block::Block{Decompressor})
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
    return vector[first(field.data)] | (vector[last(field.data)] << 8)
end

"Process the block in another thread"
queue!(block::Block) = block.task = @spawn _queue!(block)

function _queue!(block::Block{Decompressor})
    unsafe_decompress!(Base.HasLength(), block.de_compressor,
                       pointer(block.outdata), block.outlen,
                       pointer(block.indata), block.inlen)

    crc32 = unsafe_crc32(pointer(block.outdata), block.outlen)
    crc32 != block.crc32 && bgzferror("CRC32 checksum does not match")
    return nothing
end

function _queue!(block::Block{Compressor})
    # Meat: The compressed data
    compress_len = unsafe_compress!(block.de_compressor,
                   pointer(block.outdata, 19), MAX_BLOCK_SIZE - 26,
                   pointer(block.indata), block.inlen)
    block.crc32 = unsafe_crc32(pointer(block.indata), block.inlen)
    block.outlen = compress_len + 26

    # Header: 18 bytes of header
    unsafe_copyto!(block.outdata, 1, BLOCK_HEADER, 1, 16)
    bitstore(UInt16(block.outlen - 1), block.outdata, 17)

    # Tail: CRC + isize
    bitstore(block.crc32, block.outdata, 18 + compress_len + 1)
    bitstore(block.inlen % UInt32, block.outdata, 18 + compress_len + 5)
    return nothing
end
