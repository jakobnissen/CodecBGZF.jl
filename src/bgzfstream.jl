# Note: Two important features of this code is that it's multithreaded, and that the
# de/compression threads are launched asyncronously, such that it can de/compress one
# block while it's reading other blocks

"""
    BGZFCodec{T, IO} <: Codec

Codec used to compress or decompress data from the input IO source of type `IO`.
`T` must be `LibDeflate.Compressor` or `LibDeflate.Decompressor`.
"""
mutable struct BGZFCodec{T <: DE_COMPRESSOR, O <: IO} <: TranscodingStreams.Codec
    # This stores data before it is loaded into blocks. We only load "full" blocks
    # for de/compression, while this buffer may have partial blocks at times, but
    # never more than one block's worth of data.
    buffer::Vector{UInt8}
    blocks::Vector{Block{T}}

    # This keeps the last virtualoffset of previous blocks that we have already
    # shipped off to the stream's output buffer. We use this to backtrace and find
    # the virtualoffset for the stream.
    offsets::Vector{VirtualOffset}

    # We need this in order to write an EOF on end for the CompressorCodec
    io::O

    index::UInt      # Index of the currently used block in "blocks"
    bufferlen::UInt32  # Number of filled bytes of the buffer
    blockindex::UInt32 # Index of buffer within the currently used block 
end

const CompressorCodec{O} = BGZFCodec{Compressor, O} where {O <: IO}
const DecompressorCodec{O} = BGZFCodec{Decompressor, O} where {O <: IO}
const BGZFCompressorStream = TranscodingStream{CompressorCodec{O}, O} where {O <: IO}
const BGZFDecompressorStream = TranscodingStream{DecompressorCodec{O}, O} where {O <: IO}
const CACHED_VOFFSETS = 16

"""
    BGZFCompressorStream(io::IO; threads=nthreads(), compresslevel=6)

Create a `TranscodingStream` which block-gzip compresses data from the underlying `io` with
the given `compresslevel`. The stream will compress up to `threads` block concurrently.
"""
function BGZFCompressorStream(io::IO; nthreads=Threads.nthreads(), compresslevel::Int=6)
    codec = CompressorCodec(io, nthreads, compresslevel)
    return TranscodingStream(codec, io; bufsize=nfull(Block{Compressor}))
end

"""
    BGZFDecompressorStream(io::IO; threads=nthreads())

Create a `TranscodingStream` which decompresses block-gzipped data from the underlying `io`.
The stream will compress up to `threads` block concurrently.
"""
function BGZFDecompressorStream(io::IO; nthreads=Threads.nthreads())
    codec = DecompressorCodec(io, nthreads)
    return TranscodingStream(codec, io; bufsize=nfull(Block{Decompressor}))
end

function CompressorCodec(io::IO, nthreads, compresslevel)
    nthreads < 1 && throw(ArgumentError("Must use at least 1 thread"))
    buffer = Vector{UInt8}(undef, SAFE_BLOCK_SIZE)
    blocks = [Block(Compressor(compresslevel)) for i in 1:nthreads]
    offsets = fill(VirtualOffset(0,0), CACHED_VOFFSETS)
    return CompressorCodec{typeof(io)}(buffer, blocks, offsets, io, 1, 0, 1)
end

function DecompressorCodec(io::IO, nthreads)
    nthreads < 1 && throw(ArgumentError("Must use at least 1 thread"))
    buffer = Vector{UInt8}(undef, MAX_BLOCK_SIZE)
    blocks = [Block(Decompressor()) for i in 1:nthreads]
    offsets = fill(VirtualOffset(0,0), CACHED_VOFFSETS)
    return DecompressorCodec{typeof(io)}(buffer, blocks, offsets, io, 1, 0, 1)
end

# Note: This function MUST make progress in either input or output, else
# transcoding streams will keep resizing the buffers thinking the small buffer
# size blocks the codec.
function TranscodingStreams.process(codec::BGZFCodec{T}, input::Memory, output::Memory, error::Error) where T
    consumed = 0
    eof = iszero(length(input))
    
    # We must continue looping through the blocks until we have either consumed
    # or produced data
    while true
        # If we have spare data in the current block, just give that
        is_current_block_empty(codec) || return copy_from_outbuffer(codec, output, consumed)

        # If there is data to be read in, we do that.
        if (length(input) - consumed) > 0
            nb = min(remaining(codec), length(input) - consumed)
            outptr = pointer(codec.buffer, codec.bufferlen + 1)
            inptr = input.ptr + consumed
            unsafe_copyto!(outptr, inptr, nb)
            consumed += nb
            codec.bufferlen += nb
        end

        # If we have read in data, but still not enough to queue a block, return no data
        # and wait for more data to be passed
        wait = !iszero(consumed) & (codec.bufferlen < nfull(Block{T})) 
        wait && return (consumed, 0, :ok)

        # At this point, if there is any data in the buffer, it must be enough
        # to queue a whole block (since the buffer is either full, or input is EOF)
        if !iszero(codec.bufferlen)
            load_block!(codec)
        end


        # Move to next block where there is either data to return, or space to
        # load more data in (if we have more data to load in)
        moredata = !(eof & iszero(codec.bufferlen))
        blockindex = next_block!(codec, moredata)

        # This happens if there is no block to go to to either load new data or return
        # existing data. Then we are done.
        blockindex === nothing && return (0, 0, :end)
    end
end

nblocks(c::BGZFCodec) = length(c.blocks)
get_block(c::BGZFCodec) = @inbounds c.blocks[c.index]
last_block(c::BGZFCodec) = @inbounds c.blocks[ifelse(c.index == 1, nblocks(c), c.index-1)]

"Check if the currently active block has no more output bytes"
function is_current_block_empty(c::BGZFCodec)
    block = get_block(c)
    return c.blockindex > block.outlen
end

"Return data already prepared in the current block"
function copy_from_outbuffer(codec::BGZFCodec, output::Memory, consumed::Integer)
    block = get_block(codec)
    available = block.outlen - codec.blockindex + 1
    n = min(available, length(output))
    unsafe_copyto!(output.ptr, pointer(block.outdata, codec.blockindex), n)
    codec.blockindex += n

    # If we have copied all data from a block, we mark it as empty
    # and reset the blockindex
    if codec.blockindex > block.outlen
        empty!(block)
        codec.blockindex = 1
    end
    return (Int(consumed), n, :ok)
end

"Get number of remaning bytes in the codec's buffer before a new block can be indexed"
remaining(codec::BGZFCodec{T}) where T = nfull(Block{T}) - codec.bufferlen

function load_block!(codec::DecompressorCodec)
    buffer = codec.buffer
    nbytes = load_block!(get_block(codec), buffer, codec.bufferlen)
    unsafe_copyto!(buffer, 1, buffer, nbytes+1, codec.bufferlen-nbytes)
    codec.bufferlen -= nbytes
    return nothing
end

"Load data from buffer into the current block, and queue its decompression"
function load_block!(codec::CompressorCodec)
    load_block!(get_block(codec), codec.buffer, UInt16(codec.bufferlen - 1))
    codec.bufferlen = 0
    return nothing
end

"""Switches the code to the next block to process, given whether the input stream is eof.
Returns block index, or nothing if there are no more blocks to process"""
function next_block!(codec::BGZFCodec, moredata::Bool)
    block = get_block(codec)
    nextblock = nothing
    nextindex = codec.index
    while nextblock !== block
        nextindex = ifelse(nextindex == nblocks(codec), 1, nextindex + 1)
        nextblock = @inbounds codec.blocks[nextindex]
        wait(nextblock)

        # If there is more data to get from that block, or we have more
        # data to load in, we can use that block
        if moredata || !isempty(nextblock)
            
            # If the block contains data, we need to add that data to the offsets
            # isempty(nextblock) || push_offsets!(codec) # TODO
            return (codec.index = nextindex)
        end
    end

    # Else, if no blocks have any data and we have no data to load
    # into them, we are done, return nothing
    return nothing
end

######################### TODO: THIS MARKS THE END OF TESTED CODE.

"Add the current block's offsets and lengths to the codex offset vector"
function push_offsets!(codec::BGZFCodec)
    unsafe_copyto!(codec.offsets, 2, codec.offsets, 1, CACHED_VOFFSETS - 1)
    block = get_block(codec)
    oldcoffset, olduoffset = @inbounds codec.offsets[2]

    codec.offsets[1] = (block.offset, block.outlen - 1)
end

"Get the offset for the soon-to-be indexed block based on the previous block"
function get_new_offset(codec::DecompressorCodec)
    lastblock = last_block(codec)
    return lastblock.offset + lastblock.blocklen
end

"Get the offset for the soon-to-be indexed block based on the previous block"
function get_new_offset(codec::CompressorCodec)
    lastblock = last_block(codec)
    return lastblock.offset + lastblock.inlen
end

function reset!(s::BGZFDecompressorStream)
    TranscodingStreams.initbuffer!(s.state.buffer1)
    TranscodingStreams.initbuffer!(s.state.buffer2)
    for block in s.codec.blocks
        empty!(block)
    end
    s.codec.index = 1
    s.codec.bufferlen = 0
    fill!(s.codec.offsets, (0, 0))
    return s
end

function _seek(s::BGZFDecompressorStream, i::Integer)
    reset!(s)
    seek(s.stream, i)
    last(s.codec.blocks).offset = i
    return s
end

"""
    seekstart(stream::BGZFDecompressorStream)

Seek the `stream`'s input stream to its beginning, and resets the `stream`.
"""
function Base.seekstart(s::BGZFDecompressorStream)
    reset!(s)
    seekstart(s.stream)
    last(s.codec.blocks).offset = 0
    return s
end

"""
    seek(stream::BGZFDecompressorStream, v::VirtualOffset)

Seek `stream` to the given `VirtualOffset` `v`. `v` must be a valid virtual offset for the given
`stream`, i.e. its coffset must be the offset of a valid BGZF block, and its uoffset must be an
integer in [0, block_len]. Furthermore, `stream`'s underlying stream must be seekable.
"""
function Base.seek(s::BGZFDecompressorStream, v::VirtualOffset)
    block_offset, byte_offset = offsets(v)
    _seek(s, block_offset)

    # Set a "dummy offset" - when getting the virtual offset after seeking, we
    # need to be able to obtain original block offset
    s.codec.offsets[1] = (block_offset, 0)

    # Read one byte to fill in buffer
    is_eof = eof(s)
    is_eof || read(s, UInt8)

    # Now advance buffer block_offset minus the one byte we just read
    if byte_offset > get_block(s.codec).outlen
        throw(ArgumentError("Too large offset for block"))
    end
    s.state.buffer1.bufferpos += (byte_offset % Int - !is_eof)
    return s
end

"""
    VirtualOffset(stream::BGZFDecompressorStream)

Obtain the `VirtualOffset` of the curret position of `stream`. If `stream's` input stream is
seekable, seeking to this offset will leave the stream in an equivalent state to its current state.
A `BGZFDecompressorStream` only tracks the offset of the 16 previous blocks. If more than 16 blocks
are stored in `stream`'s output buffer, this operation will fail.
"""
function VirtualOffset(s::BGZFDecompressorStream)
    # This is a little tricky, because the output buffer may buffer an arbitrary
    # large amount of blocks, and we can't keep track of all these blocks'
    # offsets
    n_buffered = max(0, s.state.buffer1.bufferpos - s.state.buffer1.markpos - 1)
    blockindex = 1

    # Next we backtrace
    while n_buffered >= 0
        if blockindex > length(s.codec.offsets)
            bgzferror("Too many blocks buffered to retrace original block offset")
        end
        (offset, decompressed) = s.codec.offsets[blockindex]
        blockindex += 1
        n_buffered - decompressed <= 0 && break
        n_buffered -= decompressed
    end

    offset, decompressed = s.codec.offsets[blockindex]
    return VirtualOffset(offset, n_buffered)
end

function TranscodingStreams.finalize(codec::CompressorCodec)
    write(codec.io, EOF_BLOCK)
end

# Read exactly N bytes to i'th index of data, except if stream is EOF
function read_nbytes(io::TranscodingStream, data::Vector{UInt8}, i::Integer, N::Integer)
    n = 0
    GC.@preserve data begin
        ptr = pointer(data, i)
        while n < N
            nb = TranscodingStreams.unsafe_read(io, ptr, N-n)
            ptr += nb
            n += nb
            iszero(nb) && break
        end
    end
    return n
end

"""
    gzi(io::IO)

Construct a `Vector{UInt8}` with the GZI of a BGZF file of an `IO` representing a
BGZF file. GZI files contain offsets for each block and its decompressed payload
in a BGZF file. A GZI file is approximately 1/4000th the size of the decompressed
data in a BGZF file.

## Examples
```julia
julia open(gzi, "/my/bgzip/file.bgz")
102744-element Array{UInt8,1}:
 0x15
 0x19
 0x00
 0x00
 0x00
[ ... ]
```
"""
function gzi(io::TranscodingStream)
    buffer = Vector{UInt8}(undef, MAX_BLOCK_SIZE)
    gzi = Tuple{UInt64, UInt64}[]
    coffset, uoffset = 0, 0
    len = read_nbytes(io, buffer, 1, length(buffer))
    while !iszero(len)
        push!(gzi, (coffset, uoffset))
        blocksize, cp, isize, crc32 = index!(buffer, len)
        coffset += blocksize
        uoffset += isize
        unsafe_copyto!(buffer, 1, buffer, blocksize + 1, len - blocksize)
        len -= blocksize
        len += read_nbytes(io, buffer, len+1, length(buffer)-len)
    end

    result = Vector{UInt8}(undef, 16 * length(gzi) + 8)
    bitstore(length(gzi) % UInt64, result, 1)
    unsafe_copyto!(pointer(result, 9), Ptr{UInt8}(pointer(gzi)), length(result)-8)
    return result
end

# Fallback for non-transcodingstream objects
gzi(io::IO) = gzi(NoopStream(io))
