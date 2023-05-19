module CodecBGZF

export
    BGZFCompressorStream,
    BGZFDecompressorStream,
    BGZFError,
    VirtualOffset,
    offsets,
    gzi

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

using TranscodingStreams:
    TranscodingStreams,
    TranscodingStream

using Base.Threads: @spawn

const DE_COMPRESSOR = Union{Compressor, Decompressor}

"""
    BGZFError(message::String)

BGZF de/compressor errored with `message` when processing data."
"""
struct BGZFError <: Exception
    message::String
end

const BitInteger = Union{UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64}

function bitload(T::Type{<:BitInteger}, data::Vector{UInt8}, p::Integer)
    ltoh(unsafe_load(Ptr{T}(pointer(data, p))))
end

function bitstore!(v::BitInteger, data::Vector{UInt8}, p::Integer)
    unsafe_store!(Ptr{typeof(v)}(pointer(data, p)), htol(v))
end
    
@noinline bgzferror(s::String) = throw(BGZFError(s))

include("virtualoffset.jl")
include("block.jl")

using .Blocks

include("bgzfstream.jl")

end # module
