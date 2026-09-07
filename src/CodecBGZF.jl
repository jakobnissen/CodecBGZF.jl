module CodecBGZF

export
    BGZFCompressorStream,
    BGZFDecompressorStream,
    BGZFError,
    VirtualOffset,
    offsets,
    gzi

using LibDeflate
import LibDeflate: GzipExtraField
using TranscodingStreams

import TranscodingStreams:
    TranscodingStreams,
    TranscodingStream,
    Memory,
    Error

import Base.Threads.@spawn

const DE_COMPRESSOR = Union{Compressor, Decompressor}

"""
    BGZFError(message::String)

BGZF de/compressor errored with `message` when processing data."
"""
struct BGZFError <: Exception
    message::String
end

function bitload(T::Type{<:Base.BitInteger}, data::Vector{UInt8}, p::Integer)
    ltoh(unsafe_load(Ptr{T}(pointer(data, p))))
end

function bitstore(v::Base.BitInteger, data::Vector{UInt8}, p::Integer)
    unsafe_store!(Ptr{typeof(v)}(pointer(data, p)), htol(v))
end
    
@noinline bgzferror(s::String) = throw(BGZFError(s))

include("virtualoffset.jl")
include("block.jl")
include("bgzfstream.jl")

end # module
