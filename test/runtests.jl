using CodecBGZF
using Test

# Make a buffer type which we can read from after closing it
struct Buffer <: IO
    x::Base.GenericIOBuffer{Vector{UInt8}}
end

Buffer() = Buffer(IOBuffer())
Buffer(x) = Buffer(IOBuffer(x))
Base.read(b::Buffer, ::Type{UInt8}) = read(b.x, UInt8)
Base.write(b::Buffer, x) = write(b.x, x)
Base.write(b::Buffer, x::UInt8) = write(b.x, x)
Base.write(b::Buffer, x::Vector{UInt8}) = write(b.x, x)
Base.take!(b::Buffer) = take!(b.x)
Base.close(::Buffer) = nothing
Base.isopen(b::Buffer) = isopen(b.x)
Base.bytesavailable(b::Buffer) = bytesavailable(b.x)
Base.eof(b::Buffer) = eof(b.x)
Base.seek(b::Buffer, v::Integer) = seek(b.x, v)

@testset "VirtualOffset" begin
    voff = VirtualOffset(0, 0)
    @test voff == VirtualOffset(0, 0)
    @test voff != VirtualOffset(0, 1)
    @test voff <= VirtualOffset(0, 1)
    @test voff != VirtualOffset(1, 0)
    @test voff <= VirtualOffset(1, 0)
    @test convert(UInt64, VirtualOffset(0, 1)) == UInt(1)
    @test convert(UInt64, VirtualOffset(1, 0)) == UInt(1 << 16)
    @test convert(VirtualOffset, UInt64(0x10001)) == VirtualOffset(1, 1)
    @test isless(voff, VirtualOffset(1, 0))
    @test voff[1] == 0
    @test voff[2] == 0
    @test string(voff) == "VirtualOffset(0, 0)"

    voff += 1
    @test voff[1] == 0
    @test voff[2] == 1

    voff = VirtualOffset(1234, 555)
    @test voff[1] == 1234
    @test voff[2] == 555
    @test string(voff) == "VirtualOffset(1234, 555)"

    voff = VirtualOffset(1234, 555)
    buf = IOBuffer()
    @test write(buf, voff) == sizeof(UInt64)
    seekstart(buf)
    @test read(buf, VirtualOffset) === voff
    @test eof(buf)

    @test_throws ArgumentError VirtualOffset(1 << 48, 0)
    @test_throws ArgumentError VirtualOffset(0, 1 << 16)
end

@testset "Basics" begin
	stream = BGZFDecompressorStream(IOBuffer("foo"))
	@test stream isa BGZFDecompressorStream

	stream = BGZFCompressorStream(IOBuffer("foo"))
	@test stream isa BGZFCompressorStream

    # Bad gzip identifier
    stream = BGZFDecompressorStream(IOBuffer([0x1a, 0x1a]))
	@test_throws BGZFError read(stream, UInt8)

    # Too short
    stream = BGZFDecompressorStream(IOBuffer([0x1f, 0x8b]))
	@test_throws BGZFError read(stream, UInt8)

    # Bad compression flag
    stream = BGZFDecompressorStream(IOBuffer([0x1f, 0x8b, 0x00, 0x04]))
	@test_throws BGZFError read(stream, UInt8)

    # Too short
    stream = BGZFDecompressorStream(IOBuffer([0x1f, 0x8b, 0x08, 0x04]))
	@test_throws BGZFError read(stream, UInt8)

    # Bad flag
    stream = BGZFDecompressorStream(IOBuffer([0x1f, 0x8b, 0x08, 0xfa]))
	@test_throws BGZFError read(stream, UInt8)

	bad_subfield = UInt8[0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00,
	                     0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x03, 0x00,
	                     0x23, 0x00, 0x01, 0x05, 0x00, 0xfa, 0xff, 0x68,
	                     0x65, 0x6c, 0x6c, 0x6f, 0x86, 0xa6, 0x10, 0x36,
	                     0x05, 0x00, 0x00, 0x00]

    stream = BGZFDecompressorStream(IOBuffer(bad_subfield))
	@test_throws BGZFError read(stream, UInt8)

    no_bsize     = UInt8[0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00,
	                     0x00, 0xff, 0x06, 0x00, 0x41, 0x43, 0x02, 0x00,
	                     0x23, 0x00, 0x01, 0x05, 0x00, 0xfa, 0xff, 0x68,
	                     0x65, 0x6c, 0x6c, 0x6f, 0x86, 0xa6, 0x10, 0x36,
	                     0x05, 0x00, 0x00, 0x00]

    stream = BGZFDecompressorStream(IOBuffer(no_bsize))
	@test_throws BGZFError read(stream, UInt8)


	bad_crc = copy(CodecBGZF.EOF_BLOCK)
	bad_crc[end-6] = 0x01
	@test_throws Exception read(BGZFDecompressorStream(Buffer(bad_crc)), UInt8)

    bad_isize = copy(CodecBGZF.EOF_BLOCK)
	bad_isize[end-2] = 0x01
	@test_throws Exception read(BGZFDecompressorStream(Buffer(bad_isize)), UInt8)
end

@testset "Simple I/O" begin
	# Reading
	filename = joinpath(dirname(@__FILE__), "bar.bgz")
    stream = BGZFDecompressorStream(open(filename))
    @test read(stream, UInt8) === UInt8('b')
    @test read(stream, UInt8) === UInt8('a')
    @test read(stream, UInt8) === UInt8('r')
    @test eof(stream)
    @test read(stream) == UInt8[]
    @test_throws EOFError read(stream, UInt8)
    @test flush(stream) === nothing
    @test close(stream) === nothing
    @test_throws ArgumentError read(stream, UInt8)

	# Writing
	dump = Buffer()
	stream = BGZFCompressorStream(dump)
	write(stream, "ba")
	write(stream, UInt8('r'))
	close(stream)
	data = take!(dump)
	@test data[1:2] == [0x1f, 0x8b]
	@test data[end-27:end] == CodecBGZF.EOF_BLOCK

	# Empty files
	stream = BGZFDecompressorStream(Buffer())
	data = read(stream)
	@test eof(stream)
	@test data == UInt8[]
    close(stream)

    buffer = Buffer()
	stream = BGZFCompressorStream(buffer)
	write(stream, "")
	close(stream)
	@test take!(buffer) == CodecBGZF.EOF_BLOCK

	# No EOF file
	buffer = Buffer()
	stream = BGZFCompressorStream(buffer)
	write(stream, "hello")
	close(stream)
	data = take!(buffer)
	noend = data[1:end-28]
	stream = BGZFDecompressorStream(Buffer(noend))
	@test_throws BGZFError read(stream)
end

@testset "Larger files" begin
    A = collect(reinterpret(UInt8, rand(1:2000, 100000)))
    buffer = Buffer()
    stream = BGZFCompressorStream(buffer)
    write(stream, A)
    close(stream)
    compressed = take!(buffer)

    @test length(compressed) < 350000
    @test compressed[end-27:end] == CodecBGZF.EOF_BLOCK

    stream = BGZFDecompressorStream(Buffer(compressed))
    @test read(stream) == A
end

@testset "More roundtrips" begin
    buffer = Buffer()
    stream = BGZFCompressorStream(buffer)
    write(stream, "")
    write(stream, "")
    write(stream, "Abra")
    write(stream, "")
    write(stream, "cadabra")
    write(stream, "!")
    close(stream)

    stream = BGZFDecompressorStream(Buffer(take!(buffer)))
    @test String(read(stream)) == "Abracadabra!"
end

@testset "Seeking" begin
	filename = joinpath(dirname(@__FILE__), "bar.bgz")
    stream = BGZFDecompressorStream(open(filename))
    read(stream, 1)
    @test read(stream, UInt8) == UInt8('a')
    seekstart(stream)
    @test read(stream, UInt8) == UInt8('b')
end
	
@testset "BGZF offsets" begin
    function test_virtualoffset(stream, data)
        v = VirtualOffset(stream)
        next = read(stream, 128)
        coff, uoff = offsets(v)
        @test data[coff+1:coff+2] == [0x1f, 0x8b]
        seekstart(stream)
        seek(stream, v)
        @test VirtualOffset(stream) == v
        @test read(stream, 128) == next
    end
    
    A = collect(reinterpret(UInt8, rand(1:2000, 100000)))
    buffer = Buffer()
    stream = BGZFCompressorStream(buffer)
    write(stream, A)
    close(stream)
    compressed = take!(buffer)

    stream = BGZFDecompressorStream(Buffer(compressed))
    read(stream, 25000)
    test_virtualoffset(stream, compressed)

    # Larger offset
    seekstart(stream)
    read(stream, 125000)
    test_virtualoffset(stream, compressed)
    close(stream)

    # Go back multiple blocks for offset, and also parse lots of empty blocks
    buffer = Buffer()
    for i in 1:20
        stream = BGZFCompressorStream(buffer)
        write(stream, rand(UInt8, 50))
        close(stream)
    end
    data = take!(buffer)
    buffer = Buffer(data)
    stream = BGZFDecompressorStream(buffer)
    read(stream, 10)
    test_virtualoffset(stream, data)
end

@testset "gzi" begin
    bytes = gzi(Buffer(CodecBGZF.EOF_BLOCK))
    @test length(bytes) == 24
    @test collect(reinterpret(Int, bytes)) == [1, 0, 0]

    filename = joinpath(dirname(@__FILE__), "bar.bgz")
    inbytes = open(read, filename)
    bytes = gzi(IOBuffer(inbytes))
    @test length(bytes) == 40
    reallen = [2, 0, 0, length(inbytes) - length(CodecBGZF.EOF_BLOCK), 3]
    @test collect(reinterpret(Int, bytes)) == reallen

    A = collect(reinterpret(UInt8, rand(1:2000, 100000)))
    buffer = Buffer()
    writer = BGZFCompressorStream(buffer)
    write(writer, A)
    close(writer)
    compressed = take!(buffer)
    bytes = gzi(Buffer(compressed))
    buffer = IOBuffer(compressed)
    nums = reinterpret(Int, bytes)
    @assert nums[1] * 2 + 1 == length(nums)
    reader = BGZFDecompressorStream(buffer)

    # Now seek every block offset. It will crash if its not a valid block
    for coffset in nums[2:2:end]
        seek(reader, VirtualOffset(coffset, 0))
        @test VirtualOffset(reader) == VirtualOffset(coffset, 0)
    end
    @test eof(reader)
end
        
    
    
